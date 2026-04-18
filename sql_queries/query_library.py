"""
SQL Query Library for Market Risk Data Management & Maintenance
Demonstrates production-grade SQL for data sourcing, validation, and risk reporting.
Each query maps to a real DMM responsibility at Wells Fargo MCRM.
"""

# ─── 1. DATA SOURCING QUERIES ────────────────────────────────────────────────

DAILY_POSITION_SUMMARY = """
-- Daily Position Summary by Desk & Asset Class
-- Used for: Morning risk report generation, desk-level exposure monitoring
SELECT 
    r.date,
    r.desk,
    r.asset_class,
    r.region,
    COUNT(DISTINCT r.instrument_id) AS num_positions,
    ROUND(SUM(r.daily_pnl), 2) AS total_pnl,
    ROUND(SUM(r.var_95), 2) AS total_var95,
    ROUND(SUM(r.var_99), 2) AS total_var99,
    ROUND(SUM(r.stressed_var), 2) AS total_stressed_var,
    ROUND(AVG(r.limit_utilization_pct), 1) AS avg_limit_util,
    MAX(r.limit_utilization_pct) AS max_limit_util
FROM risk_metrics r
JOIN instruments i ON r.instrument_id = i.instrument_id
WHERE r.date = ?
    AND i.is_active = 1
GROUP BY r.date, r.desk, r.asset_class, r.region
ORDER BY total_var99 DESC
"""

GREEKS_EXPOSURE_REPORT = """
-- Greeks Exposure by Desk (Delta, Gamma, Vega, Theta)
-- Used for: Sensitivity analysis, hedging decisions, risk factor attribution
SELECT 
    r.desk,
    r.asset_class,
    ROUND(SUM(r.delta), 2) AS net_delta,
    ROUND(SUM(r.gamma), 4) AS net_gamma,
    ROUND(SUM(r.vega), 2) AS net_vega,
    ROUND(SUM(r.theta), 2) AS net_theta,
    COUNT(*) AS position_count,
    ROUND(SUM(r.var_99), 2) AS desk_var99
FROM risk_metrics r
WHERE r.date = ?
GROUP BY r.desk, r.asset_class
ORDER BY r.desk, ABS(SUM(r.delta)) DESC
"""

# ─── 2. DATA VALIDATION QUERIES ──────────────────────────────────────────────

STALE_PRICE_DETECTION = """
-- Detect Stale/Unchanged Prices (Same price for 3+ consecutive days)
-- Used for: Automated data quality checks, regulatory compliance
WITH price_changes AS (
    SELECT 
        instrument_id,
        date,
        mid_price,
        LAG(mid_price, 1) OVER (PARTITION BY instrument_id ORDER BY date) AS prev_price_1,
        LAG(mid_price, 2) OVER (PARTITION BY instrument_id ORDER BY date) AS prev_price_2
    FROM market_data
    WHERE mid_price IS NOT NULL
)
SELECT 
    pc.instrument_id,
    i.instrument_name,
    i.asset_class,
    i.data_source,
    pc.date,
    pc.mid_price,
    'STALE_PRICE_3DAY' AS issue_type
FROM price_changes pc
JOIN instruments i ON pc.instrument_id = i.instrument_id
WHERE pc.mid_price = pc.prev_price_1 
    AND pc.mid_price = pc.prev_price_2
    AND pc.date >= ?
ORDER BY pc.date DESC, pc.instrument_id
"""

BID_ASK_VALIDATION = """
-- Bid-Ask Spread Validation (Crossed markets, extreme spreads, missing quotes)
-- Used for: Market data integrity checks before risk calculations
SELECT 
    md.date,
    md.instrument_id,
    i.instrument_name,
    i.asset_class,
    i.data_source,
    md.bid_price,
    md.ask_price,
    md.mid_price,
    CASE 
        WHEN md.bid_price IS NULL OR md.ask_price IS NULL THEN 'MISSING_QUOTES'
        WHEN md.bid_price > md.ask_price THEN 'CROSSED_MARKET'
        WHEN md.mid_price > 0 AND (md.ask_price - md.bid_price) / md.mid_price > 0.05 
            THEN 'WIDE_SPREAD'
        WHEN md.bid_price <= 0 OR md.ask_price <= 0 THEN 'NEGATIVE_QUOTE'
        ELSE 'VALID'
    END AS validation_result
FROM market_data md
JOIN instruments i ON md.instrument_id = i.instrument_id
WHERE md.date = ?
    AND (
        md.bid_price IS NULL 
        OR md.ask_price IS NULL
        OR md.bid_price > md.ask_price
        OR (md.mid_price > 0 AND (md.ask_price - md.bid_price) / md.mid_price > 0.05)
        OR md.bid_price <= 0 
        OR md.ask_price <= 0
    )
ORDER BY i.asset_class, md.instrument_id
"""

PRICE_SPIKE_DETECTION = """
-- Price Spike Detection (Z-score based)
-- Used for: Real-time data quality monitoring, false break detection
WITH price_stats AS (
    SELECT 
        instrument_id,
        date,
        mid_price,
        AVG(mid_price) OVER (
            PARTITION BY instrument_id 
            ORDER BY date 
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS rolling_avg,
        -- SQLite doesn't have STDDEV, so we approximate
        AVG(mid_price * mid_price) OVER (
            PARTITION BY instrument_id 
            ORDER BY date 
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) - AVG(mid_price) OVER (
            PARTITION BY instrument_id 
            ORDER BY date 
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) * AVG(mid_price) OVER (
            PARTITION BY instrument_id 
            ORDER BY date 
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS price_variance
    FROM market_data
    WHERE mid_price IS NOT NULL AND mid_price > 0
)
SELECT 
    ps.instrument_id,
    i.instrument_name,
    i.asset_class,
    ps.date,
    ROUND(ps.mid_price, 4) AS mid_price,
    ROUND(ps.rolling_avg, 4) AS rolling_avg_20d,
    CASE 
        WHEN ps.price_variance > 0 
        THEN ROUND((ps.mid_price - ps.rolling_avg) / (ps.price_variance * 1.0), 2)
        ELSE 0 
    END AS approx_z_score
FROM price_stats ps
JOIN instruments i ON ps.instrument_id = i.instrument_id
WHERE ps.rolling_avg IS NOT NULL
    AND ps.price_variance > 0
    AND ABS(ps.mid_price - ps.rolling_avg) / (ps.price_variance * 1.0) > 3
    AND ps.date >= ?
ORDER BY ps.date DESC
"""

# ─── 3. DATA QUALITY MONITORING QUERIES ──────────────────────────────────────

DQ_SUMMARY_DASHBOARD = """
-- Data Quality Summary Dashboard
-- Used for: Daily DQ scorecard, management reporting, trend analysis
SELECT 
    dq.date,
    dq.issue_type,
    dq.severity,
    COUNT(*) AS issue_count,
    SUM(CASE WHEN dq.status = 'OPEN' THEN 1 ELSE 0 END) AS open_count,
    SUM(CASE WHEN dq.status = 'REMEDIATED' THEN 1 ELSE 0 END) AS remediated_count,
    SUM(CASE WHEN dq.status = 'ESCALATED' THEN 1 ELSE 0 END) AS escalated_count,
    ROUND(AVG(CASE WHEN dq.days_to_resolve IS NOT NULL 
        THEN dq.days_to_resolve ELSE NULL END), 1) AS avg_resolution_days,
    SUM(CASE WHEN dq.detected_by = 'ML_MODEL' THEN 1 ELSE 0 END) AS ml_detected,
    SUM(CASE WHEN dq.detected_by = 'AUTOMATED_RULE' THEN 1 ELSE 0 END) AS rule_detected
FROM data_quality_log dq
WHERE dq.date >= ?
GROUP BY dq.date, dq.issue_type, dq.severity
ORDER BY dq.date DESC, 
    CASE dq.severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END
"""

LIMIT_BREACH_REPORT = """
-- Risk Limit Breach Report  
-- Used for: Regulatory reporting, limit monitoring, escalation triggers
SELECT 
    r.date,
    r.desk,
    r.instrument_id,
    i.instrument_name,
    r.asset_class,
    r.region,
    r.limit_utilization_pct,
    r.var_99,
    r.daily_pnl,
    CASE 
        WHEN r.limit_utilization_pct >= 100 THEN 'HARD_BREACH'
        WHEN r.limit_utilization_pct >= 90 THEN 'SOFT_BREACH'
        WHEN r.limit_utilization_pct >= 80 THEN 'WARNING'
        ELSE 'WITHIN_LIMITS'
    END AS breach_status,
    r.primary_risk_factor
FROM risk_metrics r
JOIN instruments i ON r.instrument_id = i.instrument_id
WHERE r.limit_utilization_pct >= 80
    AND r.date = ?
ORDER BY r.limit_utilization_pct DESC
"""

# ─── 4. REGULATORY & CAPITAL REPORTING QUERIES ───────────────────────────────

STRESSED_VAR_CAPITAL = """
-- Stressed VaR for Capital Calculation (Basel III/IV aligned)
-- Used for: Market Risk Capital requirement computation
SELECT 
    r.date,
    r.desk,
    ROUND(SUM(r.var_99), 2) AS total_var99,
    ROUND(SUM(r.stressed_var), 2) AS total_svar,
    ROUND(SUM(r.var_99) * 3.0, 2) AS var_capital_charge,
    ROUND(SUM(r.stressed_var) * 3.0, 2) AS svar_capital_charge,
    ROUND((SUM(r.var_99) + SUM(r.stressed_var)) * 3.0, 2) AS total_capital_charge,
    COUNT(DISTINCT r.instrument_id) AS num_positions
FROM risk_metrics r
WHERE r.date = ?
GROUP BY r.date, r.desk
ORDER BY total_capital_charge DESC
"""

DATA_COMPLETENESS_CHECK = """
-- Data Completeness Check (Expected vs Actual records per day)
-- Used for: Upstream feed monitoring, SLA compliance, gap detection
WITH expected AS (
    SELECT COUNT(*) AS expected_count 
    FROM instruments 
    WHERE is_active = 1
),
actual AS (
    SELECT 
        date,
        COUNT(DISTINCT instrument_id) AS actual_count,
        COUNT(*) AS total_records,
        SUM(CASE WHEN quality_flag != 'CLEAN' THEN 1 ELSE 0 END) AS flagged_records
    FROM market_data
    WHERE date >= ?
    GROUP BY date
)
SELECT 
    a.date,
    e.expected_count,
    a.actual_count,
    a.total_records,
    a.flagged_records,
    ROUND(a.actual_count * 100.0 / e.expected_count, 2) AS completeness_pct,
    ROUND(a.flagged_records * 100.0 / a.total_records, 2) AS error_rate_pct
FROM actual a
CROSS JOIN expected e
ORDER BY a.date DESC
"""

# Dictionary for easy access
QUERY_LIBRARY = {
    'daily_position_summary': {
        'sql': DAILY_POSITION_SUMMARY,
        'description': 'Daily Position Summary by Desk & Asset Class',
        'category': 'Data Sourcing',
        'use_case': 'Morning risk report generation, desk-level exposure monitoring'
    },
    'greeks_exposure': {
        'sql': GREEKS_EXPOSURE_REPORT,
        'description': 'Greeks Exposure Report (Delta, Gamma, Vega, Theta)',
        'category': 'Data Sourcing',
        'use_case': 'Sensitivity analysis, hedging decisions'
    },
    'stale_price_detection': {
        'sql': STALE_PRICE_DETECTION,
        'description': 'Stale Price Detection (3+ day unchanged)',
        'category': 'Data Validation',
        'use_case': 'Automated data quality checks, regulatory compliance'
    },
    'bid_ask_validation': {
        'sql': BID_ASK_VALIDATION,
        'description': 'Bid-Ask Spread Validation',
        'category': 'Data Validation',
        'use_case': 'Market data integrity checks before risk calculations'
    },
    'price_spike_detection': {
        'sql': PRICE_SPIKE_DETECTION,
        'description': 'Price Spike Detection (Z-score based)',
        'category': 'Data Validation',
        'use_case': 'Real-time data quality monitoring'
    },
    'dq_summary': {
        'sql': DQ_SUMMARY_DASHBOARD,
        'description': 'Data Quality Summary Dashboard',
        'category': 'DQ Monitoring',
        'use_case': 'Daily DQ scorecard, management reporting'
    },
    'limit_breaches': {
        'sql': LIMIT_BREACH_REPORT,
        'description': 'Risk Limit Breach Report',
        'category': 'DQ Monitoring',
        'use_case': 'Regulatory reporting, limit monitoring'
    },
    'stressed_var_capital': {
        'sql': STRESSED_VAR_CAPITAL,
        'description': 'Stressed VaR Capital Calculation',
        'category': 'Regulatory & Capital',
        'use_case': 'Market Risk Capital requirement (Basel III/IV)'
    },
    'data_completeness': {
        'sql': DATA_COMPLETENESS_CHECK,
        'description': 'Data Completeness Check',
        'category': 'Regulatory & Capital',
        'use_case': 'Upstream feed monitoring, SLA compliance'
    }
}

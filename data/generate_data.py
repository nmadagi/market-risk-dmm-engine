"""
Market Risk Data Generator
Generates realistic synthetic market risk data for DMM validation pipeline.
Simulates: positions, market data feeds, P&L, VaR, Greeks, and intentional data quality issues.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import os

np.random.seed(42)

# ─── Configuration ───────────────────────────────────────────────────────────
NUM_DAYS = 252  # 1 year of trading days
NUM_INSTRUMENTS = 150
ANOMALY_RATE = 0.04  # 4% of records will have data quality issues

ASSET_CLASSES = ['Equities', 'Fixed Income', 'FX', 'Commodities', 'Credit Derivatives']
DESKS = ['Rates Trading', 'FX Options', 'Equity Derivatives', 'Credit Trading', 'Commodity Futures']
REGIONS = ['NAM', 'EMEA', 'APAC']
DATA_SOURCES = ['Bloomberg', 'Reuters', 'MarkIT', 'Internal Pricing', 'ICE']

RISK_FACTORS = {
    'Equities': ['SPX_Delta', 'NDX_Delta', 'Equity_Vega', 'Equity_Gamma'],
    'Fixed Income': ['IR_DV01', 'IR_Convexity', 'Spread_DV01', 'IR_Theta'],
    'FX': ['FX_Delta', 'FX_Vega', 'FX_Gamma', 'FX_Rho'],
    'Commodities': ['Cmdty_Delta', 'Cmdty_Vega', 'Cmdty_Theta', 'Cmdty_Rho'],
    'Credit Derivatives': ['CS01', 'CR01', 'JTD', 'Credit_Gamma']
}


def generate_instruments(n=NUM_INSTRUMENTS):
    """Generate instrument master data."""
    instruments = []
    for i in range(n):
        ac = np.random.choice(ASSET_CLASSES, p=[0.25, 0.30, 0.20, 0.10, 0.15])
        desk = np.random.choice(DESKS)
        region = np.random.choice(REGIONS, p=[0.5, 0.3, 0.2])
        source = np.random.choice(DATA_SOURCES)

        instruments.append({
            'instrument_id': f'INST_{i+1:04d}',
            'instrument_name': f'{ac[:3].upper()}_{np.random.choice(["SWAP","OPT","FWD","FUT","BOND"])}_{i+1:04d}',
            'asset_class': ac,
            'desk': desk,
            'region': region,
            'data_source': source,
            'currency': np.random.choice(['USD', 'EUR', 'GBP', 'JPY', 'CHF'], p=[0.4, 0.25, 0.15, 0.1, 0.1]),
            'notional': round(np.random.lognormal(mean=16, sigma=1.5), 2),
            'maturity_date': (datetime.now() + timedelta(days=np.random.randint(30, 3650))).strftime('%Y-%m-%d'),
            'is_active': np.random.choice([1, 0], p=[0.92, 0.08])
        })
    return pd.DataFrame(instruments)


def generate_market_data(instruments_df, num_days=NUM_DAYS):
    """Generate daily market data with intentional quality issues."""
    records = []
    base_date = datetime(2025, 1, 2)
    inst_ids = instruments_df['instrument_id'].tolist()

    for day in range(num_days):
        current_date = base_date + timedelta(days=day * 365 // 252)
        if current_date.weekday() >= 5:
            continue

        for inst_id in inst_ids:
            base_price = np.random.lognormal(mean=4, sigma=0.8)
            daily_return = np.random.normal(0, 0.02)
            price = base_price * (1 + daily_return)

            record = {
                'date': current_date.strftime('%Y-%m-%d'),
                'instrument_id': inst_id,
                'mid_price': round(price, 4),
                'bid_price': round(price * (1 - abs(np.random.normal(0.001, 0.0005))), 4),
                'ask_price': round(price * (1 + abs(np.random.normal(0.001, 0.0005))), 4),
                'volume': max(0, int(np.random.lognormal(mean=10, sigma=2))),
                'data_source_timestamp': current_date.strftime('%Y-%m-%d %H:%M:%S'),
                'is_stale': 0,
                'quality_flag': 'CLEAN'
            }

            # Inject anomalies (~4% of records)
            if np.random.random() < ANOMALY_RATE:
                anomaly_type = np.random.choice([
                    'stale_price', 'missing_bid_ask', 'price_spike',
                    'negative_price', 'zero_volume', 'duplicate_source',
                    'timestamp_delay', 'crossed_market'
                ])

                if anomaly_type == 'stale_price':
                    record['is_stale'] = 1
                    record['quality_flag'] = 'STALE'
                elif anomaly_type == 'missing_bid_ask':
                    record['bid_price'] = None
                    record['ask_price'] = None
                    record['quality_flag'] = 'MISSING_QUOTES'
                elif anomaly_type == 'price_spike':
                    record['mid_price'] = round(price * np.random.choice([3.5, 0.2]), 4)
                    record['quality_flag'] = 'SPIKE'
                elif anomaly_type == 'negative_price':
                    record['mid_price'] = round(-abs(price), 4)
                    record['quality_flag'] = 'NEGATIVE'
                elif anomaly_type == 'zero_volume':
                    record['volume'] = 0
                    record['quality_flag'] = 'ZERO_VOL'
                elif anomaly_type == 'crossed_market':
                    record['bid_price'] = record['ask_price'] + 0.5
                    record['quality_flag'] = 'CROSSED'
                elif anomaly_type == 'timestamp_delay':
                    delayed = current_date - timedelta(hours=np.random.randint(24, 72))
                    record['data_source_timestamp'] = delayed.strftime('%Y-%m-%d %H:%M:%S')
                    record['quality_flag'] = 'DELAYED'

            records.append(record)

    return pd.DataFrame(records)


def generate_risk_metrics(instruments_df, market_data_df):
    """Generate daily risk metrics (VaR, Greeks, P&L)."""
    records = []
    dates = market_data_df['date'].unique()

    for date in dates:
        day_data = market_data_df[market_data_df['date'] == date]

        for _, inst in instruments_df.iterrows():
            inst_id = inst['instrument_id']
            ac = inst['asset_class']
            notional = inst['notional']

            inst_market = day_data[day_data['instrument_id'] == inst_id]
            if inst_market.empty:
                continue

            mid_price = inst_market.iloc[0]['mid_price']
            if mid_price is None or pd.isna(mid_price):
                mid_price = 100.0

            scale = notional / 1e6

            risk_factors = RISK_FACTORS.get(ac, ['Generic_Delta'])

            record = {
                'date': date,
                'instrument_id': inst_id,
                'desk': inst['desk'],
                'asset_class': ac,
                'region': inst['region'],
                'daily_pnl': round(np.random.normal(0, 50000 * scale), 2),
                'var_95': round(abs(np.random.normal(100000, 30000)) * scale, 2),
                'var_99': round(abs(np.random.normal(150000, 45000)) * scale, 2),
                'stressed_var': round(abs(np.random.normal(250000, 75000)) * scale, 2),
                'delta': round(np.random.normal(0, 1000) * scale, 2),
                'gamma': round(np.random.normal(0, 50) * scale, 4),
                'vega': round(np.random.normal(0, 500) * scale, 2),
                'theta': round(-abs(np.random.normal(0, 200)) * scale, 2),
                'primary_risk_factor': np.random.choice(risk_factors),
                'limit_utilization_pct': round(np.random.beta(2, 5) * 100, 1)
            }

            if np.random.random() < 0.03:
                record['limit_utilization_pct'] = round(np.random.uniform(95, 130), 1)

            records.append(record)

    return pd.DataFrame(records)


def generate_data_quality_log(market_data_df):
    """Generate a data quality control log from flagged records."""
    flagged = market_data_df[market_data_df['quality_flag'] != 'CLEAN'].copy()

    severity_map = {
        'STALE': 'MEDIUM',
        'MISSING_QUOTES': 'HIGH',
        'SPIKE': 'CRITICAL',
        'NEGATIVE': 'CRITICAL',
        'ZERO_VOL': 'LOW',
        'CROSSED': 'HIGH',
        'DELAYED': 'MEDIUM'
    }

    status_choices = ['OPEN', 'UNDER_REVIEW', 'REMEDIATED', 'ESCALATED', 'FALSE_POSITIVE']
    status_probs = [0.30, 0.25, 0.25, 0.10, 0.10]

    log_records = []
    for idx, row in flagged.iterrows():
        log_records.append({
            'log_id': f'DQ_{idx:06d}',
            'date': row['date'],
            'instrument_id': row['instrument_id'],
            'issue_type': row['quality_flag'],
            'severity': severity_map.get(row['quality_flag'], 'LOW'),
            'status': np.random.choice(status_choices, p=status_probs),
            'detected_by': np.random.choice(['AUTOMATED_RULE', 'ML_MODEL', 'MANUAL_REVIEW'], p=[0.5, 0.35, 0.15]),
            'remediation_notes': '',
            'days_to_resolve': np.random.choice([0, 1, 2, 3, 5, None], p=[0.3, 0.25, 0.2, 0.1, 0.05, 0.1])
        })

    return pd.DataFrame(log_records)


def build_database():
    """Build SQLite database with all tables."""
    db_path = os.path.join(os.path.dirname(__file__), 'market_risk.db')

    print("Generating instruments...")
    instruments = generate_instruments()

    print("Generating market data (this may take a moment)...")
    market_data = generate_market_data(instruments)

    print("Generating risk metrics...")
    risk_metrics = generate_risk_metrics(instruments, market_data)

    print("Generating data quality log...")
    dq_log = generate_data_quality_log(market_data)

    print(f"Writing to {db_path}...")
    conn = sqlite3.connect(db_path)

    instruments.to_sql('instruments', conn, if_exists='replace', index=False)
    market_data.to_sql('market_data', conn, if_exists='replace', index=False)
    risk_metrics.to_sql('risk_metrics', conn, if_exists='replace', index=False)
    dq_log.to_sql('data_quality_log', conn, if_exists='replace', index=False)

    cursor = conn.cursor()
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_md_date ON market_data(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_md_inst ON market_data(instrument_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rm_date ON risk_metrics(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rm_desk ON risk_metrics(desk)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_dq_date ON data_quality_log(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_dq_severity ON data_quality_log(severity)')
    conn.commit()

    print(f"\n Database built successfully!")
    print(f"   Instruments:      {len(instruments):,} records")
    print(f"   Market Data:      {len(market_data):,} records")
    print(f"   Risk Metrics:     {len(risk_metrics):,} records")
    print(f"   DQ Log Entries:   {len(dq_log):,} records")

    conn.close()
    return db_path


if __name__ == '__main__':
    build_database()

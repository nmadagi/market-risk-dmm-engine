"""
Market Risk Data Management & Validation Engine
================================================
A comprehensive data quality management platform for Market & Counterparty Risk Management (MCRM).
Demonstrates: Data Sourcing, Validation, ML Anomaly Detection, SQL Expertise, and Risk Reporting.

Built for: Wells Fargo Lead Market Risk Officer - DMM Team Interview
"""

import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from sql_queries.query_library import QUERY_LIBRARY
from models.anomaly_detector import MarketDataAnomalyDetector

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MCRM Data Management Engine",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #FFCD41;
        margin-bottom: 0;
        letter-spacing: -0.5px;
    }
    .sub-header {
        font-size: 1.0rem;
        color: #8B8FA3;
        margin-top: -10px;
        margin-bottom: 25px;
    }
    .severity-critical { color: #FF4B4B; font-weight: 700; }
    .severity-high { color: #FFA94D; font-weight: 600; }
    .severity-medium { color: #FFCD41; }
    .severity-low { color: #69DB7C; }
    div[data-testid="stMetricValue"] { font-size: 1.8rem; }
</style>
""", unsafe_allow_html=True)


# ── Database Connection ───────────────────────────────────────────────────────
@st.cache_resource
def get_database():
    db_path = os.path.join(os.path.dirname(__file__), 'data', 'market_risk.db')
    if not os.path.exists(db_path):
        from data.generate_data import build_database
        build_database()
    return db_path


@st.cache_data(ttl=300)
def run_query(query, params=None):
    db_path = get_database()
    conn = sqlite3.connect(db_path)
    try:
        if params:
            df = pd.read_sql_query(query, conn, params=params)
        else:
            df = pd.read_sql_query(query, conn)
        return df
    finally:
        conn.close()


@st.cache_data(ttl=600)
def load_full_table(table_name):
    db_path = get_database()
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df


@st.cache_resource
def get_anomaly_detector():
    model_path = os.path.join(os.path.dirname(__file__), 'models', 'anomaly_detector.pkl')
    detector = MarketDataAnomalyDetector(contamination=0.05)
    if os.path.exists(model_path):
        detector.load_model(model_path)
    else:
        market_data = load_full_table('market_data')
        detector.fit(market_data)
        detector.save_model(model_path)
    return detector


def get_available_dates():
    df = run_query("SELECT DISTINCT date FROM market_data ORDER BY date DESC")
    return df['date'].tolist()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏦 MCRM DMM Engine")
    st.markdown("---")

    page = st.radio(
        "Navigation",
        [
            "📊 Executive Dashboard",
            "🔍 Data Quality Monitor",
            "🤖 ML Anomaly Detection",
            "📝 SQL Query Lab",
            "⚠️ Risk & Limits",
            "📋 About This Project"
        ],
        index=0
    )

    st.markdown("---")
    dates = get_available_dates()
    if dates:
        selected_date = st.selectbox("Select Date", dates[:30], index=0)
    else:
        selected_date = None

    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #8B8FA3; font-size: 0.75rem;'>
        Built for Wells Fargo<br>
        Lead Market Risk Officer<br>
        DMM Team Interview
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1: Executive Dashboard
# ═══════════════════════════════════════════════════════════════════════════════
if page == "📊 Executive Dashboard":
    st.markdown('<p class="main-header">Market Risk Data Management Dashboard</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Data Management & Maintenance (DMM) — Daily Overview</p>', unsafe_allow_html=True)

    if selected_date:
        risk_data = run_query("SELECT * FROM risk_metrics WHERE date = ?", (selected_date,))
        dq_data = run_query("SELECT * FROM data_quality_log WHERE date = ?", (selected_date,))
        market_data_day = run_query("SELECT * FROM market_data WHERE date = ?", (selected_date,))

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Active Positions", f"{len(risk_data):,}")
        with col2:
            total_var = risk_data['var_99'].sum() if len(risk_data) > 0 else 0
            st.metric("Total VaR (99%)", f"${total_var/1e6:,.1f}M")
        with col3:
            total_pnl = risk_data['daily_pnl'].sum() if len(risk_data) > 0 else 0
            delta_str = "positive" if total_pnl > 0 else "negative"
            st.metric("Daily P&L", f"${total_pnl/1e6:,.2f}M")
        with col4:
            clean_pct = (market_data_day['quality_flag'] == 'CLEAN').mean() * 100 if len(market_data_day) > 0 else 0
            st.metric("Data Quality Score", f"{clean_pct:.1f}%")
        with col5:
            critical_issues = len(dq_data[dq_data['severity'] == 'CRITICAL']) if len(dq_data) > 0 else 0
            st.metric("Critical Issues", f"{critical_issues}",
                      delta=f"{critical_issues} open" if critical_issues > 0 else "None",
                      delta_color="inverse")

        st.markdown("---")

        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("#### VaR by Desk")
            if len(risk_data) > 0:
                desk_var = risk_data.groupby('desk').agg(
                    var_99=('var_99', 'sum'),
                    stressed_var=('stressed_var', 'sum'),
                    positions=('instrument_id', 'nunique')
                ).reset_index().sort_values('var_99', ascending=True)

                fig = go.Figure()
                fig.add_trace(go.Bar(y=desk_var['desk'], x=desk_var['var_99']/1e6,
                    name='VaR 99%', orientation='h', marker_color='#D71E28'))
                fig.add_trace(go.Bar(y=desk_var['desk'], x=desk_var['stressed_var']/1e6,
                    name='Stressed VaR', orientation='h', marker_color='#FFCD41'))
                fig.update_layout(barmode='group', height=350, xaxis_title="$ Millions",
                    template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=0,r=20,t=10,b=40),
                    legend=dict(orientation="h", y=-0.15))
                st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.markdown("#### Data Quality Breakdown")
            if len(market_data_day) > 0:
                quality_dist = market_data_day['quality_flag'].value_counts().reset_index()
                quality_dist.columns = ['Flag', 'Count']
                color_map = {
                    'CLEAN': '#69DB7C', 'STALE': '#FFCD41', 'SPIKE': '#FF4B4B',
                    'MISSING_QUOTES': '#FFA94D', 'NEGATIVE': '#FF4B4B',
                    'ZERO_VOL': '#748FFC', 'CROSSED': '#FFA94D', 'DELAYED': '#FFCD41'
                }
                fig = px.pie(quality_dist, values='Count', names='Flag',
                    color='Flag', color_discrete_map=color_map, hole=0.4)
                fig.update_layout(height=350, template='plotly_dark',
                    paper_bgcolor='rgba(0,0,0,0)', margin=dict(l=0,r=0,t=10,b=40))
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### Daily P&L Distribution by Asset Class")
        if len(risk_data) > 0:
            fig = px.box(risk_data, x='asset_class', y='daily_pnl', color='asset_class',
                color_discrete_sequence=['#D71E28', '#FFCD41', '#748FFC', '#69DB7C', '#FFA94D'])
            fig.update_layout(height=350, template='plotly_dark',
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                yaxis_title="Daily P&L ($)", showlegend=False,
                margin=dict(l=0,r=20,t=10,b=40))
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2: Data Quality Monitor
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🔍 Data Quality Monitor":
    st.markdown('<p class="main-header">Data Quality Monitor</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Sourcing, Validating & Maintaining Market Risk Data</p>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["📈 Quality Trends", "🔎 Issue Drill-Down", "📊 Completeness"])

    with tab1:
        dq_trend = run_query("""
            SELECT date, severity, COUNT(*) as issue_count
            FROM data_quality_log GROUP BY date, severity ORDER BY date
        """)
        if len(dq_trend) > 0:
            fig = px.area(dq_trend, x='date', y='issue_count', color='severity',
                color_discrete_map={'CRITICAL': '#FF4B4B', 'HIGH': '#FFA94D',
                    'MEDIUM': '#FFCD41', 'LOW': '#69DB7C'})
            fig.update_layout(height=400, template='plotly_dark',
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                xaxis_title="Date", yaxis_title="Issue Count",
                margin=dict(l=0,r=20,t=10,b=40))
            st.plotly_chart(fig, use_container_width=True)

        detection_data = run_query("""
            SELECT detected_by, severity, COUNT(*) as cnt
            FROM data_quality_log GROUP BY detected_by, severity
        """)
        if len(detection_data) > 0:
            st.markdown("#### Detection Method Effectiveness")
            fig = px.bar(detection_data, x='detected_by', y='cnt', color='severity',
                barmode='group', color_discrete_map={'CRITICAL': '#FF4B4B',
                    'HIGH': '#FFA94D', 'MEDIUM': '#FFCD41', 'LOW': '#69DB7C'})
            fig.update_layout(height=350, template='plotly_dark',
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=0,r=20,t=10,b=40))
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        if selected_date:
            st.markdown(f"#### Issues for {selected_date}")
            issues = run_query(
                "SELECT * FROM data_quality_log WHERE date = ? ORDER BY severity",
                (selected_date,))
            if len(issues) > 0:
                severity_filter = st.multiselect("Filter by Severity",
                    ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'], default=['CRITICAL', 'HIGH'])
                filtered = issues[issues['severity'].isin(severity_filter)]
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Filtered Issues", len(filtered))
                with col2:
                    st.metric("Open", len(filtered[filtered['status'] == 'OPEN']))
                with col3:
                    st.metric("Escalated", len(filtered[filtered['status'] == 'ESCALATED']))
                st.dataframe(filtered[['log_id', 'instrument_id', 'issue_type', 'severity',
                    'status', 'detected_by', 'days_to_resolve']],
                    use_container_width=True, height=400)
            else:
                st.success("No data quality issues found for this date.")

    with tab3:
        completeness = run_query(QUERY_LIBRARY['data_completeness']['sql'],
            (dates[-1] if dates else '2025-01-01',))
        if len(completeness) > 0:
            st.markdown("#### Data Completeness Over Time")
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Scatter(x=completeness['date'], y=completeness['completeness_pct'],
                name='Completeness %', line=dict(color='#69DB7C', width=2)), secondary_y=False)
            fig.add_trace(go.Bar(x=completeness['date'], y=completeness['error_rate_pct'],
                name='Error Rate %', marker_color='#FF4B4B', opacity=0.6), secondary_y=True)
            fig.update_layout(height=400, template='plotly_dark',
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=0,r=20,t=10,b=40))
            fig.update_yaxes(title_text="Completeness %", secondary_y=False)
            fig.update_yaxes(title_text="Error Rate %", secondary_y=True)
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3: ML Anomaly Detection
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🤖 ML Anomaly Detection":
    st.markdown('<p class="main-header">ML-Powered Anomaly Detection</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Isolation Forest + Rule-Based Ensemble for Data Quality Validation</p>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["🎯 Detection Results", "📊 Model Performance", "⚙️ Model Architecture"])

    with tab1:
        with st.spinner("Loading ML model and running detection..."):
            detector = get_anomaly_detector()
            if selected_date:
                day_data = run_query("SELECT * FROM market_data WHERE date = ?", (selected_date,))
                if len(day_data) > 0:
                    results = detector.predict(day_data)
                    anomalies = results[results['ensemble_anomaly'] == 1]

                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Records Scanned", f"{len(results):,}")
                    with col2:
                        st.metric("Anomalies Detected", f"{len(anomalies):,}",
                            delta=f"{len(anomalies)/len(results)*100:.1f}%")
                    with col3:
                        st.metric("ML Detected", f"{results['ml_anomaly'].sum():,}")
                    with col4:
                        st.metric("Rule Detected", f"{(results['rule_flag_count'] > 0).sum():,}")

                    st.markdown("---")
                    col_left, col_right = st.columns(2)

                    with col_left:
                        st.markdown("#### Anomaly Severity Distribution")
                        sev = anomalies['severity'].value_counts().reset_index()
                        sev.columns = ['Severity', 'Count']
                        fig = px.bar(sev, x='Severity', y='Count', color='Severity',
                            color_discrete_map={'CRITICAL': '#FF4B4B', 'HIGH': '#FFA94D',
                                'MEDIUM': '#FFCD41', 'LOW': '#69DB7C'})
                        fig.update_layout(height=300, template='plotly_dark',
                            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                            showlegend=False, margin=dict(l=0,r=20,t=10,b=40))
                        st.plotly_chart(fig, use_container_width=True)

                    with col_right:
                        st.markdown("#### Issue Type Breakdown")
                        iss = anomalies['detected_issue'].value_counts().reset_index()
                        iss.columns = ['Issue', 'Count']
                        fig = px.bar(iss, x='Count', y='Issue', orientation='h',
                            color_discrete_sequence=['#D71E28'])
                        fig.update_layout(height=300, template='plotly_dark',
                            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                            margin=dict(l=0,r=20,t=10,b=40))
                        st.plotly_chart(fig, use_container_width=True)

                    st.markdown("#### Anomaly Score Distribution")
                    fig = go.Figure()
                    fig.add_trace(go.Histogram(
                        x=results[results['ensemble_anomaly']==0]['anomaly_score'],
                        name='Normal', marker_color='#69DB7C', opacity=0.7, nbinsx=50))
                    fig.add_trace(go.Histogram(
                        x=results[results['ensemble_anomaly']==1]['anomaly_score'],
                        name='Anomaly', marker_color='#FF4B4B', opacity=0.7, nbinsx=50))
                    fig.update_layout(barmode='overlay', height=300, template='plotly_dark',
                        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                        xaxis_title="Isolation Forest Score", yaxis_title="Count",
                        margin=dict(l=0,r=20,t=10,b=40))
                    st.plotly_chart(fig, use_container_width=True)

                    st.markdown("#### Flagged Records (Top 50)")
                    display_cols = ['instrument_id', 'date', 'mid_price', 'anomaly_score',
                        'severity', 'detected_issue', 'ml_anomaly', 'rule_flag_count']
                    st.dataframe(anomalies[display_cols].head(50),
                        use_container_width=True, height=400)

    with tab2:
        st.markdown("#### Model Performance Evaluation")
        st.markdown("The model is evaluated against **known data quality labels** injected during data generation.")

        with st.spinner("Evaluating model on full dataset..."):
            market_data = load_full_table('market_data')
            detector = get_anomaly_detector()
            full_results = detector.predict(market_data)

            actual = (market_data['quality_flag'] != 'CLEAN').astype(int)
            predicted = full_results['ensemble_anomaly']

            tp = ((predicted == 1) & (actual == 1)).sum()
            fp = ((predicted == 1) & (actual == 0)).sum()
            fn = ((predicted == 0) & (actual == 1)).sum()
            tn = ((predicted == 0) & (actual == 0)).sum()

            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
            accuracy = (tp + tn) / (tp + fp + fn + tn)

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Precision", f"{precision:.1%}")
            with col2:
                st.metric("Recall", f"{recall:.1%}")
            with col3:
                st.metric("F1-Score", f"{f1:.1%}")
            with col4:
                st.metric("Accuracy", f"{accuracy:.1%}")

            st.markdown("#### Confusion Matrix")
            cm = pd.DataFrame([[tn, fp], [fn, tp]],
                index=['Actual Normal', 'Actual Anomaly'],
                columns=['Predicted Normal', 'Predicted Anomaly'])
            fig = px.imshow(cm.values, text_auto=True,
                x=['Predicted Normal', 'Predicted Anomaly'],
                y=['Actual Normal', 'Actual Anomaly'],
                color_continuous_scale=['#1A1D26', '#D71E28'], aspect='auto')
            fig.update_layout(height=350, template='plotly_dark',
                paper_bgcolor='rgba(0,0,0,0)', margin=dict(l=0,r=20,t=10,b=40))
            st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.markdown("#### Model Architecture")
        st.markdown("**Ensemble Approach: 3-Layer Detection System**")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("""
            **🧠 Layer 1: Isolation Forest (ML)**
            - Unsupervised anomaly detection
            - 200 estimators, auto-sampling
            - Catches novel/unknown patterns
            - Features: price, spread, volume, staleness, crossed market flags
            """)
        with col2:
            st.markdown("""
            **📏 Layer 2: Rule-Based Engine**
            - Domain knowledge codified
            - Negative prices
            - Crossed bid/ask markets
            - Missing quotes
            - Zero volume
            - Stale price detection
            """)
        with col3:
            st.markdown("""
            **📊 Layer 3: Statistical Tests**
            - Z-score threshold (> 3 sigma)
            - IQR-based outlier detection
            - Rolling window analysis
            - Price distribution monitoring
            """)

        st.markdown("---")
        st.markdown("#### Feature Engineering Pipeline")
        model_summary = get_anomaly_detector().get_model_summary()
        st.dataframe(pd.DataFrame({'Feature': model_summary['feature_names']}),
            use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 4: SQL Query Lab
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "📝 SQL Query Lab":
    st.markdown('<p class="main-header">SQL Query Lab</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Production SQL Queries for Market Risk Data Management</p>', unsafe_allow_html=True)

    query_categories = {}
    for key, val in QUERY_LIBRARY.items():
        cat = val['category']
        if cat not in query_categories:
            query_categories[cat] = []
        query_categories[cat].append((key, val['description']))

    selected_category = st.selectbox("Query Category", list(query_categories.keys()))
    query_options = query_categories[selected_category]
    selected_query_key = st.selectbox("Select Query",
        [k for k, _ in query_options],
        format_func=lambda k: next(d for key, d in query_options if key == k))

    query_info = QUERY_LIBRARY[selected_query_key]
    st.markdown(f"**Use Case:** {query_info['use_case']}")

    with st.expander("View SQL Query", expanded=True):
        st.code(query_info['sql'], language='sql')

    if st.button("Execute Query", type="primary"):
        with st.spinner("Running query..."):
            try:
                param = selected_date if selected_date else '2025-06-01'
                result = run_query(query_info['sql'], (param,))
                st.success(f"Query returned {len(result):,} rows")
                st.dataframe(result, use_container_width=True, height=500)
                csv = result.to_csv(index=False)
                st.download_button("Download Results (CSV)", csv,
                    f"{selected_query_key}_{selected_date}.csv", "text/csv")
            except Exception as e:
                st.error(f"Query error: {str(e)}")

    st.markdown("---")
    st.markdown("#### Custom SQL Query")
    custom_sql = st.text_area("Write your own SQL query:",
        height=150, placeholder="SELECT * FROM instruments WHERE asset_class = 'Equities' LIMIT 10")

    if st.button("Run Custom Query"):
        if custom_sql.strip():
            if custom_sql.strip().upper().startswith('SELECT'):
                try:
                    result = run_query(custom_sql)
                    st.success(f"Returned {len(result):,} rows")
                    st.dataframe(result, use_container_width=True, height=400)
                except Exception as e:
                    st.error(f"Error: {str(e)}")
            else:
                st.warning("Only SELECT queries are allowed for safety.")

    with st.expander("Database Schema Reference"):
        st.markdown("""
        | Table | Description | Key Columns |
        |-------|-------------|-------------|
        | `instruments` | Instrument master data | instrument_id, asset_class, desk, notional |
        | `market_data` | Daily market prices & quotes | date, instrument_id, mid_price, bid_price, ask_price, quality_flag |
        | `risk_metrics` | Daily risk measures | date, instrument_id, var_95, var_99, daily_pnl, delta, gamma, vega |
        | `data_quality_log` | DQ issue tracking | log_id, issue_type, severity, status, detected_by |
        """)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 5: Risk & Limits
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "⚠️ Risk & Limits":
    st.markdown('<p class="main-header">Risk Limits & Capital</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Limit Monitoring, Breach Reporting & Capital Charges</p>', unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["🚨 Limit Breaches", "💰 Capital Charges"])

    with tab1:
        if selected_date:
            breaches = run_query(QUERY_LIBRARY['limit_breaches']['sql'], (selected_date,))
            if len(breaches) > 0:
                col1, col2, col3 = st.columns(3)
                hard = len(breaches[breaches['breach_status'] == 'HARD_BREACH'])
                soft = len(breaches[breaches['breach_status'] == 'SOFT_BREACH'])
                warn = len(breaches[breaches['breach_status'] == 'WARNING'])
                with col1:
                    st.metric("Hard Breaches", hard,
                        delta="Action Required" if hard > 0 else None, delta_color="inverse")
                with col2:
                    st.metric("Soft Breaches", soft)
                with col3:
                    st.metric("Warnings", warn)

                st.markdown("#### Limit Utilization by Position")
                fig = px.scatter(breaches, x='var_99', y='limit_utilization_pct',
                    color='breach_status', size='daily_pnl',
                    hover_data=['desk', 'instrument_id', 'asset_class'],
                    color_discrete_map={'HARD_BREACH': '#FF4B4B',
                        'SOFT_BREACH': '#FFA94D', 'WARNING': '#FFCD41'})
                fig.add_hline(y=100, line_dash="dash", line_color="#FF4B4B",
                    annotation_text="Hard Limit")
                fig.add_hline(y=90, line_dash="dash", line_color="#FFA94D",
                    annotation_text="Soft Limit")
                fig.update_layout(height=400, template='plotly_dark',
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                    xaxis_title="VaR 99% ($)", yaxis_title="Limit Utilization %",
                    margin=dict(l=0,r=20,t=10,b=40))
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(breaches, use_container_width=True, height=400)
            else:
                st.success("No limit breaches or warnings for this date.")

    with tab2:
        if selected_date:
            capital = run_query(QUERY_LIBRARY['stressed_var_capital']['sql'], (selected_date,))
            if len(capital) > 0:
                total_charge = capital['total_capital_charge'].sum()
                st.metric("Total Capital Charge", f"${total_charge/1e6:,.1f}M")
                fig = go.Figure()
                fig.add_trace(go.Bar(x=capital['desk'],
                    y=capital['var_capital_charge']/1e6, name='VaR Capital',
                    marker_color='#D71E28'))
                fig.add_trace(go.Bar(x=capital['desk'],
                    y=capital['svar_capital_charge']/1e6, name='SVaR Capital',
                    marker_color='#FFCD41'))
                fig.update_layout(barmode='stack', height=400, template='plotly_dark',
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                    yaxis_title="Capital Charge ($ Millions)",
                    margin=dict(l=0,r=20,t=10,b=40))
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(capital, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 6: About
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "📋 About This Project":
    st.markdown('<p class="main-header">About This Project</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Market Risk Data Management & Validation Engine</p>', unsafe_allow_html=True)

    st.markdown("""
    ### Project Overview

    This application demonstrates end-to-end capabilities in **Market Risk Data Management & Maintenance (DMM)**,
    aligned with the responsibilities of the MCRM team at Wells Fargo.

    ---

    ### How This Maps to the Role

    | Job Responsibility | Project Implementation |
    |---|---|
    | Sourcing, validating, maintaining risk data | Complete data pipeline with 150 instruments, 37K+ market data records |
    | Data and reporting solutions | Interactive Streamlit dashboard with 6 report types |
    | Strategic data initiatives | ML-powered anomaly detection (Isolation Forest ensemble) |
    | Analyze data, identify issues, root causes | SQL query library with 9 production queries across 4 categories |
    | Control frameworks | 3-layer validation: ML + Rules + Statistical checks |
    | Regulatory reporting & capital processes | Basel III/IV capital charge calculations, stressed VaR |
    | Cross-functional collaboration | Designed for consumption by front office, risk, and technology teams |

    ---

    ### Technical Stack

    | Component | Technology |
    |---|---|
    | **Data Layer** | SQLite, Python (pandas, numpy) |
    | **ML Model** | scikit-learn Isolation Forest, StandardScaler |
    | **SQL** | 9 production queries: window functions, CTEs, aggregations |
    | **Visualization** | Plotly, Streamlit |
    | **Deployment** | Streamlit Community Cloud + GitHub |

    ---

    ### Data Architecture

    ```
    Data Sources (Bloomberg, Reuters, MarkIT, ICE, Internal)
         |
         v
    +---------------------+
    |  Instrument Master   |  150 instruments across 5 asset classes
    +----------+----------+
               |
               v
    +---------------------+
    |    Market Data       |  Daily prices, quotes, volume (37K+ records)
    |    (with DQ issues)  |  ~4% intentional anomaly injection
    +----------+----------+
               |
         +-----+-----+
         v           v
    +---------+ +--------------+
    | SQL     | | ML Anomaly   |
    | Queries | | Detection    |
    +----+----+ +------+-------+
         |             |
         v             v
    +---------------------+
    |  Risk Metrics &     |  VaR, Greeks, P&L, Capital Charges
    |  DQ Control Log     |  Issue tracking & remediation
    +---------------------+
    ```

    ---

    ### About the Author

    **[Your Name]**
    - Background in [your background]
    - Passionate about leveraging data and technology to strengthen risk management
    - [LinkedIn] | [GitHub]
    """)

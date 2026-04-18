# 🏦 Market Risk Data Management & Validation Engine

A comprehensive data quality management platform for **Market & Counterparty Risk Management (MCRM)**, demonstrating end-to-end capabilities in data sourcing, validation, ML-powered anomaly detection, and risk reporting.

---

## 🎯 Project Purpose

This project demonstrates the core responsibilities of a **Data Management & Maintenance (DMM)** team within a financial institution's market risk function:

- **Data Sourcing** — Ingesting market data from multiple sources (Bloomberg, Reuters, MarkIT, ICE)
- **Data Validation** — Automated quality checks using SQL and ML models
- **Data Maintenance** — Issue tracking, remediation workflows, and control frameworks
- **Risk Reporting** — VaR, Greeks, P&L, capital charges, and limit monitoring
- **Regulatory Compliance** — Basel III/IV capital calculations, data completeness monitoring

---

## 📊 Live Demo

🔗 **[View Live Application](https://your-app-url.streamlit.app)**

---

## 🏗️ Architecture

```
Data Sources (Bloomberg, Reuters, MarkIT, ICE, Internal)
     │
     ▼
┌─────────────────────────┐
│   Instrument Master      │  150 instruments across 5 asset classes
│   (5 desks, 3 regions)   │  Equities, Fixed Income, FX, Commodities, Credit
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│     Market Data Feed     │  Daily prices, bid/ask quotes, volume
│     (~37K+ records)      │  ~4% intentional anomaly injection
└───────────┬─────────────┘
            │
      ┌─────┴─────┐
      ▼           ▼
┌──────────┐ ┌───────────────┐
│  SQL     │ │  ML Anomaly   │
│  Engine  │ │  Detection    │
│  (9 Qry) │ │  (3-Layer)    │
└────┬─────┘ └──────┬────────┘
     │              │
     ▼              ▼
┌─────────────────────────┐
│   Risk Metrics Engine    │  VaR (95/99), Stressed VaR, Greeks
│   + DQ Control Log       │  P&L, Capital Charges, Limit Monitoring
└─────────────────────────┘
```

---

## 🤖 ML Anomaly Detection — 3-Layer Ensemble

### Layer 1: Isolation Forest (Unsupervised ML)
- 200 decision tree estimators with auto-sampling
- Trained on 16 engineered features (price, spread, volume, staleness, quality indicators)
- Catches **novel and unknown** anomaly patterns that rules would miss

### Layer 2: Rule-Based Engine (Domain Knowledge)
- Negative price detection
- Crossed bid/ask market identification
- Missing quote flagging
- Stale price detection (unchanged for 3+ days)
- Zero volume alerts

### Layer 3: Statistical Tests
- Z-score threshold (> 3σ) for price spike detection
- IQR-based outlier identification
- Rolling 20-day window analysis for distributional shifts

### Why an Ensemble?
Each layer covers a different gap:
- **ML** catches things we haven't seen before
- **Rules** catch things we know are always wrong
- **Statistics** catch things that are technically possible but highly unlikely

---

## 📝 SQL Query Library — 9 Production Queries

| # | Query | Category | Key SQL Techniques |
|---|-------|----------|-------------------|
| 1 | Daily Position Summary by Desk | Data Sourcing | `GROUP BY`, `JOIN`, multi-level aggregation |
| 2 | Greeks Exposure Report | Data Sourcing | `SUM`, `ABS`, cross-asset aggregation |
| 3 | Stale Price Detection (3-day) | Data Validation | `LAG()` window function, `PARTITION BY` |
| 4 | Bid-Ask Spread Validation | Data Validation | `CASE WHEN`, null handling, derived flags |
| 5 | Price Spike Detection (Z-score) | Data Validation | `AVG() OVER (ROWS BETWEEN)`, CTEs, variance calc |
| 6 | Data Quality Dashboard | DQ Monitoring | Conditional aggregation, `CASE` severity ranking |
| 7 | Risk Limit Breach Report | DQ Monitoring | Threshold classification, multi-level `CASE` |
| 8 | Stressed VaR Capital Charge | Regulatory | Basel III/IV multiplier logic, desk-level rollup |
| 9 | Data Completeness Check | Regulatory | `CROSS JOIN`, expected vs actual, SLA metrics |

---

## 📈 Dashboard Pages

### 1. Executive Dashboard
- KPI cards: Active Positions, Total VaR, Daily P&L, Data Quality Score, Critical Issues
- VaR by desk (grouped bar chart)
- Data quality flag distribution (donut chart)
- P&L distribution by asset class (box plot)

### 2. Data Quality Monitor
- **Quality Trends** — Severity-coded area chart over time
- **Issue Drill-Down** — Filterable table with severity, status, detection method
- **Completeness** — Expected vs actual records, error rate overlay

### 3. ML Anomaly Detection
- **Detection Results** — Real-time scan with severity and issue type breakdowns
- **Model Performance** — Precision, Recall, F1-Score, Confusion Matrix
- **Architecture** — Visual breakdown of the 3-layer ensemble approach

### 4. SQL Query Lab
- Pre-built query selector with category grouping
- Live SQL execution with results display
- Custom SQL editor (SELECT-only for safety)
- CSV download for any query results
- Full schema reference

### 5. Risk & Limits
- Limit breach scatter plot with hard/soft limit lines
- Capital charge stacked bar chart by desk
- Breach status classification (HARD_BREACH / SOFT_BREACH / WARNING)

### 6. About This Project
- Responsibility-to-implementation mapping table
- Technical stack overview
- Data architecture diagram

---

## 🛠️ Technical Stack

| Component | Technology |
|-----------|-----------|
| **Language** | Python 3.9+ |
| **Data Layer** | SQLite, pandas, numpy |
| **ML Model** | scikit-learn (Isolation Forest, StandardScaler) |
| **SQL** | 9 queries with CTEs, window functions, conditional aggregation |
| **Visualization** | Plotly (interactive charts, heatmaps, scatter plots) |
| **Frontend** | Streamlit |
| **Deployment** | Streamlit Community Cloud |

---

## 📁 Project Structure

```
wells-fargo-dmm-project/
├── app.py                      # Main Streamlit application (6 pages)
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── .streamlit/
│   └── config.toml             # Wells Fargo red/gold theme
├── data/
│   ├── __init__.py
│   ├── generate_data.py        # Synthetic market risk data generator
│   └── market_risk.db          # SQLite database (auto-generated on first run)
├── models/
│   ├── __init__.py
│   ├── anomaly_detector.py     # ML ensemble anomaly detection model
│   └── anomaly_detector.pkl    # Trained model artifact (auto-generated)
└── sql_queries/
    ├── __init__.py
    └── query_library.py        # 9 production SQL queries with metadata
```

---

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/yourusername/market-risk-dmm-engine.git
cd market-risk-dmm-engine

# Install dependencies
pip install -r requirements.txt

# Run the application (database auto-generates on first run)
streamlit run app.py
```

The database and ML model are generated automatically on first launch — no manual setup required.

---

## 🔗 Role Alignment

| Wells Fargo DMM Responsibility | Project Implementation |
|---|---|
| Sourcing, validating, maintaining risk data | Full data pipeline: 150 instruments, 37K+ records, 5 asset classes |
| Deliver data and reporting solutions | 6-page interactive dashboard with drill-down capabilities |
| Strategic data initiatives | ML-powered anomaly detection with 3-layer ensemble |
| Analyze data, identify issues, root causes | SQL query library + anomaly severity classification |
| Control frameworks | Automated rules + ML + statistical thresholds |
| Regulatory reporting & capital processes | Basel III/IV capital charges, stressed VaR, completeness SLAs |
| Cross-functional collaboration | Designed for front office, risk managers, and technology teams |
| Technical guidance & mentorship | Clean, documented, modular codebase with clear architecture |

---

## 📄 License

This project is built for educational and interview demonstration purposes.

---

**Author:** [Your Name]  
**LinkedIn:** [Your LinkedIn]  
**Built for:** Wells Fargo — Lead Market Risk Officer, DMM Team

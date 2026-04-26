# Argentina Portfolio Risk Intelligence Pipeline

A Big Data analytics project combining Python notebooks, BigQuery, and dbt to analyze Argentine equities and portfolios from data ingestion through investor-facing outputs. Built with modular architecture and business-logic separation.

## 1. Project Overview

This project analyzes Argentine stocks and investor portfolios to evaluate risk, return, diversification, and downside exposure. It brings together raw market data, financial metrics, optimization methods, and Monte Carlo simulation into a reproducible analytics lifecycle.

**Key Value Propositions:**
- Modular three-notebook architecture with clear separation of concerns
- Business-logic-driven feature engineering and optimization
- Hybrid warehouse design combining notebook-driven analytics with dbt-managed transformations
- Investor-ready outputs: risk profiles, portfolio comparisons, and scenario analysis
- Academic rigor through testing, documentation, and lineage tracking

The pipeline addresses investment questions such as:
- Which Argentine equities offer the best historical risk-return tradeoff?
- How do MERVAL exposure, FX sensitivity, and factor behavior affect portfolio outcomes?
- How concentrated or diversified is a given portfolio?
- What is the downside risk and tail behavior under Monte Carlo simulation?
- Which portfolios align with conservative, balanced, or aggressive investor profiles?

## 2. Architecture and Philosophy

### Three-Notebook Modular Design

The project is organized into three focused Jupyter notebooks that run sequentially:

1. **Notebook 1: ETL and Data Landing** (`01_merval_analysis.ipynb`)
   - Downloads equity and factor data from yfinance
   - Standardizes schemas for raw and base layers
   - Uploads to BigQuery raw and processed datasets

2. **Notebook 2: Feature Engineering and Metrics** (`02_transform_features_metrics.ipynb`)
   - Reads base tables from BigQuery
   - Builds daily returns and factor features
   - Creates stock metrics (volatility, Sharpe, beta, etc.)
   - Computes correlations and risk classifications
   - Writes processed layer back to BigQuery

3. **Notebook 3: Basket-Horizon Optimization and Analytics** (`03_transform_serve_montecarlo.ipynb`)
   - Defines three investment baskets and three time horizons
   - Optimizes portfolio weights using four methods
   - Runs Monte Carlo simulation for downside analysis
   - Generates investor profiles and risk rankings
   - Publishes analytics layer and visualization outputs

### Warehouse Layers

```
raw_market              → raw prices, untransformed
├─ stock_prices
└─ factor_prices

processed_market        → notebook-engineered features
├─ base_stock_prices
├─ base_factor_prices
├─ asset_returns
├─ factor_returns
├─ stock_metrics
├─ beta_metrics
└─ correlation_matrix_long

analytics_market        → scenario and simulation outputs
├─ basket_definitions
├─ horizon_definitions
├─ basket_horizon_weights
├─ basket_horizon_metrics
├─ basket_horizon_contributions
└─ monte_carlo_summary

dbt_processed_market    → dbt-owned staging and intermediate models
└─ [dbt staging and intermediate logic]

serving_market          → final investor-facing marts
└─ [dbt serving models]
```

## 3. Tools and Technologies

| Tool | Purpose | Why Chosen |
| --- | --- | --- |
| Python | Core language for ETL, feature engineering, optimization | Flexible for financial workflows and data science |
| Jupyter notebooks | Interactive development and documentation | Blend of code, narrative, and iterative analysis |
| pandas | Tabular data and time-series manipulation | Industry standard for financial data wrangling |
| numpy | Numerical and array operations | Efficient computation for Monte Carlo simulation |
| yfinance | Market data ingestion | Fast, accessible, practical for academic use |
| statsmodels | Statistical analysis and regression | Beta, correlation, and CAPM-style calculations |
| scipy.optimize | Portfolio optimization algorithms | Max Sharpe, min volatility, risk parity |
| matplotlib | Chart generation | Report-ready visualizations |
| BigQuery | Cloud warehouse for data storage and SQL | Scalable, fits Big Data coursework requirements |
| dbt Core | Transformation and governance layer | Modular SQL, testing, documentation, lineage |
| Google Cloud | Authentication and project infrastructure | Integrated with BigQuery |

## 4. Project Structure

```
bigdata-merval-analysis/
├── 01_merval_analysis.ipynb              # ETL and data landing
├── 02_transform_features_metrics.ipynb   # Feature engineering
├── 03_transform_serve_montecarlo.ipynb   # Optimization and simulation
├── README.md                             # This file
├── requirements.txt                      # Python dependencies
├── dbt_project.yml                       # dbt configuration
├── profiles.yml                          # dbt BigQuery profile
├── .user.yml                             # User configuration
├── src/                                  # Shared Python utilities
│   ├── __init__.py
│   ├── bq_utils.py                       # BigQuery client and upload helpers
│   ├── config.py                         # Configuration, symbols, datasets
│   ├── portfolio_utils.py                # Metrics, optimization, classification
│   ├── simulation_utils.py               # Monte Carlo simulation
│   └── visualization_utils.py            # Chart generation
├── models/                               # dbt models (staged for future use)
│   ├── staging/
│   ├── intermediate/
│   └── marts/
├── macros/                               # dbt macros (custom tests, helpers)
├── tests/                                # dbt tests (singular)
├── outputs/                              # Generated artifacts
│   └── charts/
│       ├── overview_heatmap.png
│       ├── investor_profile_overview.png
│       └── [basket-horizon fact sheets]
└── logs/                                 # dbt execution logs
```

### Key Directories

| Directory | Purpose |
| --- | --- |
| `src/` | Shared Python utilities imported by all notebooks |
| `models/` | dbt models for transformation and serving |
| `outputs/charts/` | Generated visualization files from notebooks |
| `logs/` | dbt execution and debugging logs |
| `target/` | Generated dbt artifacts (manifest, catalog, docs) |

## 5. File-by-File Breakdown

### Root Configuration Files

| File | Purpose |
| --- | --- |
| `requirements.txt` | Python package dependencies |
| `dbt_project.yml` | dbt project configuration and variables |
| `profiles.yml` | dbt connection profile for BigQuery |
| `.user.yml` | User-specific settings |

### Python Utility Modules

| Module | Key Functions |
| --- | --- |
| `src/bq_utils.py` | `get_bigquery_client()`, `upload_dataframe()`, `query_to_dataframe()` |
| `src/config.py` | Datasets, table names, symbols, horizons, baskets, optimization methods |
| `src/portfolio_utils.py` | `annualize_return()`, `sharpe_ratio()`, `compute_beta()`, `optimize_max_sharpe()`, `optimize_min_volatility()`, `optimize_risk_parity()`, `classify_stock()`, `evaluate_portfolio()`, `attach_risk_profiles()` |
| `src/simulation_utils.py` | `run_monte_carlo()` for path generation and metrics |
| `src/visualization_utils.py` | `save_overview_heatmap()`, `save_investor_profile_overview()`, `save_basket_horizon_fact_sheet()` |

## 6. Notebook Workflow

### Notebook 1: ETL and Data Landing

**Location:** `01_merval_analysis.ipynb`

**Inputs:**
- yfinance symbols defined in `src/config.py` (EQUITY_SYMBOLS, FACTOR_SYMBOLS)

**Outputs:**
- `raw_market.stock_prices`
- `raw_market.factor_prices`
- `processed_market.base_stock_prices`
- `processed_market.base_factor_prices`

**Key Steps:**
1. Download equity and factor prices from yfinance
2. Flatten and standardize column names
3. Add metadata (run_id, ingestion_timestamp)
4. Upload to BigQuery with WRITE_TRUNCATE mode

**Run Time:** ~2-5 minutes

---

### Notebook 2: Feature Engineering and Metrics

**Location:** `02_transform_features_metrics.ipynb`

**Inputs:**
- `processed_market.base_stock_prices`
- `processed_market.base_factor_prices`

**Outputs:**
- `processed_market.asset_returns` (log returns with factor alignments)
- `processed_market.factor_returns` (factor log returns)
- `processed_market.stock_metrics` (15 metrics per stock)
- `processed_market.beta_metrics` (beta exposures)
- `processed_market.correlation_matrix_long` (pairwise correlations)

**Key Metrics Created:**
- **Return Quality:** average_return, volatility, sharpe_ratio, sortino_ratio, calmar_ratio
- **Risk Exposure:** beta_vs_merval, beta_vs_eem, corr_with_merval, corr_with_fx
- **Downside:** max_drawdown, downside_frequency
- **Classification:** stock_type (growth, balanced, aggressive)

**Run Time:** ~3-7 minutes

---

### Notebook 3: Basket-Horizon Optimization and Analytics

**Location:** `03_transform_serve_montecarlo.ipynb`

**Inputs:**
- `processed_market.asset_returns`
- `processed_market.stock_metrics`
- `processed_market.beta_metrics`
- Basket and horizon definitions from `src/config.py`

**Outputs:**
- `analytics_market.basket_definitions` (3 baskets with macro rationale)
- `analytics_market.horizon_definitions` (3 horizons with weight bounds)
- `analytics_market.basket_horizon_weights` (optimal weights per method)
- `analytics_market.basket_horizon_metrics` (15+ metrics per basket-horizon-method)
- `analytics_market.basket_horizon_contributions` (return and risk contributions)
- `analytics_market.monte_carlo_summary` (simulation metrics)
- PNG charts saved to `outputs/charts/`

**Key Analysis:**
- **3×3 Basket-Horizon Matrix:** 3 baskets × 3 horizons × 4 methods = 36 portfolio configurations
- **Four Optimization Methods:**
  - Equal Weight (baseline)
  - Maximum Sharpe Ratio
  - Minimum Volatility
  - Risk Parity
- **Simulation:** 5,000 Monte Carlo paths per configuration to estimate VaR, CVaR, and probability of loss
- **Risk Profiling:** Classify each configuration as conservative/balanced/aggressive based on volatility and Sharpe
- **Investor Recommendations:** Rank methods by selection score and match investor profiles

**Run Time:** ~5-15 minutes (depends on simulation parameters)

## 7. Key Business Logic and Concepts

### Baskets

Three investment baskets are defined in `src/config.py` with macro rationale:

1. **Short-Term Tactical Basket**
   - Financials and energy-sensitive names
   - For tactical/high-beta positioning
   - Tickers: GGAL, BMA, YPFD, EDN, TRAN, LOMA, BYMA

2. **Medium-Term Structural Basket**
   - Balanced exposure across sectors
   - For medium-horizon allocation
   - Tickers: GGAL, CEPU, DISC, TRAN, BYMA, MIRG, CREO

3. **Long-Term Conservative Basket**
   - Lower-beta, dividend-focused names
   - For structural, long-horizon positioning
   - Tickers: GGAL, CEPU, TRAN, MIRG, CREO

### Horizons

Three investment horizons with matching time scales:

| Horizon | Lookback | Evaluation | Simulation | Max Weight | Profile |
| --- | --- | --- | --- | --- | --- |
| **Short** | 126 days (6 months) | 63 days (3 months) | 63 days | 0.40 | Aggressive |
| **Medium** | 252 days (1 year) | 126 days (6 months) | 126 days | 0.35 | Balanced |
| **Long** | 504 days (2 years) | 252 days (1 year) | 252 days | 0.30 | Conservative |

### Optimization Methods

**Equal Weight:** Baseline 1/N allocation
**Max Sharpe:** Maximize risk-adjusted return
**Min Volatility:** Minimize portfolio variance
**Risk Parity:** Equal risk contribution from each asset

### Risk Classification

Stocks are classified based on average return, volatility, and Sharpe ratio:
- **Growth:** High return, moderate volatility, good Sharpe
- **Balanced:** Moderate return and volatility, positive Sharpe
- **Aggressive:** High return, high volatility, may have lower Sharpe

Portfolio risk profiles (conservative/balanced/aggressive) are assigned based on resulting volatility and expected return.

## 8. How to Run the Project

### Prerequisites

- Python 3.11 or 3.12
- Google Cloud access with BigQuery project
- dbt Core and dbt BigQuery adapter
- ~2 GB free disk space for notebooks and outputs

### Step 1: Environment Setup

```bash
# Create virtual environment
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Google Cloud Authentication

Option A: Application Default Credentials
```bash
gcloud auth application-default login
```

Option B: Service Account
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

### Step 3: Configure dbt

Update `profiles.yml` with your BigQuery project details:
```yaml
merval_analysis:
  target: dev
  outputs:
    dev:
      type: bigquery
      project: your-gcp-project-id
      dataset: dbt_processed_market
      threads: 4
      location: US
      method: service-account-json
      keyfile: /path/to/service-account.json
```

### Step 4: Run Notebooks in Order

```bash
# Start Jupyter
jupyter notebook

# Then run in this order:
# 1. 01_merval_analysis.ipynb
# 2. 02_transform_features_metrics.ipynb
# 3. 03_transform_serve_montecarlo.ipynb
```

### Step 5: Run dbt (Optional)

```bash
# Validate connection
dbt debug --profiles-dir .

# Run transformations
dbt build --profiles-dir .

# Generate and serve documentation
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

## 9. Outputs and Deliverables

### BigQuery Tables

**Raw Layer:**
- `raw_market.stock_prices` – historical prices
- `raw_market.factor_prices` – factor proxies (MERVAL, USDARS, VIX, EEM, US10Y)

**Processed Layer:**
- `processed_market.asset_returns` – daily log returns with factor alignments
- `processed_market.stock_metrics` – 15 financial metrics per stock
- `processed_market.correlation_matrix_long` – pairwise correlations

**Analytics Layer:**
- `analytics_market.basket_horizon_metrics` – 36 portfolio configurations with 25+ metrics
- `analytics_market.basket_horizon_contributions` – return and risk attribution
- `analytics_market.monte_carlo_summary` – simulation metrics (VaR, CVaR, probability of loss)

**dbt Serving Layer** (optional):
- `serving_market.mart_*` – final investor-facing marts

### Visualization Outputs

Saved to `outputs/charts/`:
- `overview_heatmap.png` – Sharpe ratio heatmap across 3×3 matrix
- `investor_profile_overview.png` – Risk profiles and method rankings
- `basket_horizon_fact_sheets/` – Detailed one-pagers per basket-horizon

### Data Dictionary

A comprehensive metrics reference table explains all 25+ metrics in `basket_horizon_metrics`.

## 10. Project Strengths

- **Modular Design:** Clear separation between ingestion, feature engineering, and analytics
- **Business Logic Visibility:** Baskets, horizons, and optimization methods explicitly documented
- **Investor-Ready Outputs:** Rankings, contributions, and risk profiles for decision support
- **Scalable Architecture:** BigQuery scales to larger universes; dbt enables governance
- **Simulation Integration:** Monte Carlo provides downside and tail-risk insights
- **Academic Rigor:** Tested calculations, documented assumptions, reproducible runs

## 11. Limitations and Future Improvements

### Current Limitations

- Some upstream tables remain notebook-dependent (candidates for full dbt implementation)
- Portfolio definitions are hardcoded in config; could be replaced with dynamic input tables
- Simulation uses simplified assumptions (normal returns, no jumps or regime changes)
- No live dashboard or scheduled orchestration
- Coverage limited to selected equities and factors

### Possible Extensions

- Add broader macro and inflation-adjusted factors
- Implement dynamic portfolio input interface
- Stress test scenarios beyond Monte Carlo
- Support multiple portfolios or user-submitted allocations
- Add Tableau or Looker dashboard layer
- Schedule daily/weekly notebook runs with Cloud Scheduler

## 12. Suggested Review Path

For fastest understanding:

1. **Architecture:** Read sections 2–4 of this README
2. **Notebook 1:** Open `01_merval_analysis.ipynb` – see data ingestion and schema
3. **Notebook 2:** Open `02_transform_features_metrics.ipynb` – see feature definitions
4. **Notebook 3:** Open `03_transform_serve_montecarlo.ipynb` – see optimization logic and outputs
5. **Config:** Review `src/config.py` – understand baskets, horizons, methods
6. **Utilities:** Skim `src/portfolio_utils.py` – see metric and optimization implementations
7. **Outputs:** Check `outputs/charts/` and review dbt docs (if generated)

## 13. Summary

The Argentina Portfolio Risk Intelligence Pipeline is a three-notebook Big Data analytics project that combines Python, BigQuery, and dbt to study Argentine equities and portfolios. It demonstrates modern data engineering practices—modular design, warehouse layers, business logic separation—applied to financial analytics. The pipeline is designed to be reproducible, defensible, and ready for academic review or production enhancement.

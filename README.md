# bigdata-merval-analysis

Production-style Big Data pipeline for Argentine market analytics using Python, BigQuery, and dbt Core.

## What this project includes
- Two-stage Jupyter notebook workflow covering ETL plus post-ETL transformation and serving
- Ingestion, feature engineering, stock metrics, portfolio analytics, Monte Carlo simulation, and BigQuery uploads
- Layered warehouse design: `raw_market`, `processed_market`, `analytics_market`
- dbt Core + dbt-bigquery scaffold with sources, staging models, intermediate models, marts, and tests

## Project objective
This repository supports the university assignment **Argentina Portfolio Risk Intelligence Pipeline**.

The goal is to help a user evaluate Argentine equity portfolios using:
- stock returns
- MERVAL market returns
- FX-adjusted returns
- beta and CAPM-style metrics
- volatility and Sharpe ratio
- correlations and drawdown
- Monte Carlo simulation
- VaR and CVaR

## Notebook workflow
This project now has two notebooks that should be run in sequence.

### 1. ETL notebook
- `merval_analysis.ipynb`

This notebook handles the left side of the data pipeline:
- raw market data ingestion
- feature engineering
- processed table generation
- initial portfolio analytics
- BigQuery uploads into the raw and processed layers

### 2. Transformation and serving notebook
- `02_transform_serve_montecarlo.ipynb`

This notebook handles the right side of the data engineering lifecycle:
- validation of processed BigQuery inputs
- portfolio risk-return analytics
- Monte Carlo simulation
- VaR, CVaR, and probability of loss
- portfolio classification
- dashboard-ready analytics tables
- data dictionary generation
- dbt documentation and lineage support

## BigQuery layer design

### Raw layer
- `raw_market.stock_prices`
- `raw_market.merval_prices`
- `raw_market.fx_prices`
- `raw_market.vix_prices`
- `raw_market.eem_prices`
- `raw_market.rf_rates`

### Processed layer
- `processed_market.asset_returns`
- `processed_market.factor_returns`
- `processed_market.stock_metrics`
- `processed_market.beta_metrics`
- `processed_market.correlation_matrix_long`

### Analytics / serving layer
- `analytics_market.portfolio_inputs`
- `analytics_market.portfolio_scenarios`
- `analytics_market.portfolio_contributions`
- `analytics_market.monte_carlo_paths`
- `analytics_market.monte_carlo_summary`
- `analytics_market.stock_rankings`
- `analytics_market.data_dictionary`

## dbt project structure
The dbt project complements the notebooks by making transformations explicit, testable, and documentable.

### Staging models
- `models/staging/stg_stock_prices.sql`
- `models/staging/stg_factor_prices.sql`
- `models/staging/stg_asset_returns.sql`

### Intermediate models
- `models/intermediate/int_asset_returns.sql`
- `models/intermediate/int_portfolio_metrics.sql`

### Mart models
- `models/marts/mart_stock_metrics.sql`
- `models/marts/mart_portfolio_scenarios.sql`
- `models/marts/mart_monte_carlo_summary.sql`

### Metadata and tests
- `models/sources.yml`
- `models/schema.yml`
- `macros/accepted_range.sql`

## Recommended folder structure
```text
bigdata-merval-analysis/
├── merval_analysis.ipynb
├── 02_transform_serve_montecarlo.ipynb
├── requirements.txt
├── dbt_project.yml
├── profiles.yml
├── README.md
├── docs/
├── macros/
│   └── accepted_range.sql
└── models/
   ├── schema.yml
   ├── sources.yml
   ├── staging/
   │   ├── stg_stock_prices.sql
   │   ├── stg_factor_prices.sql
   │   └── stg_asset_returns.sql
   ├── intermediate/
   │   ├── int_asset_returns.sql
   │   └── int_portfolio_metrics.sql
   └── marts/
       ├── mart_stock_metrics.sql
       ├── mart_portfolio_scenarios.sql
       └── mart_monte_carlo_summary.sql
```

## Setup instructions
1. Create and activate your virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure BigQuery auth:
   - ADC option: run `gcloud auth application-default login`
   - Service-account option: set `GOOGLE_APPLICATION_CREDENTIALS=YOUR_SERVICE_ACCOUNT_JSON_PATH`
4. Update placeholders in notebook and dbt profile:
   - `YOUR_GCP_PROJECT_ID`
   - `YOUR_SERVICE_ACCOUNT_JSON_PATH`
   - any dataset overrides if you customize `raw_market`, `processed_market`, or `analytics_market`
5. Verify the BigQuery datasets exist and that the ETL notebook has already written the processed tables.
6. Run the notebooks in order:
   - first `merval_analysis.ipynb`
   - then `02_transform_serve_montecarlo.ipynb`
7. Run dbt commands from project root:
   ```bash
   dbt debug --profiles-dir .
   dbt run --profiles-dir .
   dbt test --profiles-dir .
   dbt docs generate --profiles-dir .
   dbt docs serve --profiles-dir .
   ```

## Typical execution flow
1. Run the ETL notebook to ingest market and macro data into BigQuery.
2. Confirm the `processed_market` tables were created successfully.
3. Run the transformation and serving notebook to calculate portfolio analytics and Monte Carlo outputs.
4. Upload the final analytics tables into `analytics_market`.
5. Run dbt to build warehouse models, tests, documentation, and lineage artifacts.

## Main outputs
- Portfolio inputs and scenario tables for a selected Argentine stock portfolio
- Portfolio contributions to return and risk
- Monte Carlo simulation paths and summary statistics
- Probability of loss, VaR 95, and CVaR 95
- Stock ranking outputs and portfolio type classification
- Data dictionary and lineage-ready documentation cells for the assignment
- dbt models and tests for transformation governance

## Notes for university delivery
- The notebooks include comments and defensive checks for missing tables, missing tickers, duplicate rows, and empty inputs.
- Table names and layer naming are kept consistent across Python, BigQuery, and dbt.
- The serving outputs are designed to be reusable in a dashboard, report, or future application.
- The transformation/serving notebook includes dbt command examples, a Mermaid lineage diagram, and a data dictionary section for submission support.

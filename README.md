# bigdata-merval-analysis

Production-style Big Data pipeline for Argentine market analytics using Python, BigQuery, and dbt Core.

## What this project includes
- Jupyter notebook pipeline: ingestion, feature engineering, stock metrics, portfolio analytics, and BigQuery uploads
- Layered warehouse design: `raw_market`, `processed_market`, `analytics_market`
- dbt Core + dbt-bigquery scaffold with sources, staging models, intermediate models, marts, and tests

## Main notebook
- `merval_analysis.ipynb`

The notebook is organized in 13 sections:
1. Title and project overview
2. Imports and configuration
3. BigQuery connection setup (ADC or service account)
4. Raw data ingestion
5. Raw uploads to BigQuery
6. Processed feature engineering
7. Stock-level metrics
8. Correlation matrix
9. Portfolio analytics
10. Portfolio type classification
11. Upload processed/analytics tables
12. dbt layer overview
13. Example dbt terminal commands

## Recommended folder structure
```text
bigdata-merval-analysis/
├── merval_analysis.ipynb
├── requirements.txt
├── dbt_project.yml
├── profiles.yml.template
├── README.md
├── docs/
├── macros/
└── models/
	 ├── schema.yml
	 ├── sources.yml
	 ├── staging/
	 │   ├── stg_stock_prices.sql
	 │   └── stg_factor_prices.sql
	 ├── intermediate/
	 │   └── int_asset_returns.sql
	 └── marts/
		  ├── mart_stock_metrics.sql
		  └── mart_portfolio_scenarios.sql
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
	- `YOUR_BIGQUERY_DATASET` (if you customize dataset names)
	- `YOUR_SERVICE_ACCOUNT_JSON_PATH`
5. Copy profile template into your dbt profiles directory:
	- macOS/Linux: `~/.dbt/profiles.yml`
6. Run dbt commands from project root:
	```bash
	dbt debug
	dbt run
	dbt test
	dbt docs generate
	```

## Notes for university delivery
- The notebook includes comments and defensive checks for empty data and missing columns.
- Table names and layer naming are consistent across Python, BigQuery, and dbt.
- The output tables are designed to be consumable by a future app for risk-return comparison.

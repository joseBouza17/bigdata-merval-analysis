-- Input: processed_market.asset_returns
-- Grain: one row per date and ticker
-- Purpose: standardize processed stock return features used later in optimization, risk, and simulation work.
-- Layer: staging

with source_data as (
    select distinct
        cast(date as date) as date,
        upper(trim(cast(ticker as string))) as ticker,
        cast(close as float64) as close,
        cast(log_return as float64) as log_return,
        cast(usd_adjusted_return as float64) as usd_adjusted_return,
        cast(excess_return as float64) as excess_return,
        cast(MERVAL as float64) as merval_return,
        cast(EEM as float64) as eem_return,
        cast(VIX as float64) as vix_return,
        cast(USDARS as float64) as usdars_return,
        cast(risk_free_daily as float64) as risk_free_daily,
        cast(merval_usd_adjusted_return as float64) as merval_usd_adjusted_return,
        cast(inflation_proxy as float64) as inflation_proxy,
        cast(country_risk_proxy as float64) as country_risk_proxy,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('processed_market', 'asset_returns') }}
)
select *
from source_data
where date is not null
  and ticker is not null
  and log_return is not null

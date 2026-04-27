-- Input: analytics_market.basket_horizon_contributions
-- Grain: one row per basket_horizon_id and ticker
-- Purpose: standardize the asset-level return and risk contribution outputs created in Notebook 3.
-- Layer: staging

with source_data as (
    select distinct
        lower(trim(cast(basket_horizon_id as string))) as basket_horizon_id,
        lower(trim(cast(basket_name as string))) as basket_name,
        cast(basket_label as string) as basket_label,
        lower(trim(cast(horizon_name as string))) as horizon_name,
        cast(horizon_label as string) as horizon_label,
        lower(trim(cast(weighting_method as string))) as weighting_method,
        upper(trim(cast(ticker as string))) as ticker,
        cast(sector as string) as sector,
        lower(trim(cast(stock_type as string))) as stock_type,
        cast(weight as float64) as weight,
        cast(contribution_to_return as float64) as contribution_to_return,
        cast(contribution_to_risk_pct as float64) as contribution_to_risk_pct,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('analytics_market', 'basket_horizon_contributions') }}
)
select *
from source_data
where basket_horizon_id is not null
  and ticker is not null

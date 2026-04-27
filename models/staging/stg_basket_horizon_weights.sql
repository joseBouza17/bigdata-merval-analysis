-- Input: analytics_market.basket_horizon_weights
-- Grain: one row per basket_horizon_id and ticker
-- Purpose: standardize the weight outputs from Notebook 3 so they can be enriched and documented downstream.
-- Layer: staging

with source_data as (
    select distinct
        lower(trim(cast(basket_horizon_id as string))) as basket_horizon_id,
        lower(trim(cast(basket_name as string))) as basket_name,
        lower(trim(cast(horizon_name as string))) as horizon_name,
        lower(trim(cast(weighting_method as string))) as weighting_method,
        upper(trim(cast(ticker as string))) as ticker,
        cast(weight as float64) as weight,
        cast(weight_rank as int64) as weight_rank,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('analytics_market', 'basket_horizon_weights') }}
)
select *
from source_data
where basket_horizon_id is not null
  and ticker is not null

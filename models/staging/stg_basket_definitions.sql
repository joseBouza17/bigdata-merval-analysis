-- Input: analytics_market.basket_definitions
-- Grain: one row per basket_name and ticker
-- Purpose: standardize the static basket membership layer used throughout the serving graph.
-- Layer: staging

with source_data as (
    select distinct
        lower(trim(cast(basket_name as string))) as basket_name,
        cast(basket_label as string) as basket_label,
        upper(trim(cast(ticker as string))) as ticker,
        cast(basket_order as int64) as basket_order,
        cast(sector as string) as sector,
        cast(macro_rationale as string) as macro_rationale,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('analytics_market', 'basket_definitions') }}
)
select *
from source_data
where basket_name is not null
  and ticker is not null

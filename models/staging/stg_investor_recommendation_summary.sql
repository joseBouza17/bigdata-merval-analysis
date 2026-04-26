-- Input: analytics_market.investor_recommendation_summary
-- Grain: one row per recommendation_scope and recommendation_key
-- Purpose: standardize the horizon-winner and investor-profile recommendation outputs for the final serving layer.
-- Layer: staging

with source_data as (
    select distinct
        lower(trim(cast(recommendation_scope as string))) as recommendation_scope,
        lower(trim(cast(recommendation_key as string))) as recommendation_key,
        lower(trim(cast(investor_profile as string))) as investor_profile,
        lower(trim(cast(horizon_name as string))) as horizon_name,
        lower(trim(cast(basket_name as string))) as basket_name,
        lower(trim(cast(weighting_method as string))) as weighting_method,
        lower(trim(cast(basket_horizon_id as string))) as basket_horizon_id,
        cast(recommendation_rank as int64) as recommendation_rank,
        cast(selection_score as float64) as selection_score,
        cast(recommendation_reason as string) as recommendation_reason,
        cast(key_risk as string) as key_risk,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('analytics_market', 'investor_recommendation_summary') }}
)
select *
from source_data
where recommendation_scope is not null
  and recommendation_key is not null

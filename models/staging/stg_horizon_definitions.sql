-- Input: analytics_market.horizon_definitions
-- Grain: one row per horizon_name
-- Purpose: standardize the explicit horizon assumptions that control lookback, evaluation, and simulation windows.
-- Layer: staging

with source_data as (
    select distinct
        lower(trim(cast(horizon_name as string))) as horizon_name,
        cast(horizon_label as string) as horizon_label,
        cast(lookback_days as int64) as lookback_days,
        cast(evaluation_days as int64) as evaluation_days,
        cast(simulation_days as int64) as simulation_days,
        cast(max_weight as float64) as max_weight,
        cast(min_weight as float64) as min_weight,
        cast(investor_profile_anchor as string) as investor_profile_anchor,
        cast(description as string) as description,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('analytics_market', 'horizon_definitions') }}
)
select *
from source_data
where horizon_name is not null

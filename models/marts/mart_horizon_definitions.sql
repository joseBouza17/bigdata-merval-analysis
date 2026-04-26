-- Input: stg_horizon_definitions
-- Grain: one row per horizon_name
-- Purpose: publish the final horizon-definition mart used across the basket-horizon comparison layer.
-- Layer: serving

select
    horizon_name,
    horizon_label,
    lookback_days,
    evaluation_days,
    simulation_days,
    max_weight,
    min_weight,
    investor_profile_anchor,
    description,
    ingestion_timestamp,
    run_id
from {{ ref('stg_horizon_definitions') }}

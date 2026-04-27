-- Input: stg_basket_definitions
-- Grain: one row per basket_name and ticker
-- Purpose: publish the final basket membership mart used by reporting and investor-facing lookups.
-- Layer: serving

select
    basket_name,
    basket_label,
    ticker,
    basket_order,
    sector,
    macro_rationale,
    ingestion_timestamp,
    run_id
from {{ ref('stg_basket_definitions') }}

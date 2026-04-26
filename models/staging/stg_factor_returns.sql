-- Input: processed_market.factor_returns
-- Grain: one row per date and factor_name
-- Purpose: standardize processed factor returns used for market context and beta calculations.
-- Layer: staging

with source_data as (
    select distinct
        cast(date as date) as date,
        upper(trim(cast(factor_name as string))) as factor_name,
        cast(value as float64) as factor_value,
        cast(factor_return as float64) as factor_return,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('processed_market', 'factor_returns') }}
)
select *
from source_data
where date is not null
  and factor_name is not null
  and factor_return is not null

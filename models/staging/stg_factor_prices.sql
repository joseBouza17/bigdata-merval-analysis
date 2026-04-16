-- Staging model for factor prices in long format.
-- This table contains market and macro factors used in risk-return analysis.

select
    cast(date as date) as date,
    cast(factor_name as string) as factor_name,
    cast(value as float64) as factor_value,
    cast(source as string) as source,
    cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
    cast(run_id as string) as run_id
from {{ source('raw_market', 'factor_prices') }}

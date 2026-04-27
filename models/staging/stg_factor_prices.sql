-- Input: raw_market.factor_prices
-- Grain: one row per date and factor_name
-- Purpose: standardize raw factor and macro proxy prices and keep the latest duplicate if the same factor-date pair is reloaded.
-- Layer: staging

with source_data as (
    select
        cast(date as date) as date,
        upper(trim(cast(factor_name as string))) as factor_name,
        cast(value as float64) as factor_value,
        cast(country_risk_proxy as float64) as country_risk_proxy,
        cast(source as string) as source,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('raw_market', 'factor_prices') }}
    where date is not null
      and factor_name is not null
),
deduplicated as (
    select
        *,
        row_number() over (
            partition by date, factor_name
            order by coalesce(ingestion_timestamp, timestamp('1900-01-01')) desc, run_id desc
        ) as row_num
    from source_data
)
select
    date,
    factor_name,
    factor_value,
    country_risk_proxy,
    source,
    ingestion_timestamp,
    run_id
from deduplicated
where row_num = 1

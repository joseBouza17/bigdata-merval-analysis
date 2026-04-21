with source_data as (
    select distinct
        cast(date as date) as date,
        upper(trim(cast(factor_name as string))) as factor_name,
        cast(value as float64) as factor_value,
        cast(factor_return as float64) as factor_return
    from {{ source('processed_market', 'factor_returns') }}
)
select
    date,
    factor_name,
    factor_value,
    factor_return
from source_data
where date is not null
  and factor_name is not null
  and factor_return is not null

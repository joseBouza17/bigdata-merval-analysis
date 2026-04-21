with processed_factor_returns as (
    select
        date,
        factor_name,
        factor_value,
        factor_return
    from {{ ref('stg_factor_returns') }}
),
raw_factor_prices as (
    select
        date,
        factor_name,
        factor_value as raw_factor_value,
        country_risk_proxy
    from {{ ref('stg_factor_prices') }}
)
select
    fr.date,
    fr.factor_name,
    coalesce(fr.factor_value, fp.raw_factor_value) as factor_value,
    fp.raw_factor_value,
    fp.country_risk_proxy,
    fr.factor_return
from processed_factor_returns as fr
left join raw_factor_prices as fp
    on fr.date = fp.date
   and fr.factor_name = fp.factor_name

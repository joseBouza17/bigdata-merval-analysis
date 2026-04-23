with processed_factor_returns as (
    -- The processed factor table carries the daily factor return already aligned
    -- to the notebook's financial logic, so it is the primary source here.
    select
        date,
        factor_name,
        factor_value,
        factor_return
    from {{ ref('stg_factor_returns') }}
),
raw_factor_prices as (
    -- Raw factor values are kept as a traceable fallback and to preserve context
    -- such as the country-risk proxy that is not re-derived in dbt.
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
    -- Using the processed factor value when present keeps the level series
    -- consistent with the engineered daily returns used elsewhere.
    coalesce(fr.factor_value, fp.raw_factor_value) as factor_value,
    fp.raw_factor_value,
    fp.country_risk_proxy,
    fr.factor_return
from processed_factor_returns as fr
left join raw_factor_prices as fp
    on fr.date = fp.date
   and fr.factor_name = fp.factor_name

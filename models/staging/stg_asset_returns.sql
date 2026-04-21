with source_data as (
    select distinct
        cast(date as date) as date,
        upper(trim(cast(ticker as string))) as ticker,
        cast(close as float64) as close,
        cast(log_return as float64) as log_return,
        cast(usd_adjusted_return as float64) as usd_adjusted_return,
        cast(excess_return as float64) as excess_return,
        cast(MERVAL as float64) as merval_return,
        cast(EEM as float64) as eem_return,
        cast(VIX as float64) as vix_return,
        cast(USDARS as float64) as usdars_return,
        cast(risk_free_daily as float64) as risk_free_daily,
        cast(merval_usd_adjusted_return as float64) as merval_usd_adjusted_return,
        cast(inflation_proxy as float64) as inflation_proxy,
        cast(country_risk_proxy as float64) as country_risk_proxy
    from {{ source('processed_market', 'asset_returns') }}
)
select
    date,
    ticker,
    close,
    log_return,
    usd_adjusted_return,
    excess_return,
    merval_return,
    eem_return,
    vix_return,
    usdars_return,
    risk_free_daily,
    merval_usd_adjusted_return,
    inflation_proxy,
    country_risk_proxy
from source_data
where date is not null
  and ticker is not null
  and log_return is not null

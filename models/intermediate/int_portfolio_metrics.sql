-- Intermediate model: portfolio return series and summary metrics.
-- Uses configurable weights so the dbt layer stays aligned with the notebook scenario.

{% set w_ggal = var('w_ggal', 0.25) %}
{% set w_ypfd = var('w_ypfd', 0.25) %}
{% set w_pamp = var('w_pamp', 0.20) %}
{% set w_bma = var('w_bma', 0.15) %}
{% set w_cepu = var('w_cepu', 0.15) %}

with portfolio_weights as (
    select 'GGAL.BA' as ticker, cast({{ w_ggal }} as float64) as weight
    union all
    select 'YPFD.BA' as ticker, cast({{ w_ypfd }} as float64) as weight
    union all
    select 'PAMP.BA' as ticker, cast({{ w_pamp }} as float64) as weight
    union all
    select 'BMA.BA' as ticker, cast({{ w_bma }} as float64) as weight
    union all
    select 'CEPU.BA' as ticker, cast({{ w_cepu }} as float64) as weight
),
selected_returns as (
    select
        r.date,
        r.ticker,
        r.log_return,
        w.weight
    from {{ ref('stg_asset_returns') }} as r
    inner join portfolio_weights as w
        on r.ticker = w.ticker
),
daily_portfolio as (
    select
        date,
        sum(log_return * weight) as portfolio_return
    from selected_returns
    group by date
),
summary as (
    select
        avg(portfolio_return) * 252 as expected_portfolio_return,
        stddev(portfolio_return) * sqrt(252) as portfolio_volatility,
        safe_divide(
            avg(portfolio_return) * 252,
            nullif(stddev(portfolio_return) * sqrt(252), 0)
        ) as weighted_sharpe
    from daily_portfolio
)
select
    d.date,
    d.portfolio_return,
    s.expected_portfolio_return,
    s.portfolio_volatility,
    s.weighted_sharpe
from daily_portfolio as d
cross join summary as s

-- Mart model: example portfolio scenario output from dbt.
-- Uses configurable weights through dbt vars for reproducibility.

{% set w_ggal = var('w_ggal', 0.25) %}
{% set w_ypfd = var('w_ypfd', 0.25) %}
{% set w_pamp = var('w_pamp', 0.20) %}
{% set w_bma = var('w_bma', 0.15) %}
{% set w_cepu = var('w_cepu', 0.15) %}

with selected as (
    select date, ticker, log_return
    from {{ ref('int_asset_returns') }}
    where ticker in ('GGAL.BA', 'YPFD.BA', 'PAMP.BA', 'BMA.BA', 'CEPU.BA')
),
weighted as (
    select
        date,
        sum(
            case ticker
                when 'GGAL.BA' then log_return * {{ w_ggal }}
                when 'YPFD.BA' then log_return * {{ w_ypfd }}
                when 'PAMP.BA' then log_return * {{ w_pamp }}
                when 'BMA.BA' then log_return * {{ w_bma }}
                when 'CEPU.BA' then log_return * {{ w_cepu }}
                else 0
            end
        ) as portfolio_return
    from selected
    group by date
)
select
    date,
    portfolio_return,
    avg(portfolio_return) over () * 252 as expected_portfolio_return,
    stddev(portfolio_return) over () * sqrt(252) as portfolio_volatility
from weighted

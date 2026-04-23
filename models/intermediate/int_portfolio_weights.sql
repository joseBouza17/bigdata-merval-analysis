-- Keep weights parameterized as dbt vars so the same project can be rerun
-- for different portfolio mixes without rewriting the SQL model itself.
{% set portfolio_id = var('portfolio_id', 'argentina_demo_portfolio') %}
{% set w_ggal = var('w_ggal', 0.25) %}
{% set w_ypfd = var('w_ypfd', 0.25) %}
{% set w_pamp = var('w_pamp', 0.20) %}
{% set w_bma = var('w_bma', 0.15) %}
{% set w_cepu = var('w_cepu', 0.15) %}

with weights as (
    select '{{ portfolio_id }}' as portfolio_id, 'GGAL.BA' as ticker, cast({{ w_ggal }} as float64) as weight
    union all
    select '{{ portfolio_id }}' as portfolio_id, 'YPFD.BA' as ticker, cast({{ w_ypfd }} as float64) as weight
    union all
    select '{{ portfolio_id }}' as portfolio_id, 'PAMP.BA' as ticker, cast({{ w_pamp }} as float64) as weight
    union all
    select '{{ portfolio_id }}' as portfolio_id, 'BMA.BA' as ticker, cast({{ w_bma }} as float64) as weight
    union all
    select '{{ portfolio_id }}' as portfolio_id, 'CEPU.BA' as ticker, cast({{ w_cepu }} as float64) as weight
)
select
    portfolio_id,
    ticker,
    weight
from weights
-- Zero-weight names are excluded so downstream joins only expand active holdings.
-- The portfolio weight singular test is responsible for validating that the kept
-- weights still sum to 1.
where weight > 0

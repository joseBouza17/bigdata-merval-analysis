-- Mart model: example portfolio scenario output from dbt.
-- Uses configurable weights through dbt vars for reproducibility.

{% set portfolio_id = var('portfolio_id', 'portfolio_dbt_default') %}

with metrics as (
    select
        date,
        portfolio_return,
        expected_portfolio_return,
        portfolio_volatility,
        weighted_sharpe
    from {{ ref('int_portfolio_metrics') }}
),
portfolio_rollup as (
    select
        '{{ portfolio_id }}' as portfolio_id,
        date,
        portfolio_return,
        expected_portfolio_return,
        portfolio_volatility,
        weighted_sharpe
    from metrics
)
select
    portfolio_id,
    date,
    portfolio_return,
    expected_portfolio_return,
    portfolio_volatility,
    weighted_sharpe
from portfolio_rollup

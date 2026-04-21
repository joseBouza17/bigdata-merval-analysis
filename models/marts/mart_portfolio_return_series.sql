select
    portfolio_id,
    date,
    portfolio_log_return,
    portfolio_usd_adjusted_return,
    portfolio_excess_return,
    cumulative_return,
    cumulative_usd_return,
    drawdown
from {{ ref('int_portfolio_return_series') }}

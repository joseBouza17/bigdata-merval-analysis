-- Mart model: stock-level annualized risk-return metrics.
-- This output is suitable for serving to BI dashboards or APIs.

with base as (
    select
        ticker,
        log_return
    from {{ ref('int_asset_returns') }}
    where log_return is not null
),
metrics as (
    select
        ticker,
        avg(log_return) * 252 as average_return,
        stddev(log_return) * sqrt(252) as volatility,
        safe_divide(avg(log_return) * 252, stddev(log_return) * sqrt(252)) as sharpe_ratio,
        count(*) as observations
    from base
    group by ticker
)
select *
from metrics

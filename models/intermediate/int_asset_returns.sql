-- Intermediate model: calculates daily log returns by stock ticker.
-- Formula: log_return = ln(close_t / close_t-1)

with priced as (
    select
        date,
        ticker,
        close,
        lag(close) over (partition by ticker order by date) as prev_close
    from {{ ref('stg_stock_prices') }}
),
returns as (
    select
        date,
        ticker,
        close,
        case
            when prev_close is null or prev_close = 0 then null
            else ln(close / prev_close)
        end as log_return
    from priced
)
select *
from returns

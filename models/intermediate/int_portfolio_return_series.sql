with daily_returns as (
    select
        portfolio_id,
        date,
        sum(weighted_log_return) as portfolio_log_return,
        sum(weighted_usd_adjusted_return) as portfolio_usd_adjusted_return,
        sum(weighted_excess_return) as portfolio_excess_return
    from {{ ref('int_portfolio_asset_contributions') }}
    group by portfolio_id, date
),
wealth_index as (
    select
        portfolio_id,
        date,
        portfolio_log_return,
        portfolio_usd_adjusted_return,
        portfolio_excess_return,
        exp(
            sum(portfolio_log_return) over (
                partition by portfolio_id
                order by date
                rows between unbounded preceding and current row
            )
        ) as cumulative_wealth_index,
        exp(
            sum(portfolio_usd_adjusted_return) over (
                partition by portfolio_id
                order by date
                rows between unbounded preceding and current row
            )
        ) as cumulative_usd_wealth_index
    from daily_returns
),
drawdowns as (
    select
        portfolio_id,
        date,
        portfolio_log_return,
        portfolio_usd_adjusted_return,
        portfolio_excess_return,
        cumulative_wealth_index - 1 as cumulative_return,
        cumulative_usd_wealth_index - 1 as cumulative_usd_return,
        max(cumulative_wealth_index) over (
            partition by portfolio_id
            order by date
            rows between unbounded preceding and current row
        ) as running_peak
    from wealth_index
)
select
    portfolio_id,
    date,
    portfolio_log_return,
    portfolio_usd_adjusted_return,
    portfolio_excess_return,
    cumulative_return,
    cumulative_usd_return,
    safe_divide(cumulative_return + 1 - running_peak, running_peak) as drawdown
from drawdowns

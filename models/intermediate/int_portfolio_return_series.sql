with daily_returns as (
    -- Roll the asset-level contributions up to one row per portfolio-day before
    -- building cumulative wealth and drawdown statistics.
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
    -- Log returns are additive through time, so cumulative wealth can be rebuilt
    -- as W_t = exp(sum(log_return_1 ... log_return_t)).
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
    -- Drawdown is measured from the running peak of the wealth index instead of
    -- from raw returns so it captures path-dependent downside, not just bad days.
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
    -- Standard drawdown formula: (current_wealth - peak_wealth) / peak_wealth.
    safe_divide(cumulative_return + 1 - running_peak, running_peak) as drawdown
from drawdowns

with return_series as (
    select
        portfolio_id,
        date,
        portfolio_log_return,
        portfolio_excess_return,
        drawdown,
        cumulative_return
    from {{ ref('int_portfolio_return_series') }}
),
summary_metrics as (
    select
        portfolio_id,
        min(date) as start_date,
        max(date) as end_date,
        count(*) as observations,
        avg(portfolio_log_return) * 252 as expected_portfolio_return,
        stddev(portfolio_log_return) * sqrt(252) as portfolio_volatility,
        safe_divide(
            avg(portfolio_excess_return) * 252,
            nullif(stddev(portfolio_log_return) * sqrt(252), 0)
        ) as weighted_sharpe,
        min(drawdown) as max_drawdown,
        array_agg(cumulative_return order by date desc limit 1)[offset(0)] as total_cumulative_return
    from return_series
    group by portfolio_id
),
portfolio_exposures as (
    select
        portfolio_id,
        count(*) as num_assets,
        sum(weight * coalesce(beta_vs_merval, 0)) as weighted_beta_merval,
        sum(weight * coalesce(beta_vs_eem, 0)) as weighted_beta_eem,
        sum(weight * coalesce(notebook_volatility, realized_volatility, 0)) as weighted_average_asset_volatility
    from {{ ref('int_portfolio_asset_metrics') }}
    group by portfolio_id
)
select
    s.portfolio_id,
    s.start_date,
    s.end_date,
    s.observations,
    s.expected_portfolio_return,
    s.portfolio_volatility,
    s.weighted_sharpe,
    s.max_drawdown,
    s.total_cumulative_return,
    e.num_assets,
    e.weighted_beta_merval,
    e.weighted_beta_eem,
    e.weighted_average_asset_volatility
from summary_metrics as s
left join portfolio_exposures as e
    on s.portfolio_id = e.portfolio_id

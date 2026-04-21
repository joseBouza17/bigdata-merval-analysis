with weights as (
    select
        portfolio_id,
        ticker,
        weight
    from {{ ref('int_portfolio_weights') }}
),
realized_history as (
    select
        ticker,
        min(date) as start_date,
        max(date) as end_date,
        count(*) as observations,
        avg(log_return) * 252 as realized_average_return,
        stddev(log_return) * sqrt(252) as realized_volatility,
        safe_divide(
            avg(excess_return) * 252,
            nullif(stddev(log_return) * sqrt(252), 0)
        ) as realized_sharpe
    from {{ ref('int_asset_returns') }}
    group by ticker
)
select
    w.portfolio_id,
    w.ticker,
    w.weight,
    sm.average_return as notebook_average_return,
    sm.volatility as notebook_volatility,
    sm.sharpe_ratio as notebook_sharpe_ratio,
    sm.max_drawdown as notebook_max_drawdown,
    sm.corr_with_merval,
    sm.corr_with_fx,
    sm.stock_type,
    coalesce(sm.beta_vs_merval, bm.beta_vs_merval) as beta_vs_merval,
    coalesce(sm.beta_vs_eem, bm.beta_vs_eem) as beta_vs_eem,
    rh.start_date,
    rh.end_date,
    rh.observations,
    rh.realized_average_return,
    rh.realized_volatility,
    rh.realized_sharpe
from weights as w
left join {{ ref('stg_stock_metrics') }} as sm
    on w.ticker = sm.ticker
left join {{ ref('stg_beta_metrics') }} as bm
    on w.ticker = bm.ticker
left join realized_history as rh
    on w.ticker = rh.ticker

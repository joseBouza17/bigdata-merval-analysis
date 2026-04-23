with weights as (
    select
        portfolio_id,
        ticker,
        weight
    from {{ ref('int_portfolio_weights') }}
),
realized_history as (
    -- Recompute realized asset metrics from the daily return history so the
    -- portfolio layer has a dbt-native fallback when notebook summary fields
    -- are missing. Annualization assumes 252 trading days.
    select
        ticker,
        min(date) as start_date,
        max(date) as end_date,
        count(*) as observations,
        -- Annualized mean return = average daily log return * 252.
        avg(log_return) * 252 as realized_average_return,
        -- Annualized volatility = stddev(daily return) * sqrt(252).
        stddev(log_return) * sqrt(252) as realized_volatility,
        safe_divide(
            -- Sharpe ratio = annualized excess return / annualized volatility.
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
    -- Notebook metrics are preferred first so dbt mirrors the numbers already
    -- shown in the academic analysis, while realized_history preserves coverage
    -- if a notebook export is incomplete.
    sm.average_return as notebook_average_return,
    sm.volatility as notebook_volatility,
    sm.sharpe_ratio as notebook_sharpe_ratio,
    sm.max_drawdown as notebook_max_drawdown,
    sm.corr_with_merval,
    sm.corr_with_fx,
    sm.stock_type,
    -- Beta values are sourced from the stock metrics export when available and
    -- otherwise backfilled from the dedicated beta staging table.
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

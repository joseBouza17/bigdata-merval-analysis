-- Input: stg_stock_metrics, stg_beta_metrics, and int_asset_returns
-- Grain: one row per ticker
-- Purpose: publish the final stock-level metrics mart used for basket interpretation and presentation.
-- Layer: serving

with return_history as (
    select
        ticker,
        count(*) as observations,
        min(date) as start_date,
        max(date) as end_date,
        array_agg(close order by date desc limit 1)[offset(0)] as latest_close,
        array_agg(adj_close order by date desc limit 1)[offset(0)] as latest_adj_close,
        array_agg(volume order by date desc limit 1)[offset(0)] as latest_volume
    from {{ ref('int_asset_returns') }}
    group by ticker
)
select
    sm.ticker,
    sm.average_return,
    sm.volatility,
    sm.sharpe_ratio,
    sm.sortino_ratio,
    sm.calmar_ratio,
    sm.downside_frequency,
    sm.max_drawdown,
    coalesce(sm.beta_vs_merval, bm.beta_vs_merval) as beta_vs_merval,
    coalesce(sm.beta_vs_eem, bm.beta_vs_eem) as beta_vs_eem,
    sm.corr_with_merval,
    sm.corr_with_fx,
    sm.stock_type,
    rh.observations,
    rh.start_date,
    rh.end_date,
    rh.latest_close,
    rh.latest_adj_close,
    rh.latest_volume
from {{ ref('stg_stock_metrics') }} as sm
left join {{ ref('stg_beta_metrics') }} as bm
    on sm.ticker = bm.ticker
left join return_history as rh
    on sm.ticker = rh.ticker

with return_history as (
    -- Add the observation window and latest market levels so the final stock
    -- metrics table can support both ranking and report/dashboard annotation.
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
),
enriched_metrics as (
    select
        sm.ticker,
        sm.average_return,
        sm.volatility,
        sm.sharpe_ratio,
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
    -- Prefer the metrics export as the primary source and only backfill missing betas.
    left join {{ ref('stg_beta_metrics') }} as bm
        on sm.ticker = bm.ticker
    left join return_history as rh
        on sm.ticker = rh.ticker
)
select *
from enriched_metrics

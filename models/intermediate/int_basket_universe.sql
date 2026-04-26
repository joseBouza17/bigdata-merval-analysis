-- Input: stg_basket_definitions, stg_stock_metrics, and stg_beta_metrics
-- Grain: one row per basket_name and ticker
-- Purpose: enrich the canonical basket memberships with stock-level performance and beta context.
-- Layer: intermediate

with basket_assets as (
    select
        basket_name,
        basket_label,
        ticker,
        basket_order,
        sector,
        macro_rationale
    from {{ ref('stg_basket_definitions') }}
)
select
    b.basket_name,
    b.basket_label,
    b.ticker,
    b.basket_order,
    b.sector,
    b.macro_rationale,
    sm.average_return,
    sm.volatility,
    sm.sharpe_ratio,
    sm.sortino_ratio,
    sm.calmar_ratio,
    sm.downside_frequency,
    sm.max_drawdown,
    sm.corr_with_merval,
    sm.corr_with_fx,
    sm.stock_type,
    coalesce(sm.beta_vs_merval, bm.beta_vs_merval) as beta_vs_merval,
    coalesce(sm.beta_vs_eem, bm.beta_vs_eem) as beta_vs_eem
from basket_assets as b
left join {{ ref('stg_stock_metrics') }} as sm
    on b.ticker = sm.ticker
left join {{ ref('stg_beta_metrics') }} as bm
    on b.ticker = bm.ticker

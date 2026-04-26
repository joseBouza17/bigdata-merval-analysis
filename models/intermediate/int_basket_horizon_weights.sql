-- Input: stg_basket_horizon_weights, int_basket_universe, and stg_horizon_definitions
-- Grain: one row per basket_horizon_id and ticker
-- Purpose: enrich each optimized weight with basket context, horizon assumptions, and stock metrics.
-- Layer: intermediate

with weights as (
    select
        basket_horizon_id,
        basket_name,
        horizon_name,
        weighting_method,
        ticker,
        weight,
        weight_rank,
        ingestion_timestamp,
        run_id
    from {{ ref('stg_basket_horizon_weights') }}
),
basket_context as (
    select
        basket_name,
        basket_label,
        ticker,
        basket_order,
        sector,
        macro_rationale,
        average_return,
        volatility,
        sharpe_ratio,
        sortino_ratio,
        calmar_ratio,
        downside_frequency,
        max_drawdown,
        corr_with_merval,
        corr_with_fx,
        stock_type,
        beta_vs_merval,
        beta_vs_eem
    from {{ ref('int_basket_universe') }}
),
horizon_context as (
    select
        horizon_name,
        horizon_label,
        lookback_days,
        evaluation_days,
        simulation_days,
        max_weight,
        min_weight,
        investor_profile_anchor,
        description
    from {{ ref('stg_horizon_definitions') }}
)
select
    w.basket_horizon_id,
    w.basket_name,
    b.basket_label,
    w.horizon_name,
    h.horizon_label,
    w.weighting_method,
    w.ticker,
    b.basket_order,
    b.sector,
    b.macro_rationale,
    h.lookback_days,
    h.evaluation_days,
    h.simulation_days,
    h.max_weight,
    h.min_weight,
    h.investor_profile_anchor,
    h.description as horizon_description,
    w.weight,
    w.weight_rank,
    b.average_return,
    b.volatility,
    b.sharpe_ratio,
    b.sortino_ratio,
    b.calmar_ratio,
    b.downside_frequency,
    b.max_drawdown,
    b.corr_with_merval,
    b.corr_with_fx,
    b.stock_type,
    b.beta_vs_merval,
    b.beta_vs_eem,
    w.ingestion_timestamp,
    w.run_id
from weights as w
left join basket_context as b
    on w.basket_name = b.basket_name
   and w.ticker = b.ticker
left join horizon_context as h
    on w.horizon_name = h.horizon_name

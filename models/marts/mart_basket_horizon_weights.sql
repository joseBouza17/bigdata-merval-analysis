-- Input: int_basket_horizon_weights
-- Grain: one row per basket_horizon_id and ticker
-- Purpose: publish the final weight table with basket, horizon, and stock-level context in one place.
-- Layer: serving

select
    basket_horizon_id,
    basket_name,
    basket_label,
    horizon_name,
    horizon_label,
    weighting_method,
    ticker,
    basket_order,
    sector,
    macro_rationale,
    lookback_days,
    evaluation_days,
    simulation_days,
    max_weight,
    min_weight,
    investor_profile_anchor,
    weight,
    weight_rank,
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
    beta_vs_eem,
    ingestion_timestamp,
    run_id
from {{ ref('int_basket_horizon_weights') }}

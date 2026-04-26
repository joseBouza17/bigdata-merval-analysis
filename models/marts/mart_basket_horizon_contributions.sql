-- Input: stg_basket_horizon_contributions and mart_basket_horizon_metrics
-- Grain: one row per basket_horizon_id and ticker
-- Purpose: publish the final contribution mart that explains which stocks drive basket return and basket risk.
-- Layer: serving

with contributions as (
    select
        basket_horizon_id,
        basket_name,
        basket_label,
        horizon_name,
        horizon_label,
        weighting_method,
        ticker,
        sector,
        stock_type,
        weight,
        contribution_to_return,
        contribution_to_risk_pct,
        ingestion_timestamp,
        run_id
    from {{ ref('stg_basket_horizon_contributions') }}
),
metrics as (
    select
        basket_horizon_id,
        risk_profile,
        risk_profile_reason,
        expected_portfolio_return,
        portfolio_volatility,
        weighted_sharpe,
        macro_rationale,
        macro_risk_note
    from {{ ref('mart_basket_horizon_metrics') }}
)
select
    c.basket_horizon_id,
    c.basket_name,
    c.basket_label,
    c.horizon_name,
    c.horizon_label,
    c.weighting_method,
    c.ticker,
    c.sector,
    c.stock_type,
    c.weight,
    c.contribution_to_return,
    c.contribution_to_risk_pct,
    m.risk_profile,
    m.risk_profile_reason,
    m.expected_portfolio_return,
    m.portfolio_volatility,
    m.weighted_sharpe,
    m.macro_rationale,
    m.macro_risk_note,
    c.ingestion_timestamp,
    c.run_id
from contributions as c
left join metrics as m
    on c.basket_horizon_id = m.basket_horizon_id

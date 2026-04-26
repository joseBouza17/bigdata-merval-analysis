-- Input: stg_investor_recommendation_summary and mart_basket_horizon_method_comparison
-- Grain: one row per recommendation_scope and recommendation_key
-- Purpose: publish the final recommendation mart used for horizon-level and investor-profile guidance.
-- Layer: serving

with recommendations as (
    select
        recommendation_scope,
        recommendation_key,
        investor_profile,
        horizon_name,
        basket_name,
        weighting_method,
        basket_horizon_id,
        recommendation_rank,
        selection_score,
        recommendation_reason,
        key_risk,
        ingestion_timestamp,
        run_id
    from {{ ref('stg_investor_recommendation_summary') }}
),
comparison as (
    select
        basket_horizon_id,
        basket_label,
        horizon_label,
        expected_portfolio_return,
        portfolio_volatility,
        weighted_sharpe,
        probability_of_loss,
        var_95,
        cvar_95,
        concentration_risk_hhi,
        dbt_diversification_ratio,
        risk_profile,
        risk_profile_reason,
        macro_rationale,
        macro_risk_note
    from {{ ref('mart_basket_horizon_method_comparison') }}
)
select
    r.recommendation_scope,
    r.recommendation_key,
    r.investor_profile,
    r.horizon_name,
    c.horizon_label,
    r.basket_name,
    c.basket_label,
    r.weighting_method,
    r.basket_horizon_id,
    r.recommendation_rank,
    r.selection_score,
    r.recommendation_reason,
    r.key_risk,
    c.expected_portfolio_return,
    c.portfolio_volatility,
    c.weighted_sharpe,
    c.probability_of_loss,
    c.var_95,
    c.cvar_95,
    c.concentration_risk_hhi,
    c.dbt_diversification_ratio,
    c.risk_profile,
    c.risk_profile_reason,
    c.macro_rationale,
    c.macro_risk_note,
    r.ingestion_timestamp,
    r.run_id
from recommendations as r
left join comparison as c
    on r.basket_horizon_id = c.basket_horizon_id

-- Input: stg_monte_carlo_summary and mart_basket_horizon_metrics
-- Grain: one row per basket_horizon_id
-- Purpose: publish the final simulation summary mart with enough context to compare downside behavior across basket-horizon winners.
-- Layer: serving

with simulation as (
    select
        basket_horizon_id,
        basket_name,
        horizon_name,
        weighting_method,
        initial_value,
        num_simulations,
        simulation_days,
        mean_final_value,
        median_final_value,
        min_final_value,
        max_final_value,
        percentile_5,
        percentile_25,
        percentile_75,
        percentile_95,
        probability_of_loss,
        expected_return_simulated,
        var_95,
        cvar_95,
        max_drawdown_p50,
        max_drawdown_p95,
        ingestion_timestamp,
        run_id
    from {{ ref('stg_monte_carlo_summary') }}
),
context as (
    select
        basket_horizon_id,
        basket_label,
        horizon_label,
        expected_portfolio_return,
        portfolio_volatility,
        weighted_sharpe,
        concentration_risk_hhi,
        dbt_diversification_ratio,
        risk_profile,
        macro_rationale,
        macro_risk_note
    from {{ ref('mart_basket_horizon_metrics') }}
)
select
    s.basket_horizon_id,
    s.basket_name,
    c.basket_label,
    s.horizon_name,
    c.horizon_label,
    s.weighting_method,
    s.initial_value,
    s.num_simulations,
    s.simulation_days,
    s.mean_final_value,
    s.median_final_value,
    s.min_final_value,
    s.max_final_value,
    s.percentile_5,
    s.percentile_25,
    s.percentile_75,
    s.percentile_95,
    s.probability_of_loss,
    s.expected_return_simulated,
    s.var_95,
    s.cvar_95,
    s.max_drawdown_p50,
    s.max_drawdown_p95,
    c.expected_portfolio_return,
    c.portfolio_volatility,
    c.weighted_sharpe,
    c.concentration_risk_hhi,
    c.dbt_diversification_ratio,
    c.risk_profile,
    c.macro_rationale,
    c.macro_risk_note,
    s.ingestion_timestamp,
    s.run_id
from simulation as s
left join context as c
    on s.basket_horizon_id = c.basket_horizon_id

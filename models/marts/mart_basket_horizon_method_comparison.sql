-- Input: stg_basket_horizon_method_comparison, mart_basket_horizon_metrics, and mart_monte_carlo_summary
-- Grain: one row per basket_horizon_id
-- Purpose: publish the final method-comparison mart used to identify the best weighting method inside each basket-horizon cell.
-- Layer: serving

with comparison as (
    select
        basket_horizon_id,
        basket_name,
        horizon_name,
        weighting_method,
        selection_score,
        selection_score_version,
        risk_adjusted_score,
        return_score,
        risk_control_score,
        tail_resilience_score,
        diversification_quality_score,
        historical_drawdown_abs,
        var_95_pct_of_initial,
        cvar_95_pct_of_initial,
        simulation_tail_drawdown_abs,
        method_rank_within_basket_horizon,
        basket_rank_within_horizon,
        is_best_method_for_basket_horizon,
        is_best_basket_for_horizon,
        equal_weight_return_delta,
        equal_weight_sharpe_delta,
        equal_weight_probability_of_loss_delta,
        horizon_winner_reason,
        ingestion_timestamp,
        run_id
    from {{ ref('stg_basket_horizon_method_comparison') }}
),
metrics as (
    select
        basket_horizon_id,
        basket_label,
        horizon_label,
        expected_portfolio_return,
        portfolio_volatility,
        weighted_sharpe,
        sortino_ratio,
        calmar_ratio,
        max_drawdown,
        concentration_risk_hhi,
        dbt_diversification_ratio,
        weighted_beta_merval,
        weighted_corr_fx,
        risk_profile,
        risk_profile_reason,
        macro_rationale,
        macro_risk_note
    from {{ ref('mart_basket_horizon_metrics') }}
),
simulation as (
    select
        basket_horizon_id,
        probability_of_loss,
        var_95,
        cvar_95,
        mean_final_value,
        median_final_value
    from {{ ref('mart_monte_carlo_summary') }}
)
select
    c.basket_horizon_id,
    c.basket_name,
    m.basket_label,
    c.horizon_name,
    m.horizon_label,
    c.weighting_method,
    c.selection_score,
    c.selection_score_version,
    c.risk_adjusted_score,
    c.return_score,
    c.risk_control_score,
    c.tail_resilience_score,
    c.diversification_quality_score,
    c.historical_drawdown_abs,
    c.var_95_pct_of_initial,
    c.cvar_95_pct_of_initial,
    c.simulation_tail_drawdown_abs,
    c.method_rank_within_basket_horizon,
    c.basket_rank_within_horizon,
    c.is_best_method_for_basket_horizon,
    c.is_best_basket_for_horizon,
    c.equal_weight_return_delta,
    c.equal_weight_sharpe_delta,
    c.equal_weight_probability_of_loss_delta,
    c.horizon_winner_reason,
    m.expected_portfolio_return,
    m.portfolio_volatility,
    m.weighted_sharpe,
    m.sortino_ratio,
    m.calmar_ratio,
    m.max_drawdown,
    m.concentration_risk_hhi,
    m.dbt_diversification_ratio,
    m.weighted_beta_merval,
    m.weighted_corr_fx,
    m.risk_profile,
    m.risk_profile_reason,
    s.probability_of_loss,
    s.var_95,
    s.cvar_95,
    s.mean_final_value,
    s.median_final_value,
    m.macro_rationale,
    m.macro_risk_note,
    c.ingestion_timestamp,
    c.run_id
from comparison as c
left join metrics as m
    on c.basket_horizon_id = m.basket_horizon_id
left join simulation as s
    on c.basket_horizon_id = s.basket_horizon_id

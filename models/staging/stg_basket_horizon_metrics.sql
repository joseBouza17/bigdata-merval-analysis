-- Input: analytics_market.basket_horizon_metrics
-- Grain: one row per basket_horizon_id
-- Purpose: standardize the historical basket-horizon-method metrics written by Notebook 3.
-- Layer: staging

with source_data as (
    select distinct
        lower(trim(cast(basket_horizon_id as string))) as basket_horizon_id,
        lower(trim(cast(basket_name as string))) as basket_name,
        cast(basket_label as string) as basket_label,
        lower(trim(cast(horizon_name as string))) as horizon_name,
        cast(horizon_label as string) as horizon_label,
        lower(trim(cast(weighting_method as string))) as weighting_method,
        cast(expected_portfolio_return as float64) as expected_portfolio_return,
        cast(portfolio_volatility as float64) as portfolio_volatility,
        cast(weighted_sharpe as float64) as weighted_sharpe,
        cast(sortino_ratio as float64) as sortino_ratio,
        cast(calmar_ratio as float64) as calmar_ratio,
        cast(max_drawdown as float64) as max_drawdown,
        cast(realized_cumulative_return as float64) as realized_cumulative_return,
        cast(downside_deviation as float64) as downside_deviation,
        cast(concentration_risk_hhi as float64) as concentration_risk_hhi,
        cast(effective_number_of_assets as float64) as effective_number_of_assets,
        cast(diversification_ratio as float64) as diversification_ratio,
        cast(diversification_effect as float64) as diversification_effect,
        cast(average_pairwise_correlation as float64) as average_pairwise_correlation,
        cast(weighted_beta_merval as float64) as weighted_beta_merval,
        cast(weighted_beta_eem as float64) as weighted_beta_eem,
        cast(weighted_corr_merval as float64) as weighted_corr_merval,
        cast(weighted_corr_fx as float64) as weighted_corr_fx,
        cast(num_assets as int64) as num_assets,
        cast(lookback_days as int64) as lookback_days,
        cast(evaluation_days as int64) as evaluation_days,
        cast(simulation_days as int64) as simulation_days,
        cast(max_weight as float64) as max_weight,
        cast(min_weight as float64) as min_weight,
        lower(trim(cast(risk_profile as string))) as risk_profile,
        cast(risk_profile_reason as string) as risk_profile_reason,
        cast(macro_risk_note as string) as macro_risk_note,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('analytics_market', 'basket_horizon_metrics') }}
)
select *
from source_data
where basket_horizon_id is not null

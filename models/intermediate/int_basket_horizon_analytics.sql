-- Input: int_basket_horizon_weights, stg_correlation_matrix, and stg_basket_horizon_metrics
-- Grain: one row per basket_horizon_id
-- Purpose: reconcile notebook metrics with dbt-derived concentration, diversification, and exposure checks.
-- Layer: intermediate

with asset_inputs as (
    select
        basket_horizon_id,
        basket_name,
        basket_label,
        horizon_name,
        horizon_label,
        weighting_method,
        ticker,
        weight,
        coalesce(volatility, 0) as asset_volatility,
        coalesce(beta_vs_merval, 0) as beta_vs_merval,
        coalesce(beta_vs_eem, 0) as beta_vs_eem,
        coalesce(corr_with_fx, 0) as corr_with_fx,
        coalesce(corr_with_merval, 0) as corr_with_merval,
        macro_rationale,
        lookback_days,
        evaluation_days,
        simulation_days,
        max_weight,
        min_weight,
        investor_profile_anchor,
        horizon_description
    from {{ ref('int_basket_horizon_weights') }}
),
unique_pairs as (
    select
        a1.basket_horizon_id,
        a1.ticker as ticker_1,
        a2.ticker as ticker_2,
        a1.weight as weight_1,
        a2.weight as weight_2,
        a1.asset_volatility as asset_volatility_1,
        a2.asset_volatility as asset_volatility_2,
        coalesce(c.correlation, case when a1.ticker = a2.ticker then 1 else 0 end) as correlation
    from asset_inputs as a1
    inner join asset_inputs as a2
        on a1.basket_horizon_id = a2.basket_horizon_id
       and a1.ticker <= a2.ticker
    left join {{ ref('stg_correlation_matrix') }} as c
        on a1.ticker = c.ticker_1
       and a2.ticker = c.ticker_2
),
covariance_summary as (
    select
        basket_horizon_id,
        sum(
            case
                when ticker_1 = ticker_2 then power(weight_1 * asset_volatility_1, 2)
                else 2 * weight_1 * weight_2 * asset_volatility_1 * asset_volatility_2 * correlation
            end
        ) as portfolio_variance,
        avg(case when ticker_1 < ticker_2 then correlation end) as dbt_average_pairwise_correlation
    from unique_pairs
    group by basket_horizon_id
),
weight_stats as (
    select
        basket_horizon_id,
        any_value(basket_name) as basket_name,
        any_value(basket_label) as basket_label,
        any_value(horizon_name) as horizon_name,
        any_value(horizon_label) as horizon_label,
        any_value(weighting_method) as weighting_method,
        any_value(macro_rationale) as macro_rationale,
        any_value(lookback_days) as lookback_days,
        any_value(evaluation_days) as evaluation_days,
        any_value(simulation_days) as simulation_days,
        any_value(max_weight) as max_weight,
        any_value(min_weight) as min_weight,
        any_value(investor_profile_anchor) as investor_profile_anchor,
        any_value(horizon_description) as horizon_description,
        count(*) as dbt_num_assets,
        sum(power(weight, 2)) as dbt_weight_concentration_hhi,
        safe_divide(1, sum(power(weight, 2))) as dbt_effective_number_of_assets,
        sum(weight * asset_volatility) as dbt_weighted_average_asset_volatility,
        sum(weight * beta_vs_merval) as dbt_weighted_beta_merval,
        sum(weight * beta_vs_eem) as dbt_weighted_beta_eem,
        sum(weight * corr_with_fx) as dbt_weighted_corr_fx,
        sum(weight * corr_with_merval) as dbt_weighted_corr_merval
    from asset_inputs
    group by basket_horizon_id
),
notebook_metrics as (
    select
        basket_horizon_id,
        expected_portfolio_return,
        portfolio_volatility,
        weighted_sharpe,
        sortino_ratio,
        calmar_ratio,
        max_drawdown,
        realized_cumulative_return,
        downside_deviation,
        concentration_risk_hhi,
        effective_number_of_assets,
        diversification_ratio,
        diversification_effect,
        average_pairwise_correlation,
        weighted_beta_merval,
        weighted_beta_eem,
        weighted_corr_merval,
        weighted_corr_fx,
        num_assets,
        risk_profile,
        risk_profile_reason,
        macro_risk_note,
        ingestion_timestamp,
        run_id
    from {{ ref('stg_basket_horizon_metrics') }}
)
select
    w.basket_horizon_id,
    w.basket_name,
    w.basket_label,
    w.horizon_name,
    w.horizon_label,
    w.weighting_method,
    w.macro_rationale,
    w.lookback_days,
    w.evaluation_days,
    w.simulation_days,
    w.max_weight,
    w.min_weight,
    w.investor_profile_anchor,
    w.horizon_description,
    n.expected_portfolio_return,
    n.portfolio_volatility,
    n.weighted_sharpe,
    n.sortino_ratio,
    n.calmar_ratio,
    n.max_drawdown,
    n.realized_cumulative_return,
    n.downside_deviation,
    n.concentration_risk_hhi,
    n.effective_number_of_assets,
    n.diversification_ratio,
    n.diversification_effect,
    n.average_pairwise_correlation,
    n.weighted_beta_merval,
    n.weighted_beta_eem,
    n.weighted_corr_merval,
    n.weighted_corr_fx,
    n.num_assets,
    n.risk_profile,
    n.risk_profile_reason,
    n.macro_risk_note,
    w.dbt_num_assets,
    w.dbt_weight_concentration_hhi,
    w.dbt_effective_number_of_assets,
    c.dbt_average_pairwise_correlation,
    sqrt(greatest(c.portfolio_variance, 0)) as dbt_portfolio_volatility_from_covariance,
    safe_divide(
        w.dbt_weighted_average_asset_volatility,
        nullif(sqrt(greatest(c.portfolio_variance, 0)), 0)
    ) as dbt_diversification_ratio,
    w.dbt_weighted_average_asset_volatility - sqrt(greatest(c.portfolio_variance, 0)) as dbt_diversification_effect,
    w.dbt_weighted_beta_merval,
    w.dbt_weighted_beta_eem,
    w.dbt_weighted_corr_fx,
    w.dbt_weighted_corr_merval,
    n.concentration_risk_hhi - w.dbt_weight_concentration_hhi as concentration_gap,
    n.weighted_beta_merval - w.dbt_weighted_beta_merval as beta_merval_gap,
    n.weighted_beta_eem - w.dbt_weighted_beta_eem as beta_eem_gap,
    n.diversification_effect - (w.dbt_weighted_average_asset_volatility - sqrt(greatest(c.portfolio_variance, 0))) as diversification_effect_gap,
    n.ingestion_timestamp,
    n.run_id
from weight_stats as w
left join covariance_summary as c
    on w.basket_horizon_id = c.basket_horizon_id
left join notebook_metrics as n
    on w.basket_horizon_id = n.basket_horizon_id

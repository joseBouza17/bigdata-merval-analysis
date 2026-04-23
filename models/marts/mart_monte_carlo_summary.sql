with monte_carlo as (
    -- Monte Carlo outputs are run-specific notebook artifacts, so this CTE keeps
    -- the raw simulation summary untouched before dbt enriches it.
    select
        portfolio_id,
        run_id,
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
        ingestion_timestamp
    from {{ ref('stg_monte_carlo_summary') }}
),
portfolio_context as (
    -- Portfolio context is joined back in using run_id because the notebook can
    -- produce multiple scenario snapshots for the same logical portfolio.
    select
        portfolio_id,
        run_id,
        portfolio_type,
        portfolio_type_reason,
        dbt_expected_portfolio_return,
        dbt_portfolio_volatility,
        dbt_weighted_beta_merval,
        dbt_weighted_beta_eem,
        dbt_weighted_sharpe,
        dbt_max_drawdown,
        dbt_effective_number_of_assets,
        dbt_average_pairwise_correlation,
        dbt_diversification_ratio
    from {{ ref('mart_portfolio_scenarios') }}
),
risk_breakdown as (
    -- The risk profile is currently portfolio-level rather than run-level, so we
    -- join it separately after the run-specific scenario context is attached.
    select
        portfolio_id,
        dbt_risk_profile
    from {{ ref('mart_portfolio_risk_breakdown') }}
)
select
    m.portfolio_id,
    m.run_id,
    m.initial_value,
    m.num_simulations,
    m.simulation_days,
    m.mean_final_value,
    m.median_final_value,
    m.min_final_value,
    m.max_final_value,
    m.percentile_5,
    m.percentile_25,
    m.percentile_75,
    m.percentile_95,
    m.probability_of_loss,
    m.expected_return_simulated,
    m.var_95,
    m.cvar_95,
    p.dbt_expected_portfolio_return,
    p.dbt_portfolio_volatility,
    p.dbt_weighted_beta_merval,
    p.dbt_weighted_beta_eem,
    p.dbt_weighted_sharpe,
    p.dbt_max_drawdown,
    p.dbt_effective_number_of_assets,
    p.dbt_average_pairwise_correlation,
    p.dbt_diversification_ratio,
    p.portfolio_type,
    p.portfolio_type_reason,
    r.dbt_risk_profile,
    m.ingestion_timestamp
from monte_carlo as m
left join portfolio_context as p
    on m.portfolio_id = p.portfolio_id
   and m.run_id = p.run_id
left join risk_breakdown as r
    on m.portfolio_id = r.portfolio_id

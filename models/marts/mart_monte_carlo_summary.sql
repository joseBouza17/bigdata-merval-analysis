-- Mart model: dashboard-ready Monte Carlo summary enriched with portfolio context.
-- This model expects the notebook to have already written analytics_market source tables.

with monte_carlo as (
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
    from {{ source('analytics_market', 'monte_carlo_summary') }}
),
portfolio_context as (
    select
        portfolio_id,
        run_id,
        expected_portfolio_return,
        portfolio_volatility,
        weighted_beta_merval,
        weighted_sharpe,
        portfolio_type,
        portfolio_type_reason
    from {{ source('analytics_market', 'portfolio_scenarios') }}
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
    p.expected_portfolio_return,
    p.portfolio_volatility,
    p.weighted_beta_merval,
    p.weighted_sharpe,
    p.portfolio_type,
    p.portfolio_type_reason,
    m.ingestion_timestamp
from monte_carlo as m
left join portfolio_context as p
    on m.portfolio_id = p.portfolio_id
   and m.run_id = p.run_id

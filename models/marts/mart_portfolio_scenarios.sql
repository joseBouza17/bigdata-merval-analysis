with dbt_metrics as (
    select
        portfolio_id,
        expected_portfolio_return,
        portfolio_volatility,
        weighted_sharpe,
        max_drawdown,
        num_assets,
        weighted_beta_merval,
        weighted_beta_eem
    from {{ ref('int_portfolio_metrics') }}
),
diversification as (
    select
        portfolio_id,
        weight_concentration_hhi,
        effective_number_of_assets,
        average_pairwise_correlation,
        diversification_ratio
    from {{ ref('mart_diversification_summary') }}
),
scenario_source as (
    -- Keep the notebook scenario snapshot alongside dbt-derived metrics so the
    -- final mart can both serve the dashboard and expose any parity gaps between
    -- the notebook analytics and the warehouse rebuild.
    select
        portfolio_id,
        run_id,
        ingestion_timestamp as notebook_generated_at,
        expected_portfolio_return as notebook_expected_portfolio_return,
        portfolio_volatility as notebook_portfolio_volatility,
        weighted_beta_merval as notebook_weighted_beta_merval,
        weighted_beta_eem as notebook_weighted_beta_eem,
        weighted_sharpe as notebook_weighted_sharpe,
        max_drawdown as notebook_max_drawdown,
        diversification_effect as notebook_diversification_effect,
        concentration_risk_hhi as notebook_concentration_risk_hhi,
        num_assets as notebook_num_assets,
        portfolio_type,
        portfolio_type_reason
    from {{ ref('stg_portfolio_scenarios') }}
)
select
    -- If no notebook scenario exists, the row still survives as a dbt-only view.
    -- The synthetic run_id makes that distinction explicit in downstream outputs.
    coalesce(s.portfolio_id, d.portfolio_id) as portfolio_id,
    coalesce(s.run_id, 'dbt_derived') as run_id,
    s.notebook_generated_at,
    s.notebook_expected_portfolio_return,
    s.notebook_portfolio_volatility,
    s.notebook_weighted_beta_merval,
    s.notebook_weighted_beta_eem,
    s.notebook_weighted_sharpe,
    s.notebook_max_drawdown,
    s.notebook_diversification_effect,
    s.notebook_concentration_risk_hhi,
    s.notebook_num_assets,
    d.expected_portfolio_return as dbt_expected_portfolio_return,
    d.portfolio_volatility as dbt_portfolio_volatility,
    d.weighted_sharpe as dbt_weighted_sharpe,
    d.max_drawdown as dbt_max_drawdown,
    d.weighted_beta_merval as dbt_weighted_beta_merval,
    d.weighted_beta_eem as dbt_weighted_beta_eem,
    d.num_assets as dbt_num_assets,
    div.weight_concentration_hhi as dbt_weight_concentration_hhi,
    div.effective_number_of_assets as dbt_effective_number_of_assets,
    div.average_pairwise_correlation as dbt_average_pairwise_correlation,
    div.diversification_ratio as dbt_diversification_ratio,
    -- Gap fields are kept for validation: they show how closely the dbt rebuild
    -- matches the notebook outputs that originally wrote the analytics tables.
    d.expected_portfolio_return - s.notebook_expected_portfolio_return as expected_return_gap,
    d.portfolio_volatility - s.notebook_portfolio_volatility as volatility_gap,
    d.weighted_sharpe - s.notebook_weighted_sharpe as sharpe_gap,
    s.portfolio_type,
    s.portfolio_type_reason
from dbt_metrics as d
left join scenario_source as s
    on d.portfolio_id = s.portfolio_id
left join diversification as div
    on d.portfolio_id = div.portfolio_id

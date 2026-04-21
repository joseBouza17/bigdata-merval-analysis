with factor_pivot as (
    select
        portfolio_id,
        max(case when factor_name = 'MERVAL' then weighted_exposure end) as factor_exposure_merval,
        max(case when factor_name = 'EEM' then weighted_exposure end) as factor_exposure_eem,
        max(case when factor_name = 'USDARS' then weighted_exposure end) as factor_exposure_usdars
    from {{ ref('mart_factor_exposure_summary') }}
    group by portfolio_id
)
select
    s.portfolio_id,
    s.run_id,
    s.portfolio_type,
    s.portfolio_type_reason,
    s.dbt_expected_portfolio_return,
    s.dbt_portfolio_volatility,
    s.dbt_weighted_sharpe,
    s.dbt_max_drawdown,
    s.dbt_weighted_beta_merval,
    s.dbt_weighted_beta_eem,
    s.dbt_effective_number_of_assets,
    s.dbt_average_pairwise_correlation,
    s.dbt_diversification_ratio,
    m.probability_of_loss,
    m.var_95,
    m.cvar_95,
    m.mean_final_value,
    m.median_final_value,
    m.dbt_risk_profile,
    d.diversification_profile,
    f.factor_exposure_merval,
    f.factor_exposure_eem,
    f.factor_exposure_usdars
from {{ ref('mart_portfolio_scenarios') }} as s
left join {{ ref('mart_monte_carlo_summary') }} as m
    on s.portfolio_id = m.portfolio_id
   and s.run_id = m.run_id
left join {{ ref('mart_diversification_summary') }} as d
    on s.portfolio_id = d.portfolio_id
left join factor_pivot as f
    on s.portfolio_id = f.portfolio_id

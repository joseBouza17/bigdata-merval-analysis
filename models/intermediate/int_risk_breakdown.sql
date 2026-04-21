with base_metrics as (
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
        diversification_ratio,
        portfolio_volatility_from_covariance
    from {{ ref('int_diversification_metrics') }}
),
factor_pivot as (
    select
        portfolio_id,
        max(case when factor_name = 'MERVAL' then weighted_exposure end) as exposure_merval,
        max(case when factor_name = 'EEM' then weighted_exposure end) as exposure_eem,
        max(case when factor_name = 'USDARS' then weighted_exposure end) as exposure_usdars
    from {{ ref('int_factor_exposure') }}
    group by portfolio_id
)
select
    m.portfolio_id,
    m.expected_portfolio_return,
    m.portfolio_volatility,
    m.weighted_sharpe,
    m.max_drawdown,
    m.num_assets,
    m.weighted_beta_merval,
    m.weighted_beta_eem,
    d.weight_concentration_hhi,
    d.effective_number_of_assets,
    d.average_pairwise_correlation,
    d.diversification_ratio,
    d.portfolio_volatility_from_covariance,
    f.exposure_merval,
    f.exposure_eem,
    f.exposure_usdars,
    case
        when m.portfolio_volatility >= 0.60 or m.max_drawdown <= -0.40 then 'aggressive'
        when m.portfolio_volatility >= 0.35 or m.max_drawdown <= -0.25 then 'balanced'
        else 'defensive'
    end as dbt_risk_profile
from base_metrics as m
left join diversification as d
    on m.portfolio_id = d.portfolio_id
left join factor_pivot as f
    on m.portfolio_id = f.portfolio_id

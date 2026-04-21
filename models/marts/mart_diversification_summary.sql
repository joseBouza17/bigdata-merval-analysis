select
    portfolio_id,
    num_assets,
    weight_concentration_hhi,
    effective_number_of_assets,
    average_pairwise_correlation,
    portfolio_volatility_from_covariance,
    diversification_ratio,
    case
        when diversification_ratio >= 1.50 and average_pairwise_correlation <= 0.40 then 'well_diversified'
        when diversification_ratio >= 1.20 then 'moderately_diversified'
        else 'concentrated'
    end as diversification_profile
from {{ ref('int_diversification_metrics') }}

select
    portfolio_id,
    factor_name,
    exposure_measure,
    weighted_exposure,
    annualized_factor_return,
    factor_volatility,
    negative_return_ratio,
    latest_factor_return,
    observations
from {{ ref('int_factor_exposure') }}

with asset_inputs as (
    -- Covariance-based portfolio risk needs one volatility estimate per asset.
    -- Prefer the notebook volatility for consistency, with realized volatility
    -- as the dbt-native fallback if the notebook export is missing a value.
    select
        portfolio_id,
        ticker,
        weight,
        coalesce(notebook_volatility, realized_volatility) as asset_volatility
    from {{ ref('int_portfolio_asset_metrics') }}
),
unique_pairs as (
    -- Build the lower triangle of the covariance matrix (including the diagonal)
    -- so each pair is counted once in the portfolio variance expansion.
    select
        a1.portfolio_id,
        a1.ticker as ticker_1,
        a2.ticker as ticker_2,
        a1.weight as weight_1,
        a2.weight as weight_2,
        a1.asset_volatility as asset_volatility_1,
        a2.asset_volatility as asset_volatility_2,
        -- Self-correlation must be 1. Missing cross correlations are treated as 0,
        -- which is a conservative fallback that avoids inventing covariance.
        coalesce(c.correlation, case when a1.ticker = a2.ticker then 1 else 0 end) as correlation
    from asset_inputs as a1
    inner join asset_inputs as a2
        on a1.portfolio_id = a2.portfolio_id
       and a1.ticker <= a2.ticker
    left join {{ ref('stg_correlation_matrix') }} as c
        on a1.ticker = c.ticker_1
       and a2.ticker = c.ticker_2
),
portfolio_variance as (
    -- Portfolio variance formula:
    -- sigma_p^2 = sum_i(w_i^2 * sigma_i^2) + 2 * sum_{i<j}(w_i * w_j * sigma_i * sigma_j * rho_ij)
    select
        portfolio_id,
        sum(
            case
                when ticker_1 = ticker_2 then power(weight_1 * asset_volatility_1, 2)
                else 2 * weight_1 * weight_2 * asset_volatility_1 * asset_volatility_2 * correlation
            end
        ) as portfolio_variance,
        avg(case when ticker_1 < ticker_2 then correlation end) as average_pairwise_correlation
    from unique_pairs
    group by portfolio_id
),
weight_stats as (
    -- HHI = sum(weight^2). In a portfolio setting it measures concentration,
    -- and its inverse approximates the effective number of equally weighted names.
    select
        portfolio_id,
        count(*) as num_assets,
        sum(power(weight, 2)) as weight_concentration_hhi,
        sum(weight * asset_volatility) as weighted_average_asset_volatility
    from asset_inputs
    group by portfolio_id
)
select
    w.portfolio_id,
    w.num_assets,
    w.weight_concentration_hhi,
    -- Effective number of assets = 1 / HHI.
    safe_divide(1, w.weight_concentration_hhi) as effective_number_of_assets,
    v.average_pairwise_correlation,
    sqrt(greatest(v.portfolio_variance, 0)) as portfolio_volatility_from_covariance,
    safe_divide(
        -- Diversification ratio = weighted average stand-alone volatility / portfolio volatility.
        -- Values above 1 indicate that imperfect correlations are reducing total risk.
        w.weighted_average_asset_volatility,
        nullif(sqrt(greatest(v.portfolio_variance, 0)), 0)
    ) as diversification_ratio
from weight_stats as w
left join portfolio_variance as v
    on w.portfolio_id = v.portfolio_id

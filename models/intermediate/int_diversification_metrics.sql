with asset_inputs as (
    select
        portfolio_id,
        ticker,
        weight,
        coalesce(notebook_volatility, realized_volatility) as asset_volatility
    from {{ ref('int_portfolio_asset_metrics') }}
),
unique_pairs as (
    select
        a1.portfolio_id,
        a1.ticker as ticker_1,
        a2.ticker as ticker_2,
        a1.weight as weight_1,
        a2.weight as weight_2,
        a1.asset_volatility as asset_volatility_1,
        a2.asset_volatility as asset_volatility_2,
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
    safe_divide(1, w.weight_concentration_hhi) as effective_number_of_assets,
    v.average_pairwise_correlation,
    sqrt(greatest(v.portfolio_variance, 0)) as portfolio_volatility_from_covariance,
    safe_divide(
        w.weighted_average_asset_volatility,
        nullif(sqrt(greatest(v.portfolio_variance, 0)), 0)
    ) as diversification_ratio
from weight_stats as w
left join portfolio_variance as v
    on w.portfolio_id = v.portfolio_id

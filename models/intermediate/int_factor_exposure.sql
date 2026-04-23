with factor_summary as (
    -- Summarize each factor's historical behavior so every portfolio can be read
    -- alongside the market context that prevailed during the sample.
    select
        factor_name,
        count(*) as observations,
        -- Annualized factor return = average daily factor return * 252.
        avg(factor_return) * 252 as annualized_factor_return,
        -- Annualized factor volatility = stddev(daily factor return) * sqrt(252).
        stddev(factor_return) * sqrt(252) as factor_volatility,
        -- Fraction of days with negative returns, used as a simple downside-frequency signal.
        avg(case when factor_return < 0 then 1 else 0 end) as negative_return_ratio,
        array_agg(factor_return order by date desc limit 1)[offset(0)] as latest_factor_return
    from {{ ref('int_factor_returns') }}
    group by factor_name
),
portfolio_exposure_base as (
    -- Exposure estimates are derived with the best available proxy per factor:
    -- beta for equity benchmarks, and correlation with FX when no formal USDARS
    -- beta is available from the notebook exports.
    select
        portfolio_id,
        'MERVAL' as factor_name,
        'weighted_beta' as exposure_measure,
        sum(weight * coalesce(beta_vs_merval, 0)) as weighted_exposure
    from {{ ref('int_portfolio_asset_metrics') }}
    group by portfolio_id

    union all

    select
        portfolio_id,
        'EEM' as factor_name,
        'weighted_beta' as exposure_measure,
        sum(weight * coalesce(beta_vs_eem, 0)) as weighted_exposure
    from {{ ref('int_portfolio_asset_metrics') }}
    group by portfolio_id

    union all

    select
        portfolio_id,
        'USDARS' as factor_name,
        'weighted_correlation_proxy' as exposure_measure,
        sum(weight * coalesce(corr_with_fx, 0)) as weighted_exposure
    from {{ ref('int_portfolio_asset_metrics') }}
    group by portfolio_id
),
portfolio_ids as (
    select distinct portfolio_id
    from {{ ref('int_portfolio_weights') }}
)
select
    p.portfolio_id,
    f.factor_name,
    -- The cross join below guarantees one row per portfolio-factor pair even when
    -- no direct exposure estimate exists, which keeps downstream dashboards dense
    -- and still allows the factor's historical context to be shown.
    coalesce(e.exposure_measure, 'historical_factor_context') as exposure_measure,
    e.weighted_exposure,
    f.observations,
    f.annualized_factor_return,
    f.factor_volatility,
    f.negative_return_ratio,
    f.latest_factor_return
from portfolio_ids as p
cross join factor_summary as f
left join portfolio_exposure_base as e
    on p.portfolio_id = e.portfolio_id
   and f.factor_name = e.factor_name

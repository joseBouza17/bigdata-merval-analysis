with portfolio_weights as (
    select
        portfolio_id,
        ticker,
        weight
    from {{ ref('int_portfolio_weights') }}
),
asset_returns as (
    select
        date,
        ticker,
        log_return,
        usd_adjusted_return,
        excess_return
    from {{ ref('int_asset_returns') }}
)
select
    w.portfolio_id,
    r.date,
    r.ticker,
    w.weight,
    r.log_return,
    r.usd_adjusted_return,
    r.excess_return,
    r.log_return * w.weight as weighted_log_return,
    coalesce(r.usd_adjusted_return, 0) * w.weight as weighted_usd_adjusted_return,
    coalesce(r.excess_return, 0) * w.weight as weighted_excess_return
from asset_returns as r
inner join portfolio_weights as w
    on r.ticker = w.ticker

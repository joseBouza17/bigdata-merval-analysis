with processed_returns as (
    -- The notebook-created processed table is the canonical source for derived
    -- return features such as excess return and FX-adjusted return.
    select
        date,
        ticker,
        close as processed_close,
        log_return,
        usd_adjusted_return,
        excess_return,
        merval_return,
        eem_return,
        vix_return,
        usdars_return,
        risk_free_daily,
        merval_usd_adjusted_return,
        inflation_proxy,
        country_risk_proxy
    from {{ ref('stg_asset_returns') }}
),
raw_prices as (
    -- Raw prices are retained for auditability and dashboard context even when
    -- the processed layer already computed the return series we rely on.
    select
        date,
        ticker,
        close as raw_close,
        adj_close,
        volume,
        currency,
        source,
        ingestion_timestamp,
        run_id
    from {{ ref('stg_stock_prices') }}
)
select
    r.date,
    r.ticker,
    -- Prefer the processed close used by the notebook when it exists so all
    -- downstream metrics line up with the engineered return series. Fallback to
    -- the raw close keeps the record usable if the processed close is null.
    coalesce(r.processed_close, p.raw_close) as close,
    p.raw_close,
    p.adj_close,
    p.volume,
    p.currency,
    p.source,
    p.ingestion_timestamp,
    p.run_id,
    r.log_return,
    r.usd_adjusted_return,
    r.excess_return,
    r.merval_return,
    r.eem_return,
    r.vix_return,
    r.usdars_return,
    r.risk_free_daily,
    r.merval_usd_adjusted_return,
    r.inflation_proxy,
    r.country_risk_proxy
from processed_returns as r
left join raw_prices as p
    on r.date = p.date
   and r.ticker = p.ticker

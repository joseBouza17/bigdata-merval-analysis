-- Input: stg_asset_returns and stg_stock_prices
-- Grain: one row per date and ticker
-- Purpose: reconcile processed returns back to the raw landed price history so downstream marts can carry both return features and raw price context.
-- Layer: intermediate

with processed_returns as (
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
        country_risk_proxy,
        ingestion_timestamp,
        run_id
    from {{ ref('stg_asset_returns') }}
),
raw_prices as (
    select
        date,
        ticker,
        close as raw_close,
        adj_close,
        volume,
        currency,
        source,
        ingestion_timestamp as raw_ingestion_timestamp,
        run_id as raw_run_id
    from {{ ref('stg_stock_prices') }}
)
select
    r.date,
    r.ticker,
    coalesce(r.processed_close, p.raw_close) as close,
    p.raw_close,
    p.adj_close,
    p.volume,
    p.currency,
    p.source,
    p.raw_ingestion_timestamp,
    p.raw_run_id,
    r.ingestion_timestamp,
    r.run_id,
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

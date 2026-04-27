-- Input: stg_factor_returns and stg_factor_prices
-- Grain: one row per date and factor_name
-- Purpose: reconcile processed factor returns back to the raw proxy series used in Notebook 1.
-- Layer: intermediate

with processed_factor_returns as (
    select
        date,
        factor_name,
        factor_value,
        factor_return,
        ingestion_timestamp,
        run_id
    from {{ ref('stg_factor_returns') }}
),
raw_factor_prices as (
    select
        date,
        factor_name,
        factor_value as raw_factor_value,
        country_risk_proxy,
        source,
        ingestion_timestamp as raw_ingestion_timestamp,
        run_id as raw_run_id
    from {{ ref('stg_factor_prices') }}
)
select
    fr.date,
    fr.factor_name,
    coalesce(fr.factor_value, fp.raw_factor_value) as factor_value,
    fp.raw_factor_value,
    fp.country_risk_proxy,
    fp.source,
    fp.raw_ingestion_timestamp,
    fp.raw_run_id,
    fr.ingestion_timestamp,
    fr.run_id,
    fr.factor_return
from processed_factor_returns as fr
left join raw_factor_prices as fp
    on fr.date = fp.date
   and fr.factor_name = fp.factor_name

-- Input: processed_market.beta_metrics
-- Grain: one row per ticker
-- Purpose: standardize the stock beta lookup used later for basket-level weighted exposures.
-- Layer: staging

with source_data as (
    select distinct
        upper(trim(cast(ticker as string))) as ticker,
        cast(beta_vs_merval as float64) as beta_vs_merval,
        cast(beta_vs_eem as float64) as beta_vs_eem,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('processed_market', 'beta_metrics') }}
)
select *
from source_data
where ticker is not null

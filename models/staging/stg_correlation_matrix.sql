-- Input: processed_market.correlation_matrix_long
-- Grain: one row per unordered ticker pair
-- Purpose: canonicalize the pair ordering so basket-level diversification checks do not double count the same correlation pair.
-- Layer: staging

with source_data as (
    select
        least(
            upper(trim(cast(ticker_1 as string))),
            upper(trim(cast(ticker_2 as string)))
        ) as ticker_1,
        greatest(
            upper(trim(cast(ticker_1 as string))),
            upper(trim(cast(ticker_2 as string)))
        ) as ticker_2,
        cast(correlation as float64) as correlation,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('processed_market', 'correlation_matrix_long') }}
    where ticker_1 is not null
      and ticker_2 is not null
)
select
    ticker_1,
    ticker_2,
    any_value(correlation) as correlation,
    any_value(ingestion_timestamp) as ingestion_timestamp,
    any_value(run_id) as run_id,
    count(*) as source_row_count,
    ticker_1 = ticker_2 as is_self_pair
from source_data
group by ticker_1, ticker_2

-- Staging model for stock prices from the raw layer.
-- Keeps only the columns needed by downstream return and risk models.

select
    cast(date as date) as date,
    cast(ticker as string) as ticker,
    cast(close as float64) as close,
    cast(adj_close as float64) as adj_close,
    cast(volume as float64) as volume,
    cast(currency as string) as currency,
    cast(source as string) as source,
    cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
    cast(run_id as string) as run_id
from {{ source('raw_market', 'stock_prices') }}

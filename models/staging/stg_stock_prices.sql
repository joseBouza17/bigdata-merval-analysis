with source_data as (
    select
        cast(date as date) as date,
        upper(trim(cast(ticker as string))) as ticker,
        cast(close as float64) as close,
        cast(adj_close as float64) as adj_close,
        cast(volume as float64) as volume,
        cast(currency as string) as currency,
        cast(source as string) as source,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('raw_market', 'stock_prices') }}
    where date is not null
      and ticker is not null
),
deduplicated as (
    select
        *,
        row_number() over (
            partition by date, ticker
            order by coalesce(ingestion_timestamp, timestamp('1900-01-01')) desc, run_id desc
        ) as row_num
    from source_data
)
select
    date,
    ticker,
    close,
    adj_close,
    volume,
    currency,
    source,
    ingestion_timestamp,
    run_id
from deduplicated
where row_num = 1

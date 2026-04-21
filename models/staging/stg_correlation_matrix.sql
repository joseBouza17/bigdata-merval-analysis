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
        cast(correlation as float64) as correlation
    from {{ source('processed_market', 'correlation_matrix_long') }}
    where ticker_1 is not null
      and ticker_2 is not null
)
select
    ticker_1,
    ticker_2,
    any_value(correlation) as correlation,
    count(*) as source_row_count,
    ticker_1 = ticker_2 as is_self_pair
from source_data
group by ticker_1, ticker_2

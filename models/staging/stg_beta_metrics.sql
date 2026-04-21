with source_data as (
    select distinct
        upper(trim(cast(ticker as string))) as ticker,
        cast(beta_vs_merval as float64) as beta_vs_merval,
        cast(beta_vs_eem as float64) as beta_vs_eem
    from {{ source('processed_market', 'beta_metrics') }}
)
select *
from source_data
where ticker is not null

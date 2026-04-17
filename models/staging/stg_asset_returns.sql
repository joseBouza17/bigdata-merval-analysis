-- Staging model over processed asset returns from the ETL notebook.
-- Keeps the fields required for portfolio analytics and removes null return records.

select
    cast(date as date) as date,
    cast(ticker as string) as ticker,
    cast(log_return as float64) as log_return
from {{ source('processed_market', 'asset_returns') }}
where date is not null
  and ticker is not null
  and log_return is not null

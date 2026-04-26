-- Input: processed_market.stock_metrics
-- Grain: one row per ticker
-- Purpose: standardize stock-level return, volatility, beta, drawdown, and classification metrics created in Notebook 2.
-- Layer: staging

with source_data as (
    select distinct
        upper(trim(cast(ticker as string))) as ticker,
        cast(average_return as float64) as average_return,
        cast(volatility as float64) as volatility,
        cast(beta_vs_merval as float64) as beta_vs_merval,
        cast(beta_vs_eem as float64) as beta_vs_eem,
        cast(corr_with_merval as float64) as corr_with_merval,
        cast(corr_with_fx as float64) as corr_with_fx,
        cast(sharpe_ratio as float64) as sharpe_ratio,
        cast(sortino_ratio as float64) as sortino_ratio,
        cast(calmar_ratio as float64) as calmar_ratio,
        cast(downside_frequency as float64) as downside_frequency,
        cast(max_drawdown as float64) as max_drawdown,
        lower(trim(cast(stock_type as string))) as stock_type,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('processed_market', 'stock_metrics') }}
)
select *
from source_data
where ticker is not null

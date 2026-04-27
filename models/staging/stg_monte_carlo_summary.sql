-- Input: analytics_market.monte_carlo_summary
-- Grain: one row per basket_horizon_id
-- Purpose: standardize the simulation summary layer used for downside comparison and investor recommendations.
-- Layer: staging

with source_data as (
    select distinct
        lower(trim(cast(basket_horizon_id as string))) as basket_horizon_id,
        lower(trim(cast(basket_name as string))) as basket_name,
        lower(trim(cast(horizon_name as string))) as horizon_name,
        lower(trim(cast(weighting_method as string))) as weighting_method,
        cast(initial_value as float64) as initial_value,
        cast(num_simulations as int64) as num_simulations,
        cast(simulation_days as int64) as simulation_days,
        cast(mean_final_value as float64) as mean_final_value,
        cast(median_final_value as float64) as median_final_value,
        cast(min_final_value as float64) as min_final_value,
        cast(max_final_value as float64) as max_final_value,
        cast(percentile_5 as float64) as percentile_5,
        cast(percentile_25 as float64) as percentile_25,
        cast(percentile_75 as float64) as percentile_75,
        cast(percentile_95 as float64) as percentile_95,
        cast(probability_of_loss as float64) as probability_of_loss,
        cast(expected_return_simulated as float64) as expected_return_simulated,
        cast(var_95 as float64) as var_95,
        cast(cvar_95 as float64) as cvar_95,
        cast(max_drawdown_p50 as float64) as max_drawdown_p50,
        cast(max_drawdown_p95 as float64) as max_drawdown_p95,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('analytics_market', 'monte_carlo_summary') }}
)
select *
from source_data
where basket_horizon_id is not null

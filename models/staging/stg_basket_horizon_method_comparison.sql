-- Input: analytics_market.basket_horizon_method_comparison
-- Grain: one row per basket_horizon_id
-- Purpose: standardize the method-ranking table that picks a winner inside each basket-horizon cell.
-- Layer: staging

with source_data as (
    select distinct
        lower(trim(cast(basket_horizon_id as string))) as basket_horizon_id,
        lower(trim(cast(basket_name as string))) as basket_name,
        lower(trim(cast(horizon_name as string))) as horizon_name,
        lower(trim(cast(weighting_method as string))) as weighting_method,
        cast(selection_score as float64) as selection_score,
        cast(method_rank_within_basket_horizon as int64) as method_rank_within_basket_horizon,
        cast(basket_rank_within_horizon as int64) as basket_rank_within_horizon,
        cast(is_best_method_for_basket_horizon as bool) as is_best_method_for_basket_horizon,
        cast(is_best_basket_for_horizon as bool) as is_best_basket_for_horizon,
        cast(equal_weight_return_delta as float64) as equal_weight_return_delta,
        cast(equal_weight_sharpe_delta as float64) as equal_weight_sharpe_delta,
        cast(equal_weight_probability_of_loss_delta as float64) as equal_weight_probability_of_loss_delta,
        cast(horizon_winner_reason as string) as horizon_winner_reason,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        cast(run_id as string) as run_id
    from {{ source('analytics_market', 'basket_horizon_method_comparison') }}
)
select *
from source_data
where basket_horizon_id is not null

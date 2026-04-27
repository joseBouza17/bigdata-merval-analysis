select
    basket_horizon_id,
    sum(weight) as total_weight
from {{ ref('int_basket_horizon_weights') }}
group by basket_horizon_id
having abs(sum(weight) - 1) > 0.000001

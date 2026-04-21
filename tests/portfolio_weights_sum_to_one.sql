select
    portfolio_id,
    sum(weight) as total_weight
from {{ ref('int_portfolio_weights') }}
group by portfolio_id
having abs(sum(weight) - 1) > 0.000001

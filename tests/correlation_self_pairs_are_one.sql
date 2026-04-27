select *
from {{ ref('stg_correlation_matrix') }}
where is_self_pair
  and abs(correlation - 1) > 0.000001

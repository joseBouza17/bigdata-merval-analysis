with ranked_stocks as (
    select
        ticker,
        average_return,
        volatility,
        sharpe_ratio,
        max_drawdown,
        beta_vs_merval,
        beta_vs_eem,
        corr_with_merval,
        corr_with_fx,
        stock_type,
        observations,
        -- Separate ranks make it easier to explain why a name scores well or badly
        -- instead of hiding everything inside a single opaque composite number.
        dense_rank() over (order by sharpe_ratio desc nulls last) as sharpe_rank,
        dense_rank() over (order by average_return desc nulls last) as return_rank,
        dense_rank() over (order by volatility asc nulls last) as volatility_rank,
        dense_rank() over (order by max_drawdown desc nulls last) as drawdown_rank,
        -- Composite rank prioritizes risk-adjusted performance first, then raw return,
        -- and finally lower volatility as the tie-breaker.
        dense_rank() over (
            order by sharpe_ratio desc nulls last, average_return desc nulls last, volatility asc nulls last
        ) as composite_rank
    from {{ ref('mart_stock_metrics') }}
)
select *
from ranked_stocks

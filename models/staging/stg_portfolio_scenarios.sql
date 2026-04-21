{% set scenario_relation = source('analytics_market', 'portfolio_scenarios') %}
{% if execute %}
    {% set scenario_columns = adapter.get_columns_in_relation(scenario_relation) %}
    {% set scenario_column_names = [] %}
    {% for col in scenario_columns %}
        {% do scenario_column_names.append(col.name | lower) %}
    {% endfor %}
{% else %}
    {% set scenario_column_names = [] %}
{% endif %}

with source_data as (
    select distinct
        cast(portfolio_id as string) as portfolio_id,
        cast(run_id as string) as run_id,
        cast(expected_portfolio_return as float64) as expected_portfolio_return,
        cast(portfolio_volatility as float64) as portfolio_volatility,
        {% if 'weighted_beta_merval' in scenario_column_names and 'weighted_beta' in scenario_column_names %}
        cast(coalesce(weighted_beta_merval, weighted_beta) as float64) as weighted_beta_merval,
        {% elif 'weighted_beta_merval' in scenario_column_names %}
        cast(weighted_beta_merval as float64) as weighted_beta_merval,
        {% elif 'weighted_beta' in scenario_column_names %}
        cast(weighted_beta as float64) as weighted_beta_merval,
        {% else %}
        cast(null as float64) as weighted_beta_merval,
        {% endif %}
        {% if 'weighted_beta_eem' in scenario_column_names %}
        cast(weighted_beta_eem as float64) as weighted_beta_eem,
        {% else %}
        cast(null as float64) as weighted_beta_eem,
        {% endif %}
        cast(weighted_sharpe as float64) as weighted_sharpe,
        {% if 'max_drawdown' in scenario_column_names %}
        cast(max_drawdown as float64) as max_drawdown,
        {% else %}
        cast(null as float64) as max_drawdown,
        {% endif %}
        {% if 'diversification_effect' in scenario_column_names %}
        cast(diversification_effect as float64) as diversification_effect,
        {% else %}
        cast(null as float64) as diversification_effect,
        {% endif %}
        {% if 'concentration_risk_hhi' in scenario_column_names %}
        cast(concentration_risk_hhi as float64) as concentration_risk_hhi,
        {% else %}
        cast(null as float64) as concentration_risk_hhi,
        {% endif %}
        cast(num_assets as int64) as num_assets,
        {% if 'ingestion_timestamp' in scenario_column_names %}
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        {% else %}
        cast(null as timestamp) as ingestion_timestamp,
        {% endif %}
        {% if 'portfolio_type' in scenario_column_names %}
        lower(trim(cast(portfolio_type as string))) as portfolio_type,
        {% else %}
        cast(null as string) as portfolio_type,
        {% endif %}
        {% if 'portfolio_type_reason' in scenario_column_names %}
        cast(portfolio_type_reason as string) as portfolio_type_reason,
        {% else %}
        cast(null as string) as portfolio_type_reason,
        {% endif %}
        {% if 'weighted_beta' in scenario_column_names %}
        cast(weighted_beta as float64) as weighted_beta_legacy
        {% else %}
        cast(null as float64) as weighted_beta_legacy
        {% endif %}
    from {{ source('analytics_market', 'portfolio_scenarios') }}
)
select *
from source_data
where portfolio_id is not null
  and run_id is not null

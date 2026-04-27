{% test accepted_range(model, column_name, min_value=None, max_value=None) %}

with validation as (
    select
        {{ column_name }} as value
    from {{ model }}
),
validation_errors as (
    select
        value
    from validation
    where value is not null
      and (
            {% if min_value is not none %}
            value < {{ min_value }}
            {% else %}
            false
            {% endif %}
            {% if max_value is not none %}
            or value > {{ max_value }}
            {% endif %}
      )
)
select *
from validation_errors

{% endtest %}

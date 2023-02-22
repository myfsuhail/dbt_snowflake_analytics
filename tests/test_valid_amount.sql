{% test valid_amount(model, column_name) %}

with validation as (select {{ column_name }} as valid_amount from {{ model }})
select *
from validation
where valid_amount <= 0

{% endtest %}

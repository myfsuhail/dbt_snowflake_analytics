{% test valid_amount(model, column_name) %}

select *
from {{ ref('orders_summed')}}
where order_total  <= 0

{% endtest %}



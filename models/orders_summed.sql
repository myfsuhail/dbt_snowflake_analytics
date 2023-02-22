{{config (
    schema = 'FIN', 
    database = 'DEV',
    materialized = 'table'
)}}

{% set order_priority = ["1", "2", "3","4","5"] %}

select
O_ORDERDATE as order_date,
{% for order_priority in order_priority %}
sum(CASE WHEN substr(O_ORDERPRIORITY,1,1)='{{order_priority}}' then O_TOTALPRICE end ) as "{{order_priority}}_order_total",
{% endfor %}
sum(O_TOTALPRICE) as order_total
from
{{source('PUBLIC','ORDERS')}}
group by order_date


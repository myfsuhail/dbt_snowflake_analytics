{{config (
    schema = 'consumption', 
    database = 'dev',
    materialized = 'view'
)}}
select
    *
from {{ref('apac_customers')}}



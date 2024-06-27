{{config (
    schema = 'consumption', 
    database = 'dev',
    materialized = 'view'
)}}
select
    *, current_timestamp() as elt_time
from {{ref('apac_customers')}}



{{config (
    schema = 'consumption', 
    database = 'dev',
    materialized = 'table'
)}}

select *
from {{ ref ('customer') }}
where region_name in ('AMERICA')



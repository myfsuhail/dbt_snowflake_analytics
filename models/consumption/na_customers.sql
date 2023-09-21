{{config (
    schema = 'consumption', 
    database = 'dev',
    materialized = 'table'
)}}

select *
from {{ ref ('customer') }}
where r_name in ('AMERICA')



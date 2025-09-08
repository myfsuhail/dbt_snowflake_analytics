{{config (
    materialized = 'view'
)}}

select *
from {{ ref ('customer') }}
where region_name in ('ASIA','PACIFIC')



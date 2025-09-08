{{config (
    materialized = 'view'
)}}

select *
from {{ ref ('customer_tbl') }}
where region_name in ('AMERICA')



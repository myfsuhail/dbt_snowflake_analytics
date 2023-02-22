{{config (
    schema = 'PUBLIC', 
    database = 'DEV',
    materialized = 'table'
)}}

select *
from {{source ('PUBLIC','JOB_CTRL_TBL')}}
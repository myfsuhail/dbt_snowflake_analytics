{{config (
    schema = 'STAGING', 
    database = 'DEV',
    unique_key=['cust_key','order_key'],
    materialized = 'incremental',
    post_hook=["{{upsert_ctrl_tbl()}}"]
)}}

select 
    o_orderkey as order_key,
    o_custkey as cust_key,
    O_ORDERSTATUS as order_status,
    O_TOTALPRICE as order_price,
    O_ORDERDATE as order_dt,
    upd_ts
from {{source ('PUBLIC','ORDERS')}}
where upd_ts > (select nvl(max(upd_ts),'1970-01-01') from {{this}})

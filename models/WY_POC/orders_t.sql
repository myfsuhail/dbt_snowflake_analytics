{{ config(
    schema="STAGING", 
    database="DEV", 
    materialized="table",
    transient=false) 
}}

select
    o_orderkey as order_key,
    o_custkey as cust_key,
    o_orderstatus as order_status,
    o_totalprice as order_price,
    o_orderdate as order_dt,
    o_orderpriority as order_priority,
    o_clerk as clerk,
    o_shippriority as ship_priority,
    SYSDATE() AS upd_ts
from {{ source("PUBLIC", "ORDERS") }}

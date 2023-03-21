{{config (
    schema = 'STAGING', 
    database = 'DEV',
    unique_key=['ORDER_KEY','LINEITEM'],
    materialized = 'incremental',
    post_hook = ["{{upsert_ctrl_tbl()}}"]
)}}

select 
    L_ORDERKEY as ORDER_KEY,
    L_LINENUMBER as LINEITEM,
    SUM(L_QUANTITY) as LINEQUANTITY,
    SUM(L_EXTENDEDPRICE + (L_EXTENDEDPRICE * L_TAX) - (L_EXTENDEDPRICE * L_DISCOUNT)) as LINEPRICE,
    L_LINESTATUS as LINESTATUS,
    L_SHIPDATE as SHIPDATE,
    L_COMMITDATE as COMMITDATE,
    L_SHIPMODE as SHIPMENTMODE,
    UPD_TS
from {{source('PUBLIC','LINEITEM')}}
where upd_ts > (select nvl(max(upd_ts),'1970-01-01') from {{this}})
group by L_ORDERKEY, L_LINENUMBER, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_SHIPMODE, UPD_TS
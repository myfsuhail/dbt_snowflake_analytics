{{config (
    schema = 'NA', 
    database = 'DEV',
    materialized = 'incremental'
)}}

select
	a.cust_key,
    b.o_orderkey as order_key,
    a.cust_nm,
    a.nation,
    a.region_name,
    a.cust_acct_bal,
    O_ORDERSTATUS as order_status_cd,
    O_ORDERDATE as order_dt,
    O_TOTALPRICE as total_price,
    b.upd_ts as upd_ts
from {{ref('na_customers')}} a
inner join {{source('PUBLIC','ORDERS')}} b
on a.cust_key=b.o_custkey
where b.upd_ts > (select nvl(max(upd_ts),'1900-01-01') from {{this}})
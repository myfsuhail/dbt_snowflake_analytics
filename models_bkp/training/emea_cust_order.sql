{{config (
    schema = 'EMEA', 
    database = 'DEV',
    materialized = 'table'
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
    O_TOTALPRICE as total_price
from {{ref('customers_var')}} a
inner join {{source('PUBLIC','ORDERS')}} b
on a.cust_key=b.o_custkey
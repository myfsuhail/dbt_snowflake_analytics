{{config (
    schema = 'STAGING', 
    database = 'DEV',
    materialized = 'table',
    transient= false
)}}
select
    c_custkey as cust_key,
    c_name as cust_nm,
    c_address as cust_address,
    c_phone as cust_phone_num,
    n_name as nation,
    r_name as region_name,
    c_acctbal as cust_acct_bal
from {{source('PUBLIC','CUSTOMER')}}
left join {{source('PUBLIC','NATION')}} on customer.c_nationkey = nation.n_nationkey
left join {{source('PUBLIC','REGION')}} on nation.n_regionkey = region.r_regionkey
where r_name in ('{{ var("region_name") }}' )

{{config (
    schema = 'PUBLIC_APAC', 
    database = 'DEV',
    materialized = 'table'
)}}
select
    c_custkey as cust_key,
    c_name as cust_nm,
    c_address as cust_address,
    c_phone as cust_phone_num,
    n_name as nation,
    r_name as region_name,
    c_acctbal as cust_acct_bal
from dev.public.customer
left join dev.public.nation on customer.c_nationkey = nation.n_nationkey
left join dev.public.region on nation.n_regionkey = region.r_regionkey
where r_name in ('ASIA','PACIFIC')

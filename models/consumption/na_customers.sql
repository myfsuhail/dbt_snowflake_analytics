{{config (
    schema = 'consumption', 
    database = 'dev',
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
from {{ source ('raw','customer') }}
left join {{ source ('raw','nation') }} on customer.c_nationkey = nation.n_nationkey
left join {{ source ('raw','region') }} on nation.n_regionkey = region.r_regionkey
where r_name in ('AMERICA')



{{config (
    materialized = 'ephemeral'
)}}

select
    cust_key,
    cust_nm,
    cust_address,
    cust_phone_num,
    nation,
    region_name,
    cust_acct_bal,
    sys_insr_dt
from {{ref('apac_customers')}}
union
select
    cust_key,
    cust_nm,
    cust_address,
    cust_phone_num,
    nation,
    region_name,
    cust_acct_bal,
    sys_insr_dt
from {{ref('emea_customers')}}
union
select
    cust_key,
    cust_nm,
    cust_address,
    cust_phone_num,
    nation,
    region_name,
    cust_acct_bal,
    sys_insr_dt
from {{ref('na_customers')}}
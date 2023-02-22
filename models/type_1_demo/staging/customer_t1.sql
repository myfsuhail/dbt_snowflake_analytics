{{config (
    schema = 'STAGING', 
    database = 'DEV',
    unique_key=['cust_key'],
    materialized = 'incremental',
    post_hook=["update {{source('PUBLIC','JOB_CTRL_TBL')}} 
                set upd_ts=(select max(upd_ts) from {{this}}) where tbl_nm=upper('CUSTOMER_T1')"]
)}}
select
    c_custkey as cust_key,
    c_name as cust_nm,
    c_address as cust_address,
    c_phone as cust_phone_num,
    c_acctbal as cust_acct_bal,
    c_nationkey as nation_key,
    upd_ts
from {{source('PUBLIC','CUSTOMER')}}
where upd_ts > (select nvl(max(upd_ts),'1970-01-01') from {{this}})
{{config (
    materialized = 'incremental',
    unique_key = ['cust_key'],
    incremental_strategy='delete+insert'
)}}

{% set time_val = get_max_upd_ts() %}

select
    c_custkey as cust_key,
    c_name as cust_nm,
    c_address as cust_address,
    c_phone as cust_phone_num,
    n_name as nation,
    r_name as region_name,
    c_acctbal as cust_acct_bal,
    customer.upd_ts as src_upd_ts,
    current_timestamp() as upd_ts 
from {{ source ('raw_schema','customer') }}
left join {{ source ('raw_schema','nation') }} on customer.c_nationkey = nation.n_nationkey
left join {{ source ('raw_schema','region') }} on nation.n_regionkey = region.r_regionkey

{% if is_incremental() %}
where customer.upd_ts > '{{time_val}}'
{% endif %}

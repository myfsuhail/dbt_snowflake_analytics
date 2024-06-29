{{
    config(
        materialized = 'incremental',
        unique_key = 'c_custkey',
        merge_update_columns = ['c_acctbal'],
        post_hook='update {{this}} set c_acctbal=0 where c_custkey=150000'
    )
}}

select 
    c_custkey,
    c_nationkey,
    c_phone,
    c_acctbal,
    n.n_regionkey,
    c.update_timestamp as update_timestamp
from {{ source ('raw_schema' , 'customer') }} c
inner join {{ source('raw_schema' , 'nation')}} n
on c.c_nationkey = n.n_nationkey

{% if is_incremental() %}

where c.update_timestamp > (select coalesce(max(update_timestamp),'1900-01-01'::TIMESTAMP) from {{ this }} )

{% endif %}
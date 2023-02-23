{{config (
    schema = 'STAGING', 
    database = 'DEV',
    unique_key=['REGION_KEY'],
    materialized = 'incremental',
    post_hook=["{{upsert_ctrl_tbl()}}"]
)}}

{% set x = get_max_upd_ts()  %}

select 
    R_REGIONKEY as region_key,
    r_name as region_nm,
    r_comment as region_desc,
    upd_ts
from {{source ('PUBLIC','REGION')}}
where upd_ts > '{{x}}'

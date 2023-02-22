{{config (
    schema = 'STAGING', 
    database = 'DEV',
    unique_key=['REGION_KEY'],
    materialized = 'incremental',
    post_hook=["update {{source('PUBLIC','JOB_CTRL_TBL')}} 
                set upd_ts=(select max(upd_ts) from {{this}}) where tbl_nm=upper('REGION_T1')"]
)}}

select 
    R_REGIONKEY as region_key,
    r_name as region_nm,
    r_comment as region_desc,
    upd_ts
from {{source ('PUBLIC','REGION')}}
where upd_ts > (select nvl(max(upd_ts),'1970-01-01') from {{this}})

{{config (
    schema = 'STAGING', 
    database = 'DEV',
    unique_key=['NATION_KEY'],
    materialized = 'incremental',
    post_hook=["update {{source('PUBLIC','JOB_CTRL_TBL')}} 
                set upd_ts=(select max(upd_ts) from {{this}}) where tbl_nm=upper('NATION_T1')"]
)}}

select 
    N_NATIONKEY as NATION_key,
    n_name as name_nm,
    n_regionkey as region_key,
    n_comment as nation_desc,
    upd_ts
from {{source ('PUBLIC','NATION')}}
where upd_ts > (select nvl(max(upd_ts),'1970-01-01') from {{this}})

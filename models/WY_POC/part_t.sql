{{ config(
    schema="STAGING", 
    database="DEV", 
    materialized="table",
    transient=false
    ) }}

select
    p_partkey as part_key,
    p_name as part_nm,
    p_mfgr as part_mfgr,
    p_brand as part_brand,
    p_type as part_type,
    p_size as part_size,
    p_container as part_container,
    p_retailprice as part_retail_price,
    sysdate() as upd_ts
from {{ source("PUBLIC", "PART") }}

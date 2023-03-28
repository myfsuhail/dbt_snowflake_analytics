{% test test_src_to_tgt_cnt_check(model, src_tbl,src_schema) %}

{% set source_table=src_tbl %}
{% set source_schema=src_schema %}

with tgt as (

    select
        count(*) as tgt_cnt

    from {{ model }}


),

source as (
    select
        count(*) as src_cnt

    from {{ source(source_table | string, 
                   source_schema | string) }}

)

select *
from source,tgt
where src_cnt!=tgt_cnt

{% endtest %}
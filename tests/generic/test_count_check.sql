{% test test_count_check(model, compare_source) %}

with tgt as (

    select
        count(*) as tgt_cnt

    from {{ model }}


),

source as (
    select
        count(*) as src_cnt

    from {{ compare_source }}

)

select *
from source,tgt
where src_cnt!=tgt_cnt

{% endtest %}
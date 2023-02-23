{% macro upsert_ctrl_tbl() %}

{% set sql_statement %}

        merge into {{source('PUBLIC','JOB_CTRL_TBL')}}  
        using (select  '{{ this.name | upper }}' tbl_nm,
                        max(UPD_TS) upd_ts 
                 from {{ this }} ) as src
            on {{source('PUBLIC','JOB_CTRL_TBL')}}.tbl_nm = '{{ this.name | upper }}'
            
            when matched then update set upd_ts = src.upd_ts
            when not matched THEN insert  ( tbl_nm, upd_ts) values (src.tbl_nm, src.upd_ts);

{% endset %}

{%- do run_query(sql_statement) -%}

{%- endmacro -%}
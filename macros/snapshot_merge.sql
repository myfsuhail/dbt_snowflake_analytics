
{% macro snapshot_merge_sql(target, source, insert_cols,unique_key) -%}
  {{ adapter.dispatch('snapshot_merge_sql', 'dbt')(target, source, insert_cols,unique_key) }}
{%- endmacro %}


{% macro default__snapshot_merge_sql(target, source, insert_cols,unique_key) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_DEST.row_exp_dt=to_date('12/31/9999')
    and  DBT_INTERNAL_SOURCE.{{ unique_key }}  = DBT_INTERNAL_DEST.{{ unique_key }} 

    when matched
     and DBT_INTERNAL_DEST.row_exp_dt = to_date('12/31/9999')
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set row_exp_dt = DBT_INTERNAL_SOURCE.row_exp_dt

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ insert_cols_csv }})

{% endmacro %}
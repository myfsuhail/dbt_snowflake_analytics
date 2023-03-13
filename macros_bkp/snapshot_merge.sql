
{% macro snapshot_merge_sql(target, source, insert_cols) -%}
  {{ adapter.dispatch('snapshot_merge_sql', 'dbt')(target, source, insert_cols) }}
{%- endmacro %}


{% macro default__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_unique_key = DBT_INTERNAL_DEST.dbt_unique_key
    and DBT_INTERNAL_SOURCE.row_eff_dt = DBT_INTERNAL_DEST.row_eff_dt

    when matched
     and DBT_INTERNAL_DEST.row_exp_dt is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set row_exp_dt = DBT_INTERNAL_SOURCE.row_exp_dt

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ insert_cols_csv }})

{% endmacro %}
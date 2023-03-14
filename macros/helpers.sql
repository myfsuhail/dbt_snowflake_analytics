{#
    Add new columns to the table if applicable
#}
{% macro create_columns(relation, columns) %}
  {{ adapter.dispatch('create_columns', 'dbt')(relation, columns) }}
{% endmacro %}

{% macro default__create_columns(relation, columns) %}
  {% for column in columns %}
    {% call statement() %}
      alter table {{ relation }} add column "{{ column.name }}" {{ column.data_type }};
    {% endcall %}
  {% endfor %}
{% endmacro %}


{% macro post_snapshot(staging_relation) %}
  {{ adapter.dispatch('post_snapshot', 'dbt')(staging_relation) }}
{% endmacro %}

{% macro default__post_snapshot(staging_relation) %}
    {# no-op #}
{% endmacro %}

{% macro get_true_sql() %}
  {{ adapter.dispatch('get_true_sql', 'dbt')() }}
{% endmacro %}

{% macro default__get_true_sql() %}
    {{ return('TRUE') }}
{% endmacro %}

{% macro snapshot_staging_table(strategy, source_sql, target_relation) -%}
  {{ adapter.dispatch('snapshot_staging_table', 'dbt')(strategy, source_sql, target_relation) }}
{% endmacro %}

{% macro default__snapshot_staging_table(strategy, source_sql, target_relation) -%}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select *,
            {{ strategy.unique_key }} as dbt_unique_key,
            to_timestamp_ntz(row_eff_dt) as row_eff_dt_tgt

        from {{ target_relation }}
        where row_exp_dt =to_timestamp_ntz('12/31/9999')

    ),

    insertions_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            to_timestamp_ntz({{ strategy.updated_at }}) as row_eff_dt,
            to_timestamp_ntz('12/31/9999') as row_exp_dt
        from snapshot_query
    ),

    updates_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            to_timestamp_ntz({{ strategy.updated_at }}) as row_eff_dt,
            to_timestamp_ntz({{ strategy.updated_at }}) as row_exp_dt

        from snapshot_query
    ),

    {%- if strategy.invalidate_hard_deletes %}

    deletes_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key
        from snapshot_query
    ),
    {% endif %}

    insertions as (

        select
            'insert' as dbt_change_type,
            to_timestamp_ntz('12/31/9999') as row_eff_dt_tgt,
            source_data.*

        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                {{ strategy.row_changed }}
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            row_eff_dt_tgt,
            source_data.*

        from updates_source_data as source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            {{ strategy.row_changed }}
        )
    )

    {%- if strategy.invalidate_hard_deletes -%}
    ,

    deletes as (

        select
            'delete' as dbt_change_type,
            to_timestamp_ntz('12/31/9999') as row_eff_dt_tgt,
            source_data.*,
            {{ snapshot_get_time() }} as row_eff_dt,
            {{ snapshot_get_time() }} as row_exp_dt

        from snapshotted_data
        left join deletes_source_data as source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    )
    {%- endif %}

    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from deletes
    {%- endif %}

{%- endmacro %}


{% macro build_snapshot_table(strategy, sql) -%}
  {{ adapter.dispatch('build_snapshot_table', 'dbt')(strategy, sql) }}
{% endmacro %}

{% macro default__build_snapshot_table(strategy, sql) %}

    select *,
        to_timestamp_ntz({{ strategy.updated_at }}) as row_eff_dt,
        to_timestamp_ntz('12/31/9999') as row_exp_dt
    from (
        {{ sql }}
    ) sbq

{% endmacro %}


{% macro build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set temp_relation = make_temp_relation(target_relation) %}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {% call statement('build_snapshot_staging_relation') %}
        {{ create_table_as(True, temp_relation, select) }}
    {% endcall %}

    {% do return(temp_relation) %}
{% endmacro %}
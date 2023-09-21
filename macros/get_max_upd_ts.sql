{% macro get_max_upd_ts() %}

{% set sql_statement %}

        select nvl(max(src_upd_ts),'1970-01-01') from {{this}};

{% endset %}

{%- set max_time = run_query(sql_statement) -%}

{% if execute %}
      {% if max_time %}
        {% set result_value = max_time.columns[0].values()[0] %}
          {{ return (result_value) }}                  
      {% else %}
          {{ return ('1900-01-01') }}  
      {% endif %}
{% endif %}

{%- endmacro -%}
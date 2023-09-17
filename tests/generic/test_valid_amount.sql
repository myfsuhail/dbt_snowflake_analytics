{% test test_valid_amount(model, column_name) %}

{{ config(severity = 'error',
          store_failures= true ) }}

    select *
    from {{ model }}
    where {{ column_name }} <0

{% endtest %}
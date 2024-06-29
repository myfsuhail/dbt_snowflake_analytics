{% test validate_acctbal(model, column_name) %}

with validation_errors as (

    select
        {{ column_name }} as balance_amt
    from {{ model }}
    where {{ column_name }} < 0

)

select *
from validation_errors

{% endtest %}
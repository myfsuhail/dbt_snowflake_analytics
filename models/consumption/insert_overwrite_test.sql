{{
    config(
        materialized = 'insert_overwrite_materialization'
    )
}}

select c_custkey, c_name
from {{ source('raw_schema','customer') }}
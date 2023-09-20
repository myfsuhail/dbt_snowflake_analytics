select count(*) as cnt from {{ source('raw_conn','customer') }} -- dev.raw.customer

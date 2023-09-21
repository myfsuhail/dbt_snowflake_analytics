select count(*) as cnt from {{ source('raw','customer') }} -- dev.raw.customer

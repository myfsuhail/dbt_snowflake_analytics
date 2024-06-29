select c_custkey, c_name from {{ ref ('apac_customers') }} left join {{ ref ('apac_customers')  }}

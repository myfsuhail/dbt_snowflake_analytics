select
    c.c_custkey,
    c.c_name,
    c.c_mktsegment,
    s.description as segment_description,
    n.n_name as nation_name,
    r.r_name as region_name
from {{ source('public', 'customer') }} c
join {{ ref('customer_segment_seed') }} s
  on c.c_mktsegment = s.segment
join {{ source('public', 'nation') }} n
  on c.c_nationkey = n.n_nationkey
join {{ source('public', 'region') }} r
  on n.n_regionkey = r.r_regionkey

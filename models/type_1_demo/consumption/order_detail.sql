{{config (
    schema = 'CONSUMPTION', 
    database = 'DEV',
    unique_key=['ORDER_KEY','LINEITEM'],
    materialized = 'incremental'
)}}

select
    c.cust_key,
    c.cust_nm,
    c.CUST_ACCT_BAL,
    li.ORDER_KEY,
    o.ORDER_STATUS,
    o.order_price,
    o.order_dt,
    li.LINEITEM,
    li.LINEQUANTITY,
    li.LINEPRICE,
    li.LINESTATUS,
    li.SHIPDATE,
    li.COMMITDATE,
    li.SHIPMENTMODE,
    n.nation_key,
    n.name_nm,
    r.region_key,
    r.region_nm,
    sysdate() as UPD_TS
from {{ref('lineitem_t1')}} li
left join {{ref('orders_t1')}} o
on li.order_key = o.order_key
left join {{ref('customer_t1')}} c
on o.cust_key = c.cust_key
left join {{ref('nation_t1')}} n
on n.nation_key = c.nation_key
left join {{ref('region_t1')}} r
on r.region_key = n.region_key
{{
    config(
        schema="CONSUMPTION",
        database="DEV",
        unique_key=["ORDER_KEY", "LINEITEM"],
        materialized="incremental",
    )
}}

-- depends_on: {{ ref('int_order_detail') }}
select
    c.cust_key,
    c.cust_nm,
    c.cust_acct_bal,
    li.order_key,
    o.order_status,
    o.order_price,
    o.order_dt,
    li.lineitem,
    li.linequantity,
    li.lineprice,
    li.linestatus,
    li.shipdate,
    li.commitdate,
    li.shipmentmode,
    n.nation_key,
    n.name_nm,
    r.region_key,
    r.region_nm,
    sysdate() as upd_ts
from {{ ref("lineitem_t1") }} li

{% if is_incremental() %}
inner join
    {{ ref("int_order_detail") }} delta
    on li.order_key = delta.order_key
    and li.lineitem = delta.lineitem
{% endif %}


left join {{ ref("orders_t1") }} o on li.order_key = o.order_key
left join {{ ref("customer_t1") }} c on o.cust_key = c.cust_key
left join {{ ref("nation_t1") }} n on n.nation_key = c.nation_key
left join {{ ref("region_t1") }} r on r.region_key = n.region_key

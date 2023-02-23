{{config (
    schema = 'CONSUMPTION', 
    database = 'DEV',
    materialized = 'table'
)}}

with lineitem_t1_cte as (
	select
		li.ORDER_KEY,
		li.LINEITEM
	from {{ref('lineitem_t1')}} li
	left join {{ref('orders_t1')}} o
	on li.order_key = o.order_key
	left join {{ref('customer_t1')}} c
	on o.cust_key = c.cust_key
	left join {{ref('nation_t1')}} n
	on n.nation_key = c.nation_key
	left join {{ref('region_t1')}} r
	on r.region_key = n.region_key
    where li.UPD_TS > (select upd_ts from {{ref('job_ctrl_tbl_consumption')}} where lower(tbl_nm) = 'lineitem_t1')
),

orders_t1_cte as (
	select
		li.ORDER_KEY,
		li.LINEITEM
	from {{ref('lineitem_t1')}} li
	inner join {{ref('orders_t1')}} o
	on li.order_key = o.order_key
    where o.UPD_TS > (select upd_ts from {{ref('job_ctrl_tbl_consumption')}} where lower(tbl_nm) = 'orders_t1')
),

customer_t1_cte as (
	select
		li.ORDER_KEY,
		li.LINEITEM
	from {{ref('lineitem_t1')}} li
	left join {{ref('orders_t1')}} o
	on li.order_key = o.order_key
	inner join {{ref('customer_t1')}} c
	on o.cust_key = c.cust_key
    where c.UPD_TS > (select upd_ts from {{ref('job_ctrl_tbl_consumption')}} where lower(tbl_nm) = 'customer_t1')
),

nation_t1_cte as (
	select
		li.ORDER_KEY,
		li.LINEITEM
	from {{ref('lineitem_t1')}} li
	left join {{ref('orders_t1')}} o
	on li.order_key = o.order_key
	left join {{ref('customer_t1')}} c
	on o.cust_key = c.cust_key
	inner join {{ref('nation_t1')}} n
	on n.nation_key = c.nation_key
    where n.UPD_TS > (select upd_ts from {{ref('job_ctrl_tbl_consumption')}} where lower(tbl_nm) = 'nation_t1')
),

region_t1_cte as (
	select
		li.ORDER_KEY,
		li.LINEITEM
	from {{ref('lineitem_t1')}} li
	left join {{ref('orders_t1')}} o
	on li.order_key = o.order_key
	left join {{ref('customer_t1')}} c
	on o.cust_key = c.cust_key
	left join {{ref('nation_t1')}} n
	on n.nation_key = c.nation_key
	inner join {{ref('region_t1')}} r
	on r.region_key = n.region_key
    where r.UPD_TS > (select upd_ts from {{ref('job_ctrl_tbl_consumption')}} where lower(tbl_nm) = 'region_t1')
)


select * from lineitem_t1_cte
union 
select * from orders_t1_cte
union 
select * from customer_t1_cte
union 
select * from nation_t1_cte
union 
select * from region_t1_cte
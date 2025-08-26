-- =========================================
-- ETL Portfolio Checks (ES/UK Unified Data)
-- =========================================

-- A) DATA QUALITY & INTEGRITY
-- [A1] Profiling: row counts per unified table
-- [A2] Primary Keys: expect 0 duplicates
-- [A3] Foreign Keys: expect 0 orphans
--      - order_items.order_id  -> orders.global_order_id
--      - orders.global_customer_id    -> customers.global_customer_id
--      - products.category_code_*     -> categories.category_code
-- [A4] Date rules: ship_date >= order_date (when ship_date IS NOT NULL)
-- [A5] Value validity: unit_price > 0, quantity > 0

-- B) BUSINESS KPIs (AGGREGATIONS)
-- [B1] Sales by month & country (gross_amount_eur)
-- [B2] Lead time stats (avg, min, max days)

-- C) ADVANCED SQL (ANALYTICS)
-- [C1] Window function: product ranking by category (DENSE_RANK by unit price)




-- [A1] 
select 'orders' as name, count(*) from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\orders.parquet' as orders
	union all
	select 'order_items' as name, count(*) from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\order_items.parquet' as order_items
	union all
	select 'products' as name, count(*) from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\products.parquet' as products
	union all
	select 'categories' as name, count(*) from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\categories.parquet' as categories
	union all
	select 'customers' as name, count(*) from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\customers.parquet' as customers

-- [A2] 
select 'orders' as table_name, count(*) as duplicate_count
	from (
		select global_order_id, count(*) as c
		from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\orders.parquet'
		group by global_order_id
		having count(*) > 1
	)
	union all
	select 'order_items', count(*)
	from (
		select order_id, line_number, count(*) as c
		from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\order_items.parquet'
		group by order_id, line_number
		having count(*) > 1
	)
	union all
	select 'products', count(*)
	from (
		select product_code, count(*) as c
		from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\products.parquet'
		group by product_code
		having count(*) > 1
	)
	union all
	select 'categories', count(*)
	from (
		select category_code, count(*) as c
		from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\categories.parquet'
		group by category_code
		having count(*) > 1
	)
	union all
	select 'customers', count(*)
	from (
		select global_customer_id, count(*) as c
		from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\customers.parquet'
		group by global_customer_id
		having count(*) > 1
	);

-- [A3] 
    select count(*) as missing_orders
	from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\order_items.parquet' as order_items
	left join 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\orders.parquet' as orders
	  on order_items.order_id = orders.global_order_id
	where orders.global_order_id is null;

-- [A4] 

    select count(*)
	from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\orders.parquet' as orders
	where ship_date < order_date;

-- [A5] 

    select count(*) as bad_prices
	from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\products.parquet' as products
	where
	  (unit_price_es is null or unit_price_es <= 0)
	  or
	  (unit_price_uk is null or unit_price_uk <= 0);
	  
	select count(*) from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\order_items.parquet'
	where
		(unit_price_eur is null or unit_price_eur <= 0)
		or
		(quantity is null or quantity <= 0 or quantity > 15);

-- [B1] 

    with order_items as( 
		select order_id, sum(quantity * unit_price_eur) as total 
		from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\order_items.parquet' 
		group by order_id ) 
	select 
		source_system, month(order_date), sum(total)
	from 'c:\users\usuario\ecommerce-etl-dashboard\data\intermediate\unified\orders.parquet' as orders 
	join order_items as oi 
		on orders.global_order_id = oi.order_id 
	group by 1,2
	order by month(order_date),source_system 

-- [B2] 

    select 
		round(avg(cast(ship_date as date) - cast(order_date as date)),2) as avg_lead_time_days,
		min(cast(ship_date as date) - cast(order_date as date)) as min_days,
		max(cast(ship_date as date) - cast(order_date as date)) as max_days,
	from 
		'C:\Users\Usuario\ecommerce-etl-dashboard\data\intermediate\unified\orders.parquet'

-- [C1] 

    SELECT
	  product_code,
	  category_code_es as category,
	  COALESCE(unit_price_es, unit_price_uk) AS unit_price_local,
	  DENSE_RANK() OVER (PARTITION BY COALESCE(category_code_es, category_code_uk)
    ORDER BY unit_price_es DESC) AS price_rank
	FROM 'C:\Users\Usuario\ecommerce-etl-dashboard\data\intermediate\unified\products.parquet' 
	WHERE unit_price_es IS NOT NULL
	ORDER BY category_code_es, price_rank;
DROP TABLE IF EXISTS hive.dwh.sales_by_category;
CREATE TABLE hive.dwh.sales_by_category
WITH (
    external_location = 's3a://lakehouse/gold/trino_queries/sales_by_category',
    format = 'PARQUET'
) AS
SELECT prod.category_name, CEILING(SUM(ord_i.total)) AS total
FROM hive.dwh.silver_order_items ord_i
LEFT JOIN hive.dwh.silver_products prod
ON ord_i.product_id = prod.product_id 
GROUP BY prod.category_name
ORDER BY total DESC;


DROP TABLE IF EXISTS hive.dwh.most_sold_products;
CREATE TABLE hive.dwh.most_sold_products
WITH (
    external_location = 's3a://lakehouse/gold/trino_queries/most_sold_products',
    format = 'PARQUET'
) AS
SELECT prod.product_name, CEILING(SUM(ord_i.total)) AS total
FROM hive.dwh.silver_order_items ord_i
LEFT JOIN hive.dwh.silver_products prod
ON ord_i.product_id = prod.product_id 
GROUP BY prod.product_name
ORDER BY total DESC
LIMIT 10;


DROP TABLE IF EXISTS hive.dwh.sales_by_store;
CREATE TABLE hive.dwh.sales_by_store
WITH (
    external_location = 's3a://lakehouse/gold/trino_queries/sales_by_store',
    format = 'PARQUET'
) AS
SELECT st.store_name, ceiling(sum(ord_i.total)) AS total
FROM hive.dwh.silver_order_items ord_i
LEFT JOIN hive.dwh.silver_orders ord ON ord_i.order_id = ord.order_id
LEFT JOIN hive.dwh.silver_stores st ON ord.store_id = st.store_id 
GROUP BY st.store_name 
ORDER BY total DESC;


DROP TABLE IF EXISTS hive.dwh.sales_by_state;
CREATE TABLE hive.dwh.sales_by_state
WITH (
    external_location = 's3a://lakehouse/gold/trino_queries/sales_by_state',
    format = 'PARQUET'
) AS
select c.state, ceiling(sum(ord_i.total)) as total
from hive.dwh.silver_order_items ord_i
left join hive.dwh.silver_orders ord on ord_i.order_id = ord.order_id
left join hive.dwh.silver_customers c on ord.customer_id = c.customer_id 
group by c.state 
order by total desc;
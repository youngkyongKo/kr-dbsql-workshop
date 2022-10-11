-- Q1. DDL 

CREATE DATABASE IF NOT EXISTS mytestdb;
USE mytestdb;

CREATE TABLE IF NOT EXISTS customers USING csv OPTIONS (
  path "/databricks-datasets/retail-org/customers",
  header "true",
  inferSchema "true"
);

CREATE TABLE IF NOT EXISTS sales_gold 
USING delta LOCATION "/databricks-datasets/retail-org/solutions/gold/sales";

CREATE TABLE IF NOT EXISTS silver_purchase_orders 
USING delta LOCATION "/databricks-datasets/retail-org/solutions/silver/purchase_orders.delta";

CREATE TABLE IF NOT EXISTS silver_sales_orders 
USING delta LOCATION "/databricks-datasets/retail-org/solutions/silver/sales_orders";

CREATE TABLE IF NOT EXISTS source_silver_suppliers 
USING delta LOCATION "/databricks-datasets/retail-org/solutions/silver/suppliers";

GRANT USAGE, CREATE, MODIFY, SELECT, READ_METADATA ON DATABASE mytestdb to `users`;

-- Q2. Simple aggregation: sales per product_category 

SELECT SUM(total_price) total_sales,
    product_category
FROM mytestdb.sales_gold
GROUP BY product_category
;

-- Q3. sales per state

SELECT c.state,  
    COUNT(s.customer_id) AS cust_count,  
    SUM(s.total_price) AS sales_revenue
FROM sales_gold s 
JOIN customers c ON c.customer_id = s.customer_id 
GROUP BY c.state
;

-- Q4. total sales for Alert

SELECT sum(total_price) AS total_sales,
  count(customer_id) AS cust_cnt
FROM sales_gold
;


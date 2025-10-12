/* ====================================================================
    Business Performance Report (Gold Layer)
    --------------------------------------------------------------------
    Description:
    This script queries the Gold layer to generate key performance metrics 
    for the business, including:
    - Total sales
    - Total quantity sold
    - Average selling price
    - Total number of orders
    - Total number of products
    - Total number of customers
    - Total customers who placed orders
    --------------------------------------------------------------------
    Source Tables:
        - Golde.fact_sales
        - Golde.dim_products
        - Golde.dim_customers
    ==================================================================== */

--------------------------------------------------------
-- 1️⃣ Total Sales
--------------------------------------------------------
SELECT SUM(sales_amount) AS total_sales
FROM Golde.fact_sales;

--------------------------------------------------------
-- 2️⃣ Total Items Sold
--------------------------------------------------------
SELECT SUM(quantity) AS total_quantity
FROM Golde.fact_sales;

--------------------------------------------------------
-- 3️⃣ Average Selling Price
--------------------------------------------------------
SELECT AVG(price) AS avg_price
FROM Golde.fact_sales;

--------------------------------------------------------
-- 4️⃣ Total Number of Orders
--------------------------------------------------------
-- Including duplicates (if any)
SELECT COUNT(order_number) AS total_orders
FROM Golde.fact_sales;

-- Unique order count
SELECT COUNT(DISTINCT order_number) AS total_unique_orders
FROM Golde.fact_sales;

--------------------------------------------------------
-- 5️⃣ Total Number of Products
--------------------------------------------------------
-- Count all product records
SELECT COUNT(product_name) AS total_products
FROM Golde.dim_products;

-- Count unique product names
SELECT COUNT(DISTINCT product_name) AS total_unique_products
FROM Golde.dim_products;

--------------------------------------------------------
-- 6️⃣ Total Number of Customers
--------------------------------------------------------
SELECT COUNT(customer_key) AS total_customers
FROM Golde.dim_customers;

--------------------------------------------------------
-- 7️⃣ Total Customers Who Placed Orders
--------------------------------------------------------
SELECT COUNT(DISTINCT customer_key) AS active_customers
FROM Golde.fact_sales;

--------------------------------------------------------
-- 8️⃣ Combined Key Metrics Report
--------------------------------------------------------
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value
FROM Golde.fact_sales
UNION ALL 
SELECT 'Total Quantity', SUM(quantity)
FROM Golde.fact_sales
UNION ALL 
SELECT 'Average Price', AVG(price)
FROM Golde.fact_sales
UNION ALL
SELECT 'Total Orders', COUNT(DISTINCT order_number)
FROM Golde.fact_sales
UNION ALL 
SELECT 'Total Products', COUNT(DISTINCT product_name)
FROM Golde.dim_products
UNION ALL
SELECT 'Total Customers', COUNT(DISTINCT customer_key)
FROM Golde.dim_customers;

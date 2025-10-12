/* =====================================================================
    Ranking Analysis – Gold Layer
    ---------------------------------------------------------------------
    Purpose:
        - To rank items (e.g., products, customers) based on performance 
          or other key business metrics.
        - To identify top-performing and low-performing entities 
          (e.g., products, customers).

    SQL Techniques Used:
        - Window Ranking Functions: RANK(), DENSE_RANK(), ROW_NUMBER()
        - Aggregate Functions: SUM(), COUNT()
        - TOP clause for simple ranking
        - GROUP BY and ORDER BY for sorting

    Source Schema:
        gold
    ===================================================================== */


--------------------------------------------------------
-- 1️⃣ Top 5 Products Generating the Highest Revenue
-- Simple aggregate ranking using TOP
--------------------------------------------------------
SELECT TOP 5
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
    ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC;


--------------------------------------------------------
-- 2️⃣ Top 5 Products by Revenue (Using Window Functions)
-- More flexible and extensible ranking using RANK()
--------------------------------------------------------
SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(f.sales_amount) AS total_revenue,
        RANK() OVER (ORDER BY SUM(f.sales_amount) DESC) AS rank_products
    FROM gold.fact_sales AS f
    LEFT JOIN gold.dim_products AS p
        ON p.product_key = f.product_key
    GROUP BY p.product_name
) AS ranked_products
WHERE rank_products <= 5;


--------------------------------------------------------
-- 3️⃣ 5 Worst-Performing Products (Lowest Revenue)
--------------------------------------------------------
SELECT TOP 5
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
    ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue ASC;


--------------------------------------------------------
-- 4️⃣ Top 10 Customers Generating the Highest Revenue
--------------------------------------------------------
SELECT TOP 10
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC;


--------------------------------------------------------
-- 5️⃣ Bottom 3 Customers with the Fewest Orders
--------------------------------------------------------
SELECT TOP 3
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT f.order_number) AS total_orders
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_orders ASC;

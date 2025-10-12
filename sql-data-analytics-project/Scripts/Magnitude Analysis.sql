/* =====================================================================
    Magnitude Analysis –> Gold Layer
    ---------------------------------------------------------------------
    Purpose:
        - To quantify business metrics such as total customers, products,
          and revenue across different dimensions (country, category, gender).
        - To understand the distribution and concentration of key measures
          in the Gold Layer of the data warehouse.

    SQL Techniques Used:
        - Aggregate Functions: SUM(), COUNT(), AVG()
        - Joins for combining fact and dimension data
        - GROUP BY and ORDER BY for summarization and ranking

    Source Schema:
        Golde
    ===================================================================== */


--------------------------------------------------------
-- 1️⃣ Total Customers by Country
--------------------------------------------------------
SELECT
    country,
    COUNT(customer_key) AS total_customers
FROM Golde.dim_customers
GROUP BY country
ORDER BY total_customers DESC;


--------------------------------------------------------
-- 2️⃣ Total Customers by Gender
--------------------------------------------------------
SELECT
    gender,
    COUNT(customer_key) AS total_customers
FROM Golde.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;


--------------------------------------------------------
-- 3️⃣ Total Products by Category
--------------------------------------------------------
SELECT
    category,
    COUNT(product_key) AS total_products
FROM Golde.dim_products
GROUP BY category
ORDER BY total_products DESC;


--------------------------------------------------------
-- 4️⃣ Average Product Cost per Category
--------------------------------------------------------
SELECT
    category,
    AVG(cost) AS avg_cost
FROM Golde.dim_products
GROUP BY category
ORDER BY avg_cost DESC;


--------------------------------------------------------
-- 5️⃣ Total Revenue by Product Category
--------------------------------------------------------
SELECT
    p.category,
    SUM(f.sales_amount) AS total_revenue
FROM Golde.fact_sales AS f
LEFT JOIN Golde.dim_products AS p
    ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;


--------------------------------------------------------
-- 6️⃣ Total Revenue by Customer
--------------------------------------------------------
SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM Golde.fact_sales AS f
LEFT JOIN Golde.dim_customers AS c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC;


--------------------------------------------------------
-- 7️⃣ Distribution of Sold Items Across Countries
--------------------------------------------------------
SELECT
    c.country,
    SUM(f.quantity) AS total_sold_items
FROM Golde.fact_sales AS f
LEFT JOIN Golde.dim_customers AS c
    ON c.customer_key = f.customer_key
GROUP BY c.country
ORDER BY total_sold_items DESC;

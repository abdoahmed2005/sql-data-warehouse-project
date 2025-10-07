-- =============================================
-- Check Quality for the Customer_Dimentions Information
-- =============================================
SELECT DISTINCT gender FROM golde.dim_customers

SELECT * FROM golde.dim_customers

-- =============================================
-- Check Quality for the Product_Dimentions Information
-- =============================================

select * from golde.dim_products

-- =============================================
-- Check Quality for the Sales_Fact Information
-- =============================================
SELECT * 
FROM Golde.fact_sales f 
LEFT JOIN Golde.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN Golde.dim_products p 
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
/* ====================================================================
    Script Name   : Silver Layer Table Creation
    --------------------------------------------------------------------
    Description   :
    This SQL script creates all **Silver Layer** tables in the Data Warehouse.
    It first checks if a table already exists (using `OBJECT_ID`) and drops it 
    to ensure a clean recreation. Each table is then created with the proper 
    schema and default metadata column (`dwh_create_date`) to track when the 
    record was inserted into the warehouse.

    --------------------------------------------------------------------
    Purpose:
    - Define the structure of cleaned and standardized tables in the Silver Layer.
    - Ensure consistent data types and naming conventions across all entities.
    - Prepare tables for data loading via the ETL process from the Bronze Layer.

    --------------------------------------------------------------------
    Tables Created:
    1. Silver.crm_cust_info      → Customer master data (CRM)
    2. Silver.crm_prd_info       → Product information (CRM)
    3. Silver.crm_salse_details  → Sales transaction details (CRM)
    4. Silver.erp_loc_a101       → Customer location and country mapping (ERP)
    5. Silver.erp_cust_az12      → Customer demographics (ERP)
    6. Silver.erp_px_cat_g1v2    → Product category and maintenance data (ERP)

    --------------------------------------------------------------------
    Notes:
    - Each table includes a `dwh_create_date` column (default = current timestamp)
      for tracking ETL load time.
    - All `DROP TABLE` statements are wrapped in conditional checks to prevent errors.
    ==================================================================== */

-- ==========================================================
-- Create Silver.crm_cust_info
-- ==========================================================
IF OBJECT_ID('Silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE Silver.crm_cust_info;
CREATE TABLE Silver.crm_cust_info ( 
    cst_id INT, 
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- ==========================================================
-- Create Silver.crm_prd_info
-- ==========================================================
IF OBJECT_ID('Silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE Silver.crm_prd_info;
CREATE TABLE Silver.crm_prd_info (
    Prd_id INT,
    cat_id NVARCHAR(50),
    Prd_key NVARCHAR(50),
    Prd_nm NVARCHAR(50),
    Prd_cost INT,
    Prd_line NVARCHAR(50),
    Prd_start_dt DATE,
    Prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- ==========================================================
-- Create Silver.crm_salse_details
-- ==========================================================
IF OBJECT_ID('Silver.crm_salse_details', 'U') IS NOT NULL
    DROP TABLE Silver.crm_salse_details;
CREATE TABLE Silver.crm_salse_details (
    Sls_ord_num NVARCHAR(50),
    Sls_prd_key NVARCHAR(50),
    Sls_cust_id INT,
    Sls_order_dt DATE,
    Sls_ship_dt DATE,
    Sls_due_dt DATE,
    Sls_sales INT,
    Sls_quantity INT,
    Sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- ==========================================================
-- Create Silver.erp_loc_a101
-- ==========================================================
IF OBJECT_ID('Silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE Silver.erp_loc_a101;
CREATE TABLE Silver.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- ==========================================================
-- Create Silver.erp_cust_az12
-- ==========================================================
IF OBJECT_ID('Silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE Silver.erp_cust_az12;
CREATE TABLE Silver.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- ==========================================================
-- Create Silver.erp_px_cat_g1v2
-- ==========================================================
IF OBJECT_ID('Silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE Silver.erp_px_cat_g1v2;
CREATE TABLE Silver.erp_px_cat_g1v2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

/* ====================================================================
    ETL Script: Data Cleaning and Loading from Bronze → Silver Layer
    --------------------------------------------------------------------
    Description:
    This SQL stored procedure performs data cleansing, standardization, 
    and transformation of data from the **Bronze** (raw data) layer to the 
    **Silver** (cleaned and standardized) layer in a data warehouse.

    Key Operations:
    1. **crm_cust_info**:
        - Removes duplicates based on `cst_id` (keeps the latest record).
        - Standardizes gender and marital status values.
        - Trims extra spaces and cleans invalid entries.

    2. **crm_prd_info**:
        - Splits `prd_key` into `cat_id` and `prd_key` parts to match 
        related tables (`crm_sales_details`, `erp_px_cat_g1v2`).
        - Fixes invalid or missing cost values.
        - Normalizes `prd_line` descriptions.
        - Corrects product end dates based on start date of next record.

    3. **crm_salse_details**:
        - Fixes invalid date formats and converts them to proper date type.
        - Corrects negative or inconsistent sales, quantity, and price data.
        - Ensures consistency: `sales = quantity * price`.

    4. **erp_cust_az12**:
        - Cleans up customer IDs (removes prefixes like 'NAS').
        - Ensures birthdates are within valid ranges.
        - Standardizes gender values.

    5. **erp_loc_a101**:
        - Removes unwanted characters in `cid`.
        - Normalizes country codes (e.g., DE → Germany, US → United States).

    6. **erp_px_cat_g1v2**:
        - Directly loads and trims category-related data.

    --------------------------------------------------------------------
    Expected Outcome:
    Clean, standardized, and reliable data ready for analysis in 
    the **Silver Layer**.
    ==================================================================== */
CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT '>> Starting Silver Layer Load';
        PRINT '>> Start Time: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
        PRINT '================================================';
        PRINT '';
        PRINT '------------------------------------------------';
        PRINT '>> Loading CRM Tables';
        PRINT '------------------------------------------------';

        --------------------------------------------------
        -- Silver.crm_cust_info
        --------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: Silver.crm_cust_info';
        TRUNCATE TABLE Silver.crm_cust_info;
        PRINT '>> Inserting Data Into: Silver.crm_cust_info';

        INSERT INTO Silver.crm_cust_info (
            cst_id, 
            cst_key, 
            cst_firstname, 
            cst_lastname, 
            cst_marital_status, 
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM Bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------';

        --------------------------------------------------
        -- Silver.crm_prd_info
        --------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: Silver.crm_prd_info';
        TRUNCATE TABLE Silver.crm_prd_info;
        PRINT '>> Inserting Data Into: Silver.crm_prd_info';

        INSERT INTO Silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            TRIM(prd_nm),
            ISNULL(prd_cost, 0),
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE),
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
        FROM Bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------';

        --------------------------------------------------
        -- Silver.crm_sales_details
        --------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: Silver.crm_sales_details';
        TRUNCATE TABLE Silver.crm_salse_details;
        PRINT '>> Inserting Data Into: Silver.crm_sales_details';

        INSERT INTO Silver.crm_salse_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE TRY_CAST(CONVERT(VARCHAR(8), sls_order_dt) AS DATE)
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE TRY_CAST(CONVERT(VARCHAR(8), sls_ship_dt) AS DATE)
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE TRY_CAST(CONVERT(VARCHAR(8), sls_due_dt) AS DATE)
            END AS sls_due_dt,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM Bronze.crm_salse_details;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------';

        --------------------------------------------------
        -- Silver.erp_cust_az12
        --------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: Silver.erp_cust_az12';
        TRUNCATE TABLE Silver.erp_cust_az12;
        PRINT '>> Inserting Data Into: Silver.erp_cust_az12';

        INSERT INTO Silver.erp_cust_az12 (cid, bdate, gen)
        SELECT 
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM Bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------';
        PRINT '';
        PRINT '------------------------------------------------';
        PRINT '>> Loading ERP Tables';
        PRINT '------------------------------------------------';

        --------------------------------------------------
        -- Silver.erp_loc_a101
        --------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: Silver.erp_loc_a101';
        TRUNCATE TABLE Silver.erp_loc_a101;
        PRINT '>> Inserting Data Into: Silver.erp_loc_a101';

        INSERT INTO Silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', ''),
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END
        FROM Bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------';

        --------------------------------------------------
        -- Silver.erp_px_cat_g1v2
        --------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: Silver.erp_px_cat_g1v2';
        TRUNCATE TABLE Silver.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: Silver.erp_px_cat_g1v2';

        INSERT INTO Silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT TRIM(id), TRIM(cat), TRIM(subcat), TRIM(maintenance)
        FROM Bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------';

        --------------------------------------------------
        -- Completion Log
        --------------------------------------------------
        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT '>> Silver Layer Load Completed Successfully';
        PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> End Time: ' + CONVERT(NVARCHAR, @batch_end_time, 120);
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT '>> ERROR OCCURRED DURING SILVER LAYER LOAD';
        PRINT '------------------------------------------------';
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR);
        PRINT 'Error Line    : ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT '================================================';
    END CATCH
END;

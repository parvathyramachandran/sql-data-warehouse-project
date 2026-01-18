/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT OFF;

    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME,
        @rows INT;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '==========================';
        PRINT 'Loading Silver Layer';
        PRINT '==========================';

    /* =====================================================
       1. silver.crm_cust_info
       ===================================================== */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';

        INSERT INTO silver.crm_cust_info
        (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_material_status, cst_gndr, cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE UPPER(TRIM(cst_material_status))
                WHEN 'S' THEN 'Single'
                WHEN 'M' THEN 'Married'
                ELSE 'N/A'
            END,
            CASE UPPER(TRIM(cst_gndr))
                WHEN 'F' THEN 'Female'
                WHEN 'M' THEN 'Male'
                ELSE 'N/A'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id
                       ORDER BY cst_create_date DESC, cst_key DESC
                   ) AS rn
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE rn = 1;

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '(' + CAST(@rows AS VARCHAR) + ' rows affected)';
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';
        PRINT '>> ------------------------';

    /* =====================================================
       2. silver.crm_prd_info
       ===================================================== */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';

        INSERT INTO silver.crm_prd_info
        (
            prd_id, cat_id, prd_key, prd_nm,
            prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
            SUBSTRING(prd_key,7,LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost,0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(LEAD(prd_start_dt) OVER
                 (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
        FROM bronze.crm_prd_info;

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '(' + CAST(@rows AS VARCHAR) + ' rows affected)';
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';
        PRINT '>> ------------------------';

    /* =====================================================
       3. silver.crm_sales_details
       ===================================================== */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';

        INSERT INTO silver.crm_sales_details
        (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
                 ELSE CONVERT(DATE, CONVERT(VARCHAR(8), sls_order_dt))
            END,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
                 ELSE CONVERT(DATE, CONVERT(VARCHAR(8), sls_ship_dt))
            END,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
                 ELSE CONVERT(DATE, CONVERT(VARCHAR(8), sls_due_dt))
            END,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity,0)
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '(' + CAST(@rows AS VARCHAR) + ' rows affected)';
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';
        PRINT '>> ------------------------';

    /* =====================================================
       4. silver.erp_cust_az12
       ===================================================== */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';

        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
            CASE 
                WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M','MALE') THEN 'Male'
                ELSE 'N/A'
            END
        FROM bronze.erp_cust_az12;

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '(' + CAST(@rows AS VARCHAR) + ' rows affected)';
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';
        PRINT '>> ------------------------';

    /* =====================================================
       5. silver.erp_loc_a101
       ===================================================== */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid,'-',''),
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'N/A'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '(' + CAST(@rows AS VARCHAR) + ' rows affected)';
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';
        PRINT '>> ------------------------';

    /* =====================================================
       6. silver.erp_px_cat_g1v2
       ===================================================== */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT
            id,
            TRIM(cat),
            TRIM(subcat),
            TRIM(maintenance)
        FROM bronze.erp_px_cat_g1v2;

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '(' + CAST(@rows AS VARCHAR) + ' rows affected)';
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) + ' seconds';
        PRINT '>> ------------------------';

        SET @batch_end_time = GETDATE();

        PRINT '==============================';
        PRINT 'Loading Silver Layer Completed';
        PRINT 'Total Load Duration: ' 
              + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS VARCHAR)
              + ' seconds';
        PRINT '==============================';

    END TRY
    BEGIN CATCH
        PRINT '==============================';
        PRINT 'ERROR OCCURRED DURING SILVER LOAD';
        PRINT ERROR_MESSAGE();
        PRINT '==============================';
    END CATCH
END;
GO

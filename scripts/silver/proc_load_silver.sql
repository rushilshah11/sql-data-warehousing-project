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


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMPTZ;
    end_time   TIMESTAMPTZ;
    duration   INTERVAL;
BEGIN
    -- Record the START TIME
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '================================================';
    RAISE NOTICE '[%] Starting FULL Load to Silver Layer...', TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI:SS');
    RAISE NOTICE '================================================';

    --
    -- 1. TRUNCATE PHASE (Clear all target tables)
    --
    RAISE NOTICE '[%] Truncating Silver Tables...', CLOCK_TIMESTAMP();

    TRUNCATE TABLE silver.crm_cust_info;
    TRUNCATE TABLE silver.crm_prd_info;
    TRUNCATE TABLE silver.crm_sales_details;
    TRUNCATE TABLE silver.erp_cust_az12;
    TRUNCATE TABLE silver.erp_loc_a101;
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    --
    -- 2. LOAD & CLEANSING PHASE
    --

    -- Load silver.crm_cust_info (CRM Customer)
    RAISE NOTICE '[%] Loading silver.crm_cust_info (CRM Customer)...', CLOCK_TIMESTAMP();
    INSERT INTO silver.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname, cst_material_staus, cst_gndr, cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        -- Cleansing: Removes unwanted leading/trailing spaces
        trim(cst_firstname) AS cst_firstname,
        trim(cst_lastname) AS cst_lastname,
        -- Cleansing/Standardization: Maps S/M to Single/Married
        CASE
            WHEN upper(trim(cst_material_staus)) = 'S' THEN 'Single'
            WHEN upper(trim(cst_material_staus)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_material_staus,
        -- Cleansing/Standardization: Maps F/M to Female/Male
        CASE
            WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
            WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    -- Cleansing: Filters for non-null IDs and keeps only the latest (flag_last = 1) for duplicate removal
    FROM (
        SELECT
            *,
            row_number() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info cci
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;


    -- Load silver.crm_prd_info (CRM Product)
    RAISE NOTICE '[%] Loading silver.crm_prd_info (CRM Product)...', CLOCK_TIMESTAMP();
    INSERT INTO silver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        -- Cleansing/Extraction: Extracts category and standardizes delimiter ('-' to '_')
        replace(substring(prd_key, 1, 5), '-', '_') AS cat_id,
        -- Cleansing/Extraction: Extracts actual product key
        substring(prd_key, 7, length(prd_key)) AS prd_key,
        prd_nm,
        -- Cleansing: Handles missing cost values by setting them to 0
        coalesce(prd_cost, 0) AS prd_cost,
        -- Cleansing/Standardization: Converts line codes (M/R/S/T) to full names
        CASE
            WHEN upper(trim(prd_line)) = 'M' THEN 'Mountain'
            WHEN upper(trim(prd_line)) = 'R' THEN 'Road'
            WHEN upper(trim(prd_line)) = 'S' THEN 'Other Sales'
            WHEN upper(trim(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        -- Cleansing: Ensures date type consistency
        cast(prd_start_dt AS date) AS prd_start_dt,
        -- Cleansing/Temporal Fix: Sets end date to day before next row's start date
        cast(
            lead(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - interval '1 day'
            AS date
        ) AS prd_end_dt
    FROM bronze.crm_prd_info;


    -- Load silver.crm_sales_details (CRM Sales)
    RAISE NOTICE '[%] Loading silver.crm_sales_details (CRM Sales)...', CLOCK_TIMESTAMP();
    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        -- Cleansing/Transformation: Validates and converts YYYYMMDD integer to DATE.
        CASE
            WHEN sls_order_dt = 0 OR length(sls_order_dt::text) != 8 THEN null
            ELSE to_date(sls_order_dt::text, 'YYYYMMDD')
        END AS sls_order_dt,
        -- Cleansing/Transformation: Validates and converts YYYYMMDD integer to DATE.
        CASE
            WHEN sls_ship_dt = 0 OR length(sls_ship_dt::text) != 8 THEN null
            ELSE to_date(sls_ship_dt::text, 'YYYYMMDD')
        END AS sls_ship_dt,
        -- Cleansing/Transformation: Validates and converts YYYYMMDD integer to DATE.
        CASE
            WHEN sls_due_dt = 0 OR length(sls_due_dt::text) != 8 THEN null
            ELSE to_date(sls_due_dt::text, 'YYYYMMDD')
        END AS sls_due_dt,
        -- Cleansing: Corrects sales value if missing/invalid by recalculating (qty * abs(price)).
        CASE
            WHEN sls_sales IS null OR sls_sales <= 0 THEN sls_quantity * abs(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        -- Cleansing: Corrects price value if missing/invalid by recalculating (sales / qty), using NULLIF.
        CASE
            WHEN sls_price IS null OR sls_price <= 0 THEN sls_sales / nullif(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;

    -- Load silver.erp_cust_az12 (ERP Customer)
    RAISE NOTICE '[%] Loading silver.erp_cust_az12 (ERP Customer)...', CLOCK_TIMESTAMP();
    INSERT INTO silver.erp_cust_az12 (
        cid, bdate, gen
    )
    SELECT
        -- Transformation: Remove 'NAS' prefix
        CASE
            WHEN cid LIKE 'NAS%' THEN substring(cid, 4, length(cid))
            ELSE cid
        END AS cid,
        -- Cleansing: Nullify future birth dates
        CASE
            WHEN bdate > now() THEN null
            ELSE bdate
        END AS bdate,
        -- Standardization: Normalize gender values
        CASE
            WHEN upper(trim(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN upper(trim(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;


    -- Load silver.erp_loc_a101 (ERP Location)
    RAISE NOTICE '[%] Loading silver.erp_loc_a101 (ERP Location)...', CLOCK_TIMESTAMP();
    INSERT INTO silver.erp_loc_a101 (
        cid, cntry
    )
    SELECT
        -- Cleansing: Remove hyphens from Customer ID
        replace(cid, '-', '') AS cid,
        -- Standardization: Map country codes to full names and handle null/blank values
        CASE
            WHEN trim(cntry) = 'DE' THEN 'Germany'
            WHEN trim(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN trim(cntry) = '' OR cntry IS null THEN 'n/a'
            ELSE trim(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;


    -- Load silver.erp_px_cat_g1v2 (ERP Product Category)
    RAISE NOTICE '[%] Loading silver.erp_px_cat_g1v2 (ERP Product Category)...', CLOCK_TIMESTAMP();
    INSERT INTO silver.erp_px_cat_g1v2 (
        id, cat, subcat, maintenance
    )
    SELECT
        id, cat, subcat, maintenance
    -- NO TRANSFORMATION: Straight-through load
    FROM bronze.erp_px_cat_g1v2;


    --
    -- 3. FINALIZATION
    --
    
    -- Record END TIME and calculate DURATION
    end_time := CLOCK_TIMESTAMP();
    duration := end_time - start_time;

    RAISE NOTICE '================================================';
    RAISE NOTICE '[%] FULL Load to Silver Layer Complete.', TO_CHAR(end_time, 'YYYY-MM-DD HH24:MI:SS');
    RAISE NOTICE 'TOTAL DURATION: %', duration;
    RAISE NOTICE '================================================';

EXCEPTION
    -- Catch all errors and log them before re-raising
    WHEN OTHERS THEN
        RAISE EXCEPTION '[%] Silver Layer Load FAILED. SQLSTATE: %, Error: %', CLOCK_TIMESTAMP(), SQLSTATE, SQLERRM;
END;
$$;

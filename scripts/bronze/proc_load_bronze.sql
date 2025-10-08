/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
-- DECLARE section: This is where you declare variables
DECLARE
    start_time TIMESTAMPTZ;
    end_time   TIMESTAMPTZ;
    duration   INTERVAL;
BEGIN
    -- 1. Record the START TIME immediately upon entering the procedure
    start_time := CLOCK_TIMESTAMP();
    
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer - START: %', TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI:SS');
    RAISE NOTICE '================================================';

    RAISE NOTICE 'Loading CRM Tables...';
    
    -- Load bronze.crm_cust_info
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM '/Users/rushilshah/Documents/CSProjects/sql-data-warehousing-project/datasets/source_crm/cust_info.csv'
    DELIMITER ','
    CSV HEADER;

    -- Load bronze.crm_prd_info
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/Users/rushilshah/Documents/CSProjects/sql-data-warehousing-project/datasets/source_crm/prd_info.csv'
    DELIMITER ','
    CSV HEADER;

    -- Load bronze.crm_sales_details
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/Users/rushilshah/Documents/CSProjects/sql-data-warehousing-project/datasets/source_crm/sales_details.csv'
    DELIMITER ','
    CSV HEADER;

    RAISE NOTICE 'Loading ERP Tables...';
    
    -- Load bronze.erp_loc_a101
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/Users/rushilshah/Documents/CSProjects/sql-data-warehousing-project/datasets/source_erp/loc_a101.csv'
    DELIMITER ','
    CSV HEADER;

    -- Load bronze.erp_cust_az12
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/Users/rushilshah/Documents/CSProjects/sql-data-warehousing-project/datasets/source_erp/cust_az12.csv'
    DELIMITER ','
    CSV HEADER;

    -- Load bronze.erp_px_cat_g1v2
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/Users/rushilshah/Documents/CSProjects/sql-data-warehousing-project/datasets/source_erp/px_cat_g1v2.csv'
    DELIMITER ','
    CSV HEADER;
    
    -- 2. Record the END TIME
    end_time := CLOCK_TIMESTAMP();
    
    -- 3. Calculate the DURATION
    duration := end_time - start_time;
    
    -- 4. Output the final duration
    RAISE NOTICE 'Bronze layer loading complete. END: %', TO_CHAR(end_time, 'YYYY-MM-DD HH24:MI:SS');
    RAISE NOTICE 'TOTAL DURATION: %', duration;

EXCEPTION
    -- Error logging section: Still important to log an error time
    WHEN undefined_file THEN
        RAISE EXCEPTION '[%] File Not Found Error: Could not locate a specified input file.', CLOCK_TIMESTAMP();

    WHEN data_exception THEN
        RAISE EXCEPTION '[%] Data Format Error: Check the CSV file structure and table definitions. Error: %', CLOCK_TIMESTAMP(), SQLERRM;

    WHEN OTHERS THEN
        RAISE WARNING '[%] An UNEXPECTED ERROR occurred. SQLSTATE: %, Error Message: %', CLOCK_TIMESTAMP(), SQLSTATE, SQLERRM;
        RAISE; 

END;
$$;

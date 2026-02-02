/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

USE data_warehouse;
GO

-- ====================================================================
-- Checking 'bronze.crm_cust_info' Before Inserting Data Into 'silver.crm_cust_info'
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT cst_id, COUNT(*) AS cnt
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces in String Values
-- Expectation: No Results
SELECT cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
    OR cst_lastname != TRIM(cst_lastname)
    OR cst_marital_status != TRIM(cst_marital_status)
    OR cst_gndr != TRIM(cst_gndr);

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;


-- ====================================================================
-- Checking 'bronze.crm_prd_info' Before Inserting Data Into 'silver.crm_prd_info'
-- ====================================================================

-- Check for Null or Duplicates in Primary Key
-- Expectation: No Results
SELECT prd_id, COUNT(*) AS cnt
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces in String Values
-- Expectation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt;


-- ====================================================================
-- Checking 'bronze.crm_sales_details' Before Inserting Data Into 'silver.crm_sales_details'
-- ====================================================================

-- Check for Unwanted Spaces in String Values
-- Expectation: No Results
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Check connecting to another tables
-- Expectation: 'sls_prd_key' values from 'crm_sales_details' table exist in 'crm_prd_info' table with matching 'prd_key'. 
-- We compare 'bronze.crm_sales_details' and 'silver.crm_prd_info'
SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
    SELECT prd_key
FROM silver.crm_prd_info
)

-- Expectation: 'sls_cust_id' values from 'crm_sales_details' table exist in 'crm_cust_info' table with matching 'cst_id'. 
-- We compare 'bronze.crm_sales_details' and 'silver.crm_cust_info'
SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
    SELECT cst_id
FROM silver.crm_cust_info
)

-- Check for Invalid Dates
-- Expectation: No Invalid Dates

-- In our dataset columns 'sls_order_dt', 'sls_ship_dt', 'sls_due_dt' are stored as INT and we need to check for invalid date values 
-- Negative numbers or zeros can't be CAST to a Date

SELECT sls_order_dt,
    NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 -- We have zeros in the data that's why we must Replace '0' into NULL
    -- Our Integer data has the year's information at the start, then the month and the day at the end (YYYYMMDD format). 
    -- In this scenario the LENGTH of the Integer should be 8 characters
    OR LEN(sls_order_dt) != 8 -- We have values that not equal to 8 and we must to Replace these values into NULL with CASE statement
    -- Check for outliers by Validating the Boundaries of the date range (we expect the dates to be between '2000-01-01' and '2026-12-31')
    OR sls_order_dt > 20261231 -- no results
    OR sls_order_dt < 20000101;
-- no results

SELECT sls_ship_dt,
    NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 -- no results
    -- Our Integer data has the year's information at the start, then the month and the day at the end (YYYYMMDD format). 
    -- In this scenario the LENGTH of the Integer should be 8 characters
    OR LEN(sls_ship_dt) != 8 -- no results
    -- Check for outliers by Validating the Boundaries of the date range (we expect the dates to be between '2000-01-01' and '2026-12-31')
    OR sls_ship_dt > 20261231 -- no results
    OR sls_ship_dt < 20000101;
-- no results

SELECT sls_due_dt,
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 -- no results
    -- Our Integer data has the year's information at the start, then the month and the day at the end (YYYYMMDD format). 
    -- In this scenario the LENGTH of the Integer should be 8 characters
    OR LEN(sls_due_dt) != 8 -- no results
    -- Check for outliers by Validating the Boundaries of the date range (we expect the dates to be between '2000-01-01' and '2026-12-31')
    OR sls_due_dt > 20261231 -- no results
    OR sls_due_dt < 20000101;
-- no results

-- Check for Invalid Dates Orders: 'sls_order_dt' must always be earlier than the 'sls_ship_dt' and 'sls_due_dt' (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt;

-- Check Data Consistency between 'sls_sales', 'sls_quantity' and 'sls_price' columns
/* Business Rules:
->> Sales = Quantity * Price
->> Values must not be NULL, zero or negative */
-- Expectation: No Results
SELECT DISTINCT sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price
-- , CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) -- ABS to handle negative prices
--     THEN sls_quantity * ABS(sls_price)
--     ELSE sls_sales
-- END AS sls_sales,
-- CASE WHEN sls_price IS NULL OR sls_price <= 0
--     THEN sls_sales / NULLIF(sls_quantity,0) -- NULLIF to avoid division by zero
--     ELSE sls_price
-- END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL OR sls_sales <= 0
    OR sls_quantity IS NULL OR sls_quantity <= 0
    OR sls_price IS NULL OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


-- ====================================================================
-- Checking 'bronze.erp_cust_az12' Before Inserting Data Into 'silver.erp_cust_az12'
-- ====================================================================

-- Check connection between 'cid' from 'erp_cust_az12' and 'cst_key' from 'crm_cust_info' table
-- Expectation: 'cid' values from 'erp_cust_az12' table exist in 'crm_cust_info' table with matching 'cst_key'
SELECT cid
    , CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' Prefix from 'cid'
        ELSE cid
    END AS cid
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
        ELSE cid
    END NOT IN (
        SELECT DISTINCT cst_key
FROM silver.crm_cust_info);

-- Identify Out-of-Range Birth Dates
-- Expectation: Birthdates between 1926-01-01 and Today
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1926-01-01' OR bdate > GETDATE()
ORDER BY bdate;

-- Data Standardization & Consistency

/* Inside the value of the 'gen' field there is a hidden string carry symbol.  
Arrow ↵ means newline character.
Arrow ↵ in grid VS Code = Carriage Return / Line Feed, that is:
CHAR(13) → CR
CHAR(10) → LF */
SELECT DISTINCT gen
-- , CASE WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('F', 'FEMALE')
--     THEN 'Female'
--     WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('M', 'MALE')
--     THEN 'Male'
--     ELSE 'n/a'
-- END AS gen,
-- LEN(TRIM(REPLACE(gen, CHAR(13), ''))) AS gen_trimmed_length
FROM bronze.erp_cust_az12;

-- Researching which numerical code the symbol will turn into using UNICODE()
/* This query takes each unique value of 'gen', extracts the last character, and shows its numeric code to find hidden characters like 
'CR' CHAR(13), 'LF' CHAR(10), spaces, or 'NBSP' CHAR(160) */
SELECT
    DISTINCT gen,
    UNICODE(SUBSTRING(gen, LEN(gen), 1)) AS last_char_code
FROM bronze.erp_cust_az12;

/* CHAR(13) - the Carriage Return (CR) character. Its primary function is to move the cursor to the beginning of the current line without advancing to the next line (macOS)
CHAR(10) - the Line Feed (LF) character. Its primary function is to move the cursor down one line without returning to the beginning of the line. 
These two characters are commonly used in combination to signify a new line, but usage varies by operating system.  */

SELECT DISTINCT gen,
    LEN(gen) AS gen_length,
    REPLACE(gen, CHAR(13), '') AS gen_no_cr,
    LEN(REPLACE(gen, CHAR(13), '')) AS gen_no_cr_length,
    TRIM(REPLACE(gen, CHAR(13), '')) AS gen_trimmed,
    LEN(TRIM(REPLACE(gen, CHAR(13), ''))) AS gen_trimmed_length
FROM bronze.erp_cust_az12;

-- REPLACE Below is NOT working
/* SELECT DISTINCT gen, LEN(gen) AS gen_length,
    REPLACE(gen, '\n', '') AS gen_no_cr, 
    LEN(REPLACE(gen, '\n', '')) AS gen_no_cr_length, 
    TRIM(REPLACE(gen, '\n', '')) AS gen_trimmed, 
    LEN(TRIM(REPLACE(gen, '\n', ''))) AS gen_trimmed_length
FROM bronze.erp_cust_az12; */

/* \n — LF (Line Feed) — goes to a new line (Linux/macOS)
\r — CR (Carriage Return) - returns the cursor to the beginning of the line, but does not switch to a new one
\r\n — CRLF (Carriage Return + Line Feed) — returns the cursor and goes to a new line (Windows) */


-- ====================================================================
-- Checking 'bronze.erp_loc_a101' Before Inserting Data Into 'silver.erp_loc_a101'
-- ====================================================================

-- Check connection between 'cid' from 'erp_loc_a101' and 'cst_key' from 'crm_cust_info' table
-- Expectation: 'cid' values from 'erp_loc_a101' table exist in 'crm_cust_info' table with matching 'cst_key'
SELECT cid,
    TRIM(REPLACE(cid, '-', '')) as cid
-- Remove '-' from 'cid'
FROM bronze.erp_loc_a101
WHERE TRIM(REPLACE(cid, '-', '')) NOT IN (SELECT cst_key
FROM silver.crm_cust_info);

-- Data Standardization & Consistency

/* Inside the value of the 'cntry' field there is a hidden string carry symbol.  
Arrow ↵ means newline character.
Arrow ↵ in grid VS Code = Carriage Return / Line Feed, that is:
CHAR(13) → CR
CHAR(10) → LF */
SELECT DISTINCT cntry AS old_cntry
-- , CASE WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = 'DE' THEN 'Germany'
-- WHEN TRIM(REPLACE(cntry, CHAR(13), '')) IN ('US', 'USA') THEN 'United States'
-- WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = '' OR cntry IS NULL THEN 'n/a'
-- ELSE TRIM(REPLACE(cntry, CHAR(13), ''))
-- END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

-- Researching which numerical code the symbol will turn into using UNICODE()
SELECT
    DISTINCT cntry,
    UNICODE(SUBSTRING(cntry, LEN(cntry), 1)) AS last_char_code
FROM bronze.erp_loc_a101;


-- ====================================================================
-- Checking 'bronze.erp_px_cat_g1v2' Before Inserting Data Into 'silver.erp_px_cat_g1v2'
-- ====================================================================

-- Check connection between 'id' from 'bronze.erp_px_cat_g1v2' and 'cat_id' (a new column) from 'silver.crm_prd_info' table
-- Expectation: 'id' values from 'bronze.erp_px_cat_g1v2' table exist in 'silver.crm_prd_info' table with matching 'cat_id'
SELECT id
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (
SELECT cat_id
FROM silver.crm_prd_info);

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
    OR subcat != TRIM(subcat)
    OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency

SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;

/* Inside the value of the 'maintenance' field there is a hidden string carry symbol.  
Arrow ↵ means newline character.
Arrow ↵ in grid VS Code = Carriage Return / Line Feed, that is:
CHAR(13) → CR
CHAR(10) → LF */
SELECT DISTINCT maintenance AS old_maintenance
-- , TRIM(REPLACE(maintenance, CHAR(13), '')) AS maintenance
FROM bronze.erp_px_cat_g1v2
ORDER BY maintenance;

-- Researching which numerical code the symbol will turn into using UNICODE()
SELECT
    DISTINCT maintenance,
    UNICODE(SUBSTRING(maintenance, LEN(maintenance), 1)) AS last_char_code
FROM bronze.erp_px_cat_g1v2;
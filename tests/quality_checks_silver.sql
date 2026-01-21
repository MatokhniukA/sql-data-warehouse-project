USE data_warehouse;
GO

-- CLEAN & LOAD crm_cust_info

-- Check for Null or Duplicates in Primary Key
-- Expectation: No Results
SELECT cst_id, COUNT(*) AS cnt
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces in String Values
-- Expectation: No Results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;


-- CLEAN & LOAD crm_prd_info

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

-- Check for NULLs or Negative Values
-- Expectation: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Check for Invalid Date Orders
-- Expectation: No Results
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt;


-- CLEAN & LOAD crm_sales_details

-- Check for Unwanted Spaces in String Values
-- Expectation: No Results
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Check connecting to another tables

-- Expectation: 'sls_prd_key' values from 'crm_sales_details' table exist in 'crm_prd_info' table with matching 'prd_key'. We compare 'bronze.crm_sales_details' and 'silver.crm_prd_info'
SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
    SELECT prd_key
FROM silver.crm_prd_info
)

-- Expectation: 'sls_cust_id' values from 'crm_sales_details' table exist in 'crm_cust_info' table with matching 'cst_id'. We compare 'bronze.crm_sales_details' and 'silver.crm_cust_info'
SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
    SELECT cst_id
FROM silver.crm_cust_info
)

-- Check for Invalid Dates
-- Expectation: No Results

-- In our dataset columns 'sls_order_dt', 'sls_ship_dt', 'sls_due_dt' are stored as INT and we need to check for invalid date values (e.g., Negative numbers or zeros that can't be CAST to a Date)

SELECT sls_order_dt,
    NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 -- We have zeros in the data that's why we must Replace '0' into NULL
    -- Our Integer data has the year's information at the start, then the month and the day at the end (YYYYMMDD format). In this scenario the LENGTH of the Integer should be 8 characters
    OR LEN(sls_order_dt) != 8 -- We have values that not equal to 8 and we must to Replace these values into NULL with CASE statement
    -- Check for outliers by Validating the Boundaries of the date range (we expect the dates to be between '2000-01-01' and '2026-12-31')
    OR sls_order_dt > 20261231 -- no results
    OR sls_order_dt < 20000101;
-- no results

SELECT sls_ship_dt,
    NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 -- no results
    -- Our Integer data has the year's information at the start, then the month and the day at the end (YYYYMMDD format). In this scenario the LENGTH of the Integer should be 8 characters
    OR LEN(sls_ship_dt) != 8 -- no results
    -- Check for outliers by Validating the Boundaries of the date range (we expect the dates to be between '2000-01-01' and '2026-12-31')
    OR sls_ship_dt > 20261231 -- no results
    OR sls_ship_dt < 20000101;
-- no results

SELECT sls_due_dt,
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 -- no results
    -- Our Integer data has the year's information at the start, then the month and the day at the end (YYYYMMDD format). In this scenario the LENGTH of the Integer should be 8 characters
    OR LEN(sls_due_dt) != 8 -- no results
    -- Check for outliers by Validating the Boundaries of the date range (we expect the dates to be between '2000-01-01' and '2026-12-31')
    OR sls_due_dt > 20261231 -- no results
    OR sls_due_dt < 20000101;
-- no results

-- Check for Invalid Dates Orders: 'sls_order_dt' must always be earlier than the 'sls_ship_dt' and 'sls_due_dt'
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
SELECT DISTINCT sls_sales AS old_sls_sales, sls_quantity, sls_price AS old_sls_price,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) -- ABS to handle negative prices
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0) -- NULLIF to avoid division by zero
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL OR sls_sales <= 0
    OR sls_quantity IS NULL OR sls_quantity <= 0
    OR sls_price IS NULL OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


-- 
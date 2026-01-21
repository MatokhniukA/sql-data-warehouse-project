USE data_warehouse;
GO

-- CLEAN & LOAD crm_cust_info

INSERT INTO silver.crm_cust_info
    (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
    )
SELECT cst_id,
    cst_key,
    -- Data Transformation: Remove Unwanted Spaces in String Values
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    -- Data Transformation: Data Standardization & Consistency (Store Clear and Meaningful Values Rather than Using Abbraviated Terms)
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a' -- Handling missing values (NULLs)
    END AS cst_marital_status, -- Normalize marital status values to readable format
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a' -- Handling missing values (NULLs)
    END AS cst_gndr, -- Normalize gender values to readable format
    cst_create_date
FROM (
    -- Data Transformation: Remove Duplicates Based on Latest Create Date (The Most "Fresh" Data)
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
    from bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
    ) AS t
WHERE flag_last = 1;
-- Select the most recent record per customer ID

-- Quality Check: Verify Data Loaded into silver.crm_cust_info
SELECT *
FROM silver.crm_cust_info;


-- CLEAN & LOAD crm_prd_info

INSERT INTO silver.crm_prd_info
    (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
    )
SELECT prd_id,
    -- 'prd_key' is Split into 2 Informations and Deriving 2 New Columns ('cat_id' and 'prd_key')
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
    /* There Is a Column 'id' in the Table 'erp_px_cat_g1v2' That Matches the 'cat_id' from the 'crm_prd_info' Table
    SELECT DISTINCT id
    FROM bronze.erp_px_cat_g1v2 */
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract product key
    /* There Is a Column 'sls_prd_key' in the Table 'crm_sales_details' That Matches the 'prd_key' from the 'crm_prd_info' Table
    SELECT sls_prd_key
    FROM bronze.crm_sales_details */
    -- Data Transformation: Remove Unwanted Spaces in String Values
    TRIM(prd_nm) AS prd_nm,
    -- Data Transformation: Replace NULL Values with '0' 
    ISNULL(prd_cost, 0) AS prd_cost,
    -- Data Transformation: Data Standardization & Consistency (Store Clear and Meaningful Values Rather than Using Abbraviated Terms)
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a' -- Handling missing values (NULLs)
    END AS prd_line, -- Map product line codes to descriptive values
    -- Data Transformation: Converting Data Type DATETIME to DATE and Ensuring Valid Date Orders
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    -- Data Transformation: Calculate 'prd_end_dt' Based on the Next 'prd_start_dt' for the Same 'prd_key' 
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
-- Data Enrichment: Add new, relevant Data ('prd_end_dt') to Enhance the Dataset for Analysis
FROM
    (
    -- Data Transformation: Remove Duplicates in Primary Key
    SELECT *, COUNT(*) OVER (PARTITION BY prd_id) AS cnt
    FROM bronze.crm_prd_info
    WHERE prd_id IS NOT NULL
    )
AS t
WHERE cnt = 1;
/*  AND REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN
   (SELECT DISTINCT id
    FROM bronze.erp_px_cat_g1v2) */
/* AND SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
    SELECT sls_prd_key
    FROM bronze.crm_sales_details) */

-- Quality Check: Verify Data Loaded into silver.crm_prd_info
SELECT *
FROM silver.crm_prd_info;


-- CLEAN & LOAD crm_sales_details

INSERT INTO silver.crm_sales_details
    (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
    )
SELECT TRIM(sls_ord_num) AS sls_ord_num, -- Data Transformation: Remove Unwanted Spaces in String Values
    sls_prd_key,
    sls_cust_id,
    -- Data Transformation: Replace Invalid Dates to NULL and Convert Integer Date to Proper Date Format
    CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- CAST first to VARCHAR because we cannot cast from INT to DATE directly in SQL Server. AND then - from VARCHAR to DATE
    END AS sls_order_dt, -- Handle Invalid Dates 
    CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) -- CAST first to VARCHAR because we cannot cast from INT to DATE directly in SQL Server. AND then - from VARCHAR to DATE
    END AS sls_ship_dt, -- Handle Invalid Dates 
    CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) -- CAST first to VARCHAR because we cannot cast from INT to DATE directly in SQL Server. AND then - from VARCHAR to DATE
    END AS sls_due_dt, -- Handle Invalid Dates 
    -- Data Transformation: Recalculate sales if original value is NULL or incorrect
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) -- ABS to handle negative prices
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    -- Data Transformation: Derive price if original value is NULL or incorrect
    CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0) -- NULLIF to avoid division by zero
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;
-- Connecting to another tables
/* WHERE sls_prd_key NOT IN (
    SELECT prd_key
    FROM silver.crm_prd_info
)
    AND sls_cust_id NOT IN (
    SELECT cst_id
FROM silver.crm_cust_info
) */

-- Quality Check: Verify Data Loaded into silver.crm_sales_details
SELECT *
FROM silver.crm_sales_details;

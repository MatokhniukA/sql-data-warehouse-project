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
    -- Data Transformation: Trim Unwanted Spaces in String Values
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
    -- prd_key is Split into 2 Informations and Deriving 2 New Columns (cat_id and prd_key)
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    /* There Is a Column 'id' in the Table 'erp_px_cat_g1v2' That Matches the 'cat_id' from the 'crm_prd_info' Table
    SELECT DISTINCT id
    FROM bronze.erp_px_cat_g1v2 */
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    /* There Is a Column 'sls_prd_key' in the Table 'crm_sales_details' That Matches the 'prd_key' from the 'crm_prd_info' Table
    SELECT sls_prd_key
    FROM bronze.crm_sales_details */
    -- Data Transformation: Trim Unwanted Spaces in String Values
    TRIM(prd_nm) AS prd_nm,
    -- Replaces NULL Values with '0' and Ensures No Negative Values Exist
    ISNULL(prd_cost, 0) AS prd_cost,
    -- Data Transformation: Data Standardization & Consistency (Store Clear and Meaningful Values Rather than Using Abbraviated Terms)
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a' -- Handling missing values (NULLs)
    END AS prd_line, -- Normalize product line values to readable format
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
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
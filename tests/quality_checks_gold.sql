USE data_warehouse;

-- Dimension Customers

-- Check for Duplicates after Joining Tables
-- Expectation: No Results
SELECT cst_id,
    COUNT(*) AS count
FROM (
SELECT ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
    FROM silver.crm_cust_info AS ci
        LEFT JOIN silver.erp_cust_az12 AS ca
        ON ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 AS la
        ON ci.cst_key = la.cid
) AS t
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Check for Gender Information
-- 'cst_gndr' comes from CRM, 'gen' comes from ERP
-- We have to do Data Integration (Get Data From the Two Sources in One)
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is Master table for Gender Info
    ELSE COALESCE(ca.gen, 'n/a') -- Fallback to ERP data
    END AS new_gen
FROM silver.crm_cust_info AS ci
    LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid
ORDER BY ci.cst_gndr, ca.gen;

-- Check Quality of Customers View
SELECT *
FROM gold.dim_customers;

SELECT DISTINCT gender
FROM gold.dim_customers;


-- Dimension Products

-- Check for Duplicates after Joining Tables
-- Expectation: No Results
SELECT prd_key,
    COUNT(*) as count
FROM (
    SELECT
        pi.prd_id,
        pi.cat_id,
        pi.prd_key,
        pi.prd_nm,
        pi.prd_cost,
        pi.prd_line,
        pi.prd_start_dt,
        pi.prd_end_dt,
        pc.cat,
        pc.subcat,
        pc.maintenance
    FROM silver.crm_prd_info AS pi
        LEFT JOIN silver.erp_px_cat_g1v2 AS pc
        ON pi.cat_id = pc.id
    WHERE pi.prd_end_dt IS NULL -- Filter out all historical data
) AS t
GROUP BY prd_key
HAVING COUNT(*) > 1;

-- Check Quality of Products View
SELECT *
FROM gold.dim_products;


-- Fact Sales

-- Check Quality of Sales View
SELECT *
FROM gold.fact_sales;

-- Foreign Key Integrity (Dimensions)
-- Expectation: No Results

SELECT *
FROM gold.fact_sales AS s
    LEFT JOIN gold.dim_customers AS c
    ON s.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

SELECT *
FROM gold.fact_sales AS s
    LEFT JOIN gold.dim_products AS p
    ON s.product_key = p.product_key
WHERE p.product_key IS NULL;
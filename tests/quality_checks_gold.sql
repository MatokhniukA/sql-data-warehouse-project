/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

USE data_warehouse;
GO

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================

-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Check for Duplicates after Joining Tables
-- Expectation: No Results
SELECT cst_id,
    COUNT(*) AS duplicate_count
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


-- ====================================================================
-- Checking 'gold.dim_products'
-- ====================================================================

-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

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


-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================

-- Check the data model connectivity between fact and dimensions (Foreign Key Integrity)
-- Expectation: No Results
SELECT *
FROM gold.fact_sales AS s
    LEFT JOIN gold.dim_customers AS c
    ON s.customer_key = c.customer_key
    LEFT JOIN gold.dim_products AS p
    ON s.product_key = p.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL;
/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks of loaded data across the 'bronze' layer.
    It includes:
    - Check that the data have not shifted and are in the correct columns.
    - Count loaded data in the table and compare with the data in the csv file.
===============================================================================
*/

USE data_warehouse;
GO
-- ====================================================================
-- Checking 'bronze.crm_cust_info'
-- ====================================================================
SELECT *
FROM bronze.crm_cust_info;

SELECT COUNT(*)
FROM bronze.crm_cust_info;

-- ====================================================================
-- Checking 'bronze.crm_prd_info'
-- ====================================================================
SELECT *
FROM bronze.crm_prd_info;

SELECT COUNT(*)
FROM bronze.crm_prd_info;

-- ====================================================================
-- Checking 'bronze.crm_sales_details'
-- ====================================================================
SELECT *
FROM bronze.crm_sales_details;

SELECT COUNT(*)
FROM bronze.crm_sales_details;

-- ====================================================================
-- Checking 'bronze.erp_cust_az12'
-- ====================================================================
SELECT *
FROM bronze.erp_cust_az12;

SELECT COUNT(*)
FROM bronze.erp_cust_az12;

-- ====================================================================
-- Checking 'bronze.erp_loc_a101'
-- ====================================================================
SELECT *
FROM bronze.erp_loc_a101;

SELECT COUNT(*)
FROM bronze.erp_loc_a101;

-- ====================================================================
-- Checking 'bronze.erp_px_cat_g1v2'
-- ====================================================================
SELECT *
FROM bronze.erp_px_cat_g1v2;

SELECT COUNT(*)
FROM bronze.erp_px_cat_g1v2;
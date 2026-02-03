USE data_warehouse;
GO

-- Dimension Customers
CREATE VIEW gold.dim_customers
AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, -- This is Surrogate Key = Primary Key
        ci.cst_id AS customer_id,
        ci.cst_key AS customer_number,
        ci.cst_firstname AS first_name,
        ci.cst_lastname AS last_name,
        la.cntry AS country,
        ci.cst_marital_status AS marital_status,
        CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is Master table for Gender Info
        ELSE COALESCE(ca.gen, 'n/a') -- Fallback to ERP data
        END AS gender,
        ca.bdate AS birthdate,
        ci.cst_create_date AS create_date
    FROM silver.crm_cust_info AS ci
        LEFT JOIN silver.erp_cust_az12 AS ca
        ON ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 AS la
        ON ci.cst_key = la.cid;
GO


-- Dimension Products
CREATE VIEW gold.dim_products
AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key, -- This is Surrogate Key = Primary Key
        pi.prd_id AS product_id,
        pi.prd_key AS product_number,
        pi.prd_nm AS product_name,
        pi.cat_id AS category_id,
        pc.cat AS category,
        pc.subcat AS subcategory,
        pc.maintenance,
        pi.prd_cost AS cost,
        pi.prd_line AS product_line,
        pi.prd_start_dt AS start_date
    FROM silver.crm_prd_info AS pi
        LEFT JOIN silver.erp_px_cat_g1v2 AS pc
        ON pi.cat_id = pc.id
    WHERE pi.prd_end_dt IS NULL;
-- Filter out all historical data
GO


-- Fact Sales
CREATE VIEW gold.fact_sales
AS
    SELECT
        sd.sls_ord_num AS order_number,
        pr.product_key, -- Surrogate Key from gold.dim_products
        cu.customer_key, -- Surrogate Key from gold.dim_customers 
        sd.sls_order_dt AS order_date,
        sd.sls_ship_dt AS shipping_date,
        sd.sls_due_dt AS due_date,
        sd.sls_sales AS sales_amount,
        sd.sls_quantity AS quantity,
        sd.sls_price AS price
    FROM silver.crm_sales_details AS sd
        LEFT JOIN gold.dim_customers AS cu
        ON sd.sls_cust_id = cu.customer_id
        LEFT JOIN gold.dim_products AS pr
        ON sd.sls_prd_key = pr.product_number;
GO

SELECT DISTINCT category
from gold.dim_products;
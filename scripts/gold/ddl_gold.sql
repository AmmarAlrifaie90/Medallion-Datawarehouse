--========================================================
-- CREATE VIEW gold.dim_customers
--========================================================
-- Purpose:
-- This view creates the Customer Dimension table in the Gold layer.
-- It assigns surrogate keys (customer_key) and provides descriptive
-- attributes about customers (name, marital status, gender, etc.)
-- that will be used for analytics and reporting.
-- Notes:
-- - Uses ROW_NUMBER() to generate a surrogate key.
-- - Gender is derived from ERP data (GEN) with a fallback to 'n/a'.
-- - Joins CRM data with ERP Customer and ERP Location tables to enrich
--   customer attributes (DOB, country).
--========================================================
if OBJECT_ID('gold.dim_customers', 'V') is not null
    DROP VIEW gold.dim_customers;
GO

create view gold.dim_customers AS 
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,  -- Surrogate key
    cst_id AS customer_id,                                -- Natural key
    cst_key AS customer_number,
    cst_firstname AS customer_firstname,
    cst_lastname AS customer_lastname,
    cst_marital_status AS customer_marital_status,
    CASE WHEN cst_gndr = 'n/a' then cst_gndr
         else isnull(ec.GEN, 'n/a') 
    end customer_gender,                                  -- Gender handling
    cst_create_date AS customer_create_date,
    ec.BDATE AS customer_date_of_birth,
    el.CNTRY AS customer_country
FROM
    silver.crm_cust_info ci
JOIN silver.erp_CUST_AZ12 ec
    ON ci.cst_key = ec.CID
JOIN silver.erp_LOC_A101 el
    ON ci.cst_key = el.CID;



--========================================================
-- CREATE VIEW gold.dim_products
--========================================================
-- Purpose:
-- This view creates the Product Dimension table in the Gold layer.
-- It assigns surrogate keys (product_key) and provides product attributes
-- such as name, category, subcategory, cost, and product lifecycle dates.
-- Notes:
-- - Uses ROW_NUMBER() to generate surrogate key.
-- - Joins with ERP Category table for category/subcategory descriptions.
-- - Filters out historical products (prd_end_dt IS NOT NULL) to only
--   include currently active products.
--========================================================
if OBJECT_ID('gold.dim_products', 'V') is not null
    DROP VIEW gold.dim_products;
GO

create view gold.dim_products as
select
    row_number() over (order by prd_start_dt, prd_id) as product_key, -- Surrogate key
    prd_id as product_id,                                             -- Natural key
    prd_key as product_number,
    prd_nm as product_name,
    cat_id as category_id,
    epc.CAT as category,
    epc.SUBCAT as sub_category,
    prd_cost as cost,
    prd_line as line,
    prd_start_dt as product_start_date,
    prd_end_dt as product_end_date,
    epc.MAINTENANCE
from silver.crm_prd_info cpi
left join silver.erp_PX_CAT_G1V2 epc
    on cpi.cat_id = epc.ID
where cpi.prd_end_dt is null -- Exclude inactive/historical products



--========================================================
-- CREATE VIEW gold.fact_sales
--========================================================
-- Purpose:
-- This view creates the Sales Fact table in the Gold layer.
-- It contains transaction-level sales data, linking to the
-- Product and Customer dimensions for a star schema model.
-- Notes:
-- - Foreign keys (product_key, customer_key) come from dimension tables.
-- - Provides measures such as sales_amount, sales_quantity, and sales_price.
-- - Dates (order, ship, due) are preserved for time-based analysis.
--========================================================
if OBJECT_ID('gold.fact_sales', 'V') is not null
    DROP VIEW gold.fact_sales;
GO

create view gold.fact_sales as 
select
    sls_ord_num as sales_order_number,     -- Transaction identifier
    dp.product_key as product_key,         -- FK to dim_products
    dc.customer_key as customer_key,       -- FK to dim_customers
    sls_order_dt as sales_order_date,
    sls_ship_dt as sales_ship_date,
    sls_due_dt as sales_due_date,
    sls_sales as sales_amount,             -- Fact measure
    sls_quantity as sales_quantity,        -- Fact measure
    sls_price as sales_price               -- Fact measure
from silver.crm_sales_details f
left join gold.dim_customers dc  
    on f.sls_cust_id = dc.customer_id
left join gold.dim_products dp 
    on f.sls_prd_key = dp.product_number;

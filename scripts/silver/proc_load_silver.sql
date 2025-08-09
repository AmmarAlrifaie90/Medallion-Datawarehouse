/*
================================================================================
Procedure Name: silver.load_silver
================================================================================
Purpose:
    Loads and transforms data from the bronze layer into the silver layer tables.
    This procedure performs the following for each target table:
        1. Truncates the existing data.
        2. Applies necessary data cleaning, formatting, and validation.
        3. Inserts the transformed records into the silver schema.
        4. Logs execution times for performance tracking.

Details:
    - Handles nulls, blanks, and invalid formats with default values.
    - Standardizes text fields with TRIM/UPPER.
    - Uses ROW_NUMBER for deduplication where applicable.
    - Includes error handling to capture and display error number, message, and state.
================================================================================
*/

CREATE OR ALTER procedure silver.load_silver AS 
begin
    declare @start_time as datetime, @end_time as datetime, @batch_start_time AS DATETIME , @batch_end_time AS DATETIME
set @start_time  = GETDATE()
set @batch_start_time = GETDATE()
begin try
print '>> truncating silver.crm_cust_info'
truncate table silver.crm_cust_info
print '>> inserting to crm_cust_info'
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)

SELECT cst_id
    ,trim(cst_key)
    ,trim(cst_firstname) AS cst_firstname
    ,trim(cst_lastname) AS cst_lastname
    ,case when cst_marital_status IS NULL OR TRIM(cst_marital_status) = '' then 'n/a'
    else Upper(trim(cst_marital_status)) END cst_marital_status

    ,case when cst_gndr IS NULL then 'n/a'
    else Upper(trim(cst_gndr)) END cst_gndr
    ,cst_create_date
FROM (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) AS last_updated
    FROM bronze.crm_cust_info
) t 
WHERE last_updated = 1 AND cst_id IS NOT NULL;
print '====================================='
SET @end_time = GETDATE()
PRINT 'silver.crm_cust_info has done loading in ' + cast( datediff(second, @start_time, @end_time) as varchar) + 's'
print '====================================='
set @start_time  = GETDATE()
print '>> truncating silver.crm_prd_info'
truncate table silver.crm_prd_info
print '>> inserting to crm_prd_info'
insert into silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)

select
	prd_id,
	replace(substring(prd_key,1,5),'-','_') as cat_id,
	substring(prd_key,7,len(prd_key)) as prd_key,
	prd_nm,
	isnull(prd_cost, 0) as prd_cost,
	case when trim(prd_line) = '' or prd_line is null then 'n/a'
	else upper(trim(prd_line))
	end prd_line,
	cast(prd_start_dt as date) prd_start_dt,
	cast(lead(prd_start_dt) over( partition by prd_key order by prd_start_dt) - 1 as date)   prd_end_dt

from bronze.crm_prd_info
print '====================================='
SET @end_time = GETDATE()
PRINT 'silver.crm_prd_info has done loading in ' + cast( datediff(second, @start_time, @end_time) as varchar) + 's'
print '====================================='
set @start_time  = GETDATE()
print '>> truncating silver.crm_sales_details'
truncate table silver.crm_sales_details
print '>> inserting to crm_sales_details'
insert into silver.crm_sales_details(
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

SELECT sls_ord_num
      ,sls_prd_key
      ,sls_cust_id
      ,case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
      else cast(cast(sls_order_dt as varchar) as  date) end sls_order_dt,
      case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
      else cast(cast(sls_ship_dt as varchar) as  date) end sls_ship_dt,
      case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
      else cast(cast(sls_due_dt as varchar) as  date) end sls_due_dt
      ,case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
      then  sls_quantity * abs(sls_price)
      else sls_sales
      end sls_sales
      ,sls_quantity
      ,case when sls_price is null or sls_price <= 0 then 
      sls_sales / nullif(sls_quantity, 0)
      else sls_price
      end sls_price
  FROM bronze.crm_sales_details
  print '====================================='
SET @end_time = GETDATE()
PRINT 'silver.crm_sales_details has done loading in ' + cast( datediff(second, @start_time, @end_time) as varchar) + 's'
print '====================================='
set @start_time  = GETDATE()
print '>> truncating silver.erp_CUST_AZ12'
truncate table silver.erp_CUST_AZ12
print '>> inserting to erp_CUST_AZ12'
INSERT INTO silver.erp_CUST_AZ12 (
    CID,
    BDATE,
    GEN
)

SELECT  
    CASE 
        WHEN CID LIKE 'NAS%' THEN TRIM(SUBSTRING(CID, 4, LEN(CID)))
        ELSE CID 
    END AS CID,
    case 
        when BDATE > CAST(GETDATE() AS DATE) THEN null
        else BDATE
    END AS BDATE,
    CASE
        WHEN UPPER(TRIM(GEN))  = 'FEMALE' THEN 'F'
        WHEN UPPER(TRIM(GEN)) = 'MALE' THEN 'M'
        ELSE 'n/a'
    end as GEN
FROM bronze.erp_CUST_AZ12 
print '====================================='
SET @end_time = GETDATE()
PRINT 'silver.erp_CUST_AZ12 has done loading in ' + cast( datediff(second, @start_time, @end_time) as varchar) + 's'
print '====================================='
set @start_time  = GETDATE()
print '>> truncating silver.erp_LOC_A101'
truncate table silver.erp_LOC_A101
print '>> inserting to erp_LOC_A101'
insert into silver.erp_LOC_A101 (
	CID,
	CNTRY
)

SELECT DISTINCT
	trim(REPLACE(CID,'-','')),
	CASE WHEN UPPER(TRIM(CNTRY)) = 'DE'	 THEN 'Germany'
	WHEN UPPER(TRIM(CNTRY)) = 'USA' OR UPPER(TRIM(CNTRY)) = 'US' THEN 'United States'
	WHEN UPPER(TRIM(CNTRY)) = '' OR UPPER(TRIM(CNTRY)) IS NULL THEN 'n/a'
	ELSE CNTRY
	END AS CNTRY
FROM bronze.erp_LOC_A101
print '====================================='
SET @end_time = GETDATE()
PRINT 'silver.erp_LOC_A101 has done loading in ' + cast( datediff(second, @start_time, @end_time) as varchar) + 's'
print '====================================='
set @start_time  = GETDATE()
print '>> truncating silver.erp_PX_CAT_G1V2'
truncate table silver.erp_PX_CAT_G1V2
print '>> inserting to erp_PX_CAT_G1V2'
INSERT INTO silver.erp_PX_CAT_G1V2 (
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
)

SELECT  
    trim(ID) as ID
      ,trim(CAT) as CAT
      ,trim(SUBCAT) AS SUBCAT
      ,trim(MAINTENANCE) AS MAINTENANCE
  FROM [bronze].[erp_PX_CAT_G1V2]
  where ID  in (select cat_id from silver.crm_prd_info)
  print '====================================='
SET @end_time = GETDATE()
set @batch_end_time = GETDATE()
PRINT 'silver.erp_PX_CAT_G1V2 has done loading in ' + cast( datediff(second, @start_time, @end_time) as varchar) + 's'
print '====================================='
PRINT 'silver layer has done loading in ' + cast( datediff(second, @batch_start_time, @batch_end_time) as varchar) + 's'
print '====================================='
print 'The end of the silver layer'
END TRY
	BEGIN CATCH
	PRINT '================================='
	PRINT 'ERROR WHILE LOADING SILVER LAYER'
	PRINT 'ERROR NUMBER ' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE ' + ERROR_MESSAGE();
	PRINT 'ERROR STAT ' + CAST(ERROR_STATE() AS NVARCHAR); 
	PRINT '================================='
end catch
END

/*
=====================================================================================
Stored Procedure: bronze.load_bronze
=====================================================================================

Purpose:
This stored procedure loads data from source CSV files into the Bronze layer tables 
(under the 'bronze' schema). It performs the following actions:

- Truncates existing data from the target Bronze tables
- Loads fresh data from source CSV files using BULK INSERT
- Calculates and prints the load duration for each table
- Handles errors gracefully and provides error details if any step fails

Usage:
Executes as a single batch to load all Bronze tables with one command.

*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time as datetime, @end_time as datetime, @batch_start_time as datetime, @batch_end_time as datetime;
		set @batch_start_time = GETDATE();
BEGIN TRY
		PRINT '========================='
		PRINT 'LOADING THE BRONZE LAYER'
		PRINT '========================='

		PRINT '-------------------------'
		PRINT 'LOADING CRM TABLES'
		PRINT '-------------------------'

		PRINT '>>TRUNCATING bronze.crm_cust_info'

		set @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>>INSERTING bronze.crm_cust_info'

		BULK INSERT bronze.crm_cust_info

		from 'C:\Users\mramm\Desktop\Data Enginner\WarehouseProject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as varchar ) + 'seconds';

		PRINT '>>TRUNCATING bronze.crm_prd_inf'

		set @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>>INSERTING bronze.crm_prd_inf'

		BULK INSERT bronze.crm_prd_info
		from 'C:\Users\mramm\Desktop\Data Enginner\WarehouseProject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as varchar ) + 'seconds';

		PRINT '>>TRUNCATING bronze.crm_sales_details'

		set @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>INSERTING bronze.crm_sales_details'

		BULK INSERT bronze.crm_sales_details
		from 'C:\Users\mramm\Desktop\Data Enginner\WarehouseProject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as varchar ) + 'seconds';

			PRINT '-------------------------'
			PRINT 'LOADING ERP TABLES'
			PRINT '-------------------------'

			PRINT '>>TRUNCATING bronze.erp_CUST_AZ12'
		set @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_CUST_AZ12;

		PRINT '>>INSERTING bronze.erp_CUST_AZ12'

		BULK INSERT bronze.erp_CUST_AZ12

		from 'C:\Users\mramm\Desktop\Data Enginner\WarehouseProject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as varchar ) + 'seconds';

		PRINT '>>TRUNCATING bronze.erp_LOC_A101'

		set @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_LOC_A101;

		PRINT '>>INSERTING bronze.erp_LOC_A101'

		BULK INSERT bronze.erp_LOC_A101
		from 'C:\Users\mramm\Desktop\Data Enginner\WarehouseProject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as varchar ) + 'seconds';

		PRINT '>>TRUNCATING bronze.erp_PX_CAT_G1V2'

		set @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

		PRINT '>>INSERTING bronze.erp_PX_CAT_G1V2'

		BULK INSERT bronze.erp_PX_CAT_G1V2
		from 'C:\Users\mramm\Desktop\Data Enginner\WarehouseProject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			TABLOCK
		);
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as varchar ) + 'seconds';

		print'============================================================='

		set @batch_end_time = GETDATE();
		print '>>Batch Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as varchar ) + 'seconds';
		print'============================================================='

	END TRY
	BEGIN CATCH
	PRINT '================================='
	PRINT 'ERROR WHILE LOADING BRONZE LAYER'
	PRINT 'ERROR NUMBER ' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE ' + ERROR_MESSAGE();
	PRINT 'ERROR STAT ' + CAST(ERROR_STATE() AS NVARCHAR); 
	PRINT '================================='
	END CATCH
END

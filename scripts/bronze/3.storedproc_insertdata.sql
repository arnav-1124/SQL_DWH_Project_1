-- Creating a Stored procedure to run this query every time to get fresh data.
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY -- error catching
		SET @batch_start_time = GETDATE();

		PRINT '=====================================';
		PRINT ' LOADING BRONZE LAYER';
		PRINT '=====================================';

		PRINT '-------------------------------------';
		PRINT ' Loading CRM Tables';
		PRINT '-------------------------------------';


		SET @start_time = GETDATE();
		-- 1.Truncating and then Bulk inserting the data into table 'bronze.crm_cust_info'
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'F:\SQL\DATA With Baraa\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '----------------------------------'



		SET @start_time = GETDATE();
		-- 2.Truncating and then Bulk inserting the data into table 'bronze.crm_prd_info'
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'F:\SQL\DATA With Baraa\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '----------------------------------'



		SET @start_time = GETDATE();
		-- 3.Truncating and then Bulk inserting the data into table 'bronze.crm_sales_details'
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'F:\SQL\DATA With Baraa\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '----------------------------------'




		PRINT '-------------------------------------';
		PRINT ' Loading ERP Tables';
		PRINT '-------------------------------------';


		SET @start_time = GETDATE();
		-- 4.Truncating and then Bulk inserting the data into table 'bronze.erp_cust_az12'
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'F:\SQL\DATA With Baraa\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '----------------------------------'



		SET @start_time = GETDATE();
		-- 5.Truncating and then Bulk inserting the data into table 'bronze.erp_loc_a101'
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'F:\SQL\DATA With Baraa\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '----------------------------------'



		SET @start_time = GETDATE();
		-- 6.Truncating and then Bulk inserting the data into table 'bronze.erp_px_cat_g1v2'
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'F:\SQL\DATA With Baraa\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '----------------------------------'



		SET @batch_end_time = GETDATE();
		PRINT '========================================';
		PRINT ' LOADING BRONZE LAYER IS COMPLETED ';
		PRINT '		- Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.';
		PRINT '========================================';

	END TRY
	BEGIN CATCH		-- error handling
		PRINT '===========================================';
		PRINT ' ERROR OCCURED DURING LOADING BRONZE LAYER ';
		PRINT ' Error Message: ' + ERROR_MESSAGE();
		PRINT ' Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT ' Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR);
		PRINT ' Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================';
	END CATCH;
END;

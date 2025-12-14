-- Creating a sub-procedure to truncate and insert all 6 table with 1 execution:
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		
		SET @batch_start_time = GETDATE();

		PRINT '=====================================';
		PRINT ' LOADING SILVER LAYER';
		PRINT '=====================================';

		PRINT '-------------------------------------';
		PRINT ' Loading CRM Tables';
		PRINT '-------------------------------------';


		SET @start_time = GETDATE()
		-- 1.TRANSFORMING & INSERTING INTO --> crm_cust_info:
		PRINT '>> Truncating Data from: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>> Inserting Data into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr, 
			cst_create_date)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,	-- removing unwanted spaces in names
			TRIM(cst_lastname) AS cst_lastname,

			CASE
				WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
				WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'		
			END AS 	cst_marital_status, -- normalize marital status and make it readable

			CASE
				WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
				WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr,   -- normalize marital status and make it readable
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_latest
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) AS t
		WHERE flag_latest = 1;		-- selecting only the most recent record per customer
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '----------------------------------'



		SET @start_time = GETDATE();
		-- 2.TRANSFORMING & INSERTING INTO --> crm_prd_info:
		PRINT '>> Truncating Data from: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_line,
			prd_cost,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,	-- extracting category id
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,			-- extracting product key
			prd_nm,
			CASE UPPER(TRIM(prd_line))		-- simplifying and making the column more readable 
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			COALESCE(prd_cost, 0) AS prd_cost,		-- converting the NUll cost to 0
			CAST(prd_start_dt AS DATE) AS prd_start_dt,		-- ensuring both start and end date is real date and end date must be higher than start date
			CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC))AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '----------------------------------'





		SET @start_time = GETDATE();
		-- 3.TRANSFORMING & INSERTING INTO --> crm_sales_details:
		PRINT '>> Truncating Data from: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
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
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,	--handling invalid dates and converting the integer into date

			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,

			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,

			CASE
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> (sls_quantity * ABS(sls_price)) 
					THEN (sls_quantity * ABS(sls_price))
				ELSE sls_sales
			END AS sls_sales,		--handling invalid sales value and recalculating by using price and qty
			sls_quantity,
			CASE
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN (sls_sales / NULLIF(sls_quantity, 0))
				ELSE sls_price
			END AS sls_price	-- deriving price if orginial one was invalid (like null or 0).
		FROM bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '----------------------------------'


		PRINT '-------------------------------------';
		PRINT ' Loading ERP Tables';
		PRINT '-------------------------------------';


		SET @start_time = GETDATE();
		-- 4.TRANSFORMING & INSERTING INTO --> erp_cust_az12:
		PRINT '>> Truncating Data from: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT 
			CASE WHEN cid LIKE 'NAS%'THEN SUBSTRING(cid, 4, len(cid))
				 ELSE cid
			END AS cid,		-- transforming customer id to connect with other table from 'crm'
			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
			END AS bdate,	-- handling invalid birth_dates
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END as gen		--handling missing & abbreviated data and make it more readable
		FROM bronze.erp_cust_az12;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '----------------------------------'




		SET @start_time = GETDATE();
		-- 5.TRANSFORMING & INSERTING INTO --> erp_loc_a101:
		PRINT '>> Truncating Data from: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT
			REPLACE(cid,'-','') AS cid,  -- transforming cid to match this col to other table from 'crm'
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
				WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'n/a'
			ELSE TRIM(cntry)	-- handling blanks, nulls and abbreviations in country column and making it more readable
			END AS cntry
		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '----------------------------------'




		SET @start_time = GETDATE();
		-- 6.TRANSFORMING & INSERTING INTO --> erp_px_cat_g1v2:
		PRINT '>> Truncating Data from: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT 
			id,
			TRIM(cat) AS cat,
			TRIM(subcat) AS subcat,
			TRIM(maintenance) AS maintenance
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '----------------------------------'



		SET @batch_end_time = GETDATE();
		PRINT '========================================';
		PRINT ' LOADING SILVER LAYER IS COMPLETED ';
		PRINT '		- Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.';
		PRINT '========================================';



	END TRY
	BEGIN CATCH -- error handling
		PRINT '===========================================';
		PRINT ' ERROR OCCURED DURING LOADING SILVER LAYER ';
		PRINT ' Error Message: ' + ERROR_MESSAGE();
		PRINT ' Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT ' Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR);
		PRINT ' Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================';
	END CATCH;

END;
-- Creating Virtual Table for Customer Information:
CREATE VIEW gold.dim_customers AS 
	(SELECT
		ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		ca.bdate AS birth_date,
		el.cntry AS country,
		ci.cst_marital_status AS marital_status,
		CASE WHEN cst_gndr = 'n/a' THEN COALESCE(ca.gen, 'n/a')  -- crm is the master table for gender info
			 ELSE cst_gndr
		END AS gender,
		ci.cst_create_date AS create_date
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 el
	ON ci.cst_key = el.cid
);




-- Creating Virtual Table for Product Information:
CREATE VIEW gold.dim_product AS
	(SELECT 
		ROW_NUMBER() OVER(ORDER BY pri.prd_start_dt, pri.prd_key) AS product_key,
		pri.prd_id AS product_id,
		pri.prd_key AS product_number,
		pri.prd_nm AS product_name,
		pri.cat_id AS category_id,
		pc.cat AS category,
		pc.subcat AS subcategory,
		pc.maintenance,
		pri.prd_cost AS cost,
		pri.prd_line AS product_line,
		pri.prd_start_dt AS product_start_date
	FROM silver.crm_prd_info AS pri
	LEFT JOIN silver.erp_px_cat_g1v2 AS pc
	ON pri.cat_id = pc.id
	WHERE pri.prd_end_dt IS NULL
);	-- filter out historized product data and take info of only currently available product
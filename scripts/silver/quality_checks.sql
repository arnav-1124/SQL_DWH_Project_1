-- Quality Check of the silver layer


--1.Check for Nulls or Duplicates in Primary key
-- Expectation: No Result
SELECT 
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;


--2.Check for unwanted spaces
-- Expectation: No Result
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);


--3.Data standardization and Consistency
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;


--4. Checking Nulls or duplicates in prd_id
SELECT 
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


-- 5.check for unwanted spaces
-- Expectation: No Result
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm)

-- 6.Checking for nulls and negative numbers in prd_cost
-- Expectation: No Result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost <= 0

-- 7.CHECK for column prd_line as it has low cardinality
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- 8.Checking for start data must be earlier than end date
-- Expectation: No result
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt <= prd_start_dt

-- 9.Check for invalid order date
-- Expectation: No Result
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- 10.Check for price*qty = sales
-- any of these 3 cols cannot be zero, null or negative in sales details table
-- Expectation: NULL
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE 
	(sls_price * sls_quantity) != sls_sales
	OR sls_price IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL
	OR sls_price <= 0 OR sls_quantity <= 0 OR sls_sales <= 0;


-- 11.Data standardization and consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12

-- 12.Invalid birthDates
SELECT *
FROM silver.erp_cust_az12
WHERE bdate > GETDATE();










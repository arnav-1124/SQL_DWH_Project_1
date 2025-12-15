-- Checking gold layer's data standardization and consistency
-- Expectation: UNIQUE ROWS
SELECT DISTINCT gender
FROM gold.dim_customers;

SELECT DISTINCT marital_status
FROM gold.dim_customers;

-- Total number of rows:
SELECT * FROM gold.dim_product

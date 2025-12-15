-- Checking gold layer's data standardization and consistency
-- Expectation: UNIQUE ROWS
SELECT DISTINCT gender
FROM gold.dim_customers;

SELECT DISTINCT marital_status
FROM gold.dim_customers;

-- Total number of rows:
SELECT * FROM gold.dim_product

-- Foreign keys integrity check:
-- Expectation: No Result
SELECT * 
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key
LEFT JOIN gold.dim_product p
ON s.product_key = p.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL;

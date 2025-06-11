/*  ####################################################################################*/

-- bronze.crm_cust_info
-- Analysis
SELECT *
FROM bronze.crm_cust_info


-- Trim spaces before and after `cst_firstname` and `cst_lastname` to maintain consistency.
-- Ensure `cst_marital_status` and `cst_gndr` are standardized to either uppercase or lowercase for uniformity.
-- Also write full form for `cst_marital_status` and `cst_gndr`  for better under standing
-- Handle NULL values in primary key (`cst_id`) columns to prevent integrity issues.

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id IS NULL

SELECT *
FROM bronze.crm_cust_info
WHERE cst_key IS NULL

SELECT cst_id
	,count(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1
	OR count(*) IS NULL

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29449 -- same key  , gender null , first name null

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29473 -- same key  , gender null , last name null , maritial status null

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29433 -- same key  ,  first name and last name null

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29483 -- same key  , gender null , first name null , maritial status null

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466 -- same key  , gender null , first name and last name null , maritial status null

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id IS NULL -- all npk is null and id is also null. 

-- Observations:
-- 1. Gender and marital status should be standardized using full-form values.
-- 2. First name and last name should be trimmed to remove extra spaces and standardized.
-- 3. Latest record (based on `cst_create_date`) appears to have correct values, which can be used to update missing entries.
-- 4. However, there is a risk—older records might be correct, while the latest record might be incorrect.
-- 5. If we blindly use the latest record for corrections, we may overwrite accurate data with incorrect values.

-- Solution Approach:
-- 1. Instead of ranking records by `cst_create_date` using `PARTITION BY`, 
--		update each column based on the primary key (`cst_id`), 
--		ensuring that missing values are replaced with the maximum non-null value.
-- 2. Fill missing values using the most frequent correct entries instead of assuming the latest record is always right.
-- 3. Implement proper validation to ensure incorrect values are not propagated.
-- 4. Allow overrides only when missing data exists instead of forcing a blanket update.


DROP TABLE

IF EXISTS ##crm_cust_info
	SELECT *
	INTO ##crm_cust_info
	FROM bronze.crm_cust_info

UPDATE ##crm_cust_info
SET cst_firstname = COALESCE(cst_firstname, t.max_firstname)
	,cst_lastname = COALESCE(cst_lastname, t.max_lastname)
	,cst_marital_status = COALESCE(cst_marital_status, t.max_marital_status)
	,cst_gndr = COALESCE(cst_gndr, t.max_gndr)
FROM (
	SELECT cst_id
		,MAX(cst_firstname) AS max_firstname
		,MAX(cst_lastname) AS max_lastname
		,MAX(cst_marital_status) AS max_marital_status
		,MAX(cst_gndr) AS max_gndr
	FROM bronze.crm_cust_info
	GROUP BY cst_id
	) t
WHERE ##crm_cust_info.cst_id = t.cst_id;

SELECT *
FROM ##crm_cust_info
WHERE cst_id = 29449 -- same key  , gender null , first name null

SELECT *
FROM ##crm_cust_info
WHERE cst_id = 29473 -- same key  , gender null , last name null , maritial status null

SELECT *
FROM ##crm_cust_info
WHERE cst_id = 29433 -- same key  ,  first name and last name null

SELECT *
FROM ##crm_cust_info
WHERE cst_id = 29483 -- same key  , gender null , first name null , maritial status null

SELECT *
FROM ##crm_cust_info
WHERE cst_id = 29466 -- same key  , gender null , first name and last name null , maritial status null
	--other approach could be  , but this will not be 100% right
	;

WITH cte
AS (
	SELECT *
		,ROW_NUMBER() OVER (
			PARTITION BY cst_id ORDER BY cst_create_date DESC
			) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)
SELECT *
FROM cte
WHERE flag_last = 1;

/*  ####################################################################################*/
SELECT *
FROM bronze.crm_prd_info

SELECT *
FROM bronze.crm_prd_info
WHERE prd_key IN (
		SELECT prd_key
		FROM bronze.crm_prd_info
		GROUP BY prd_key
		HAVING COUNT(*) > 1
		);

-- Observations:
-- 1. The `prd_key` can be split into two separate components for better organization.
-- 2. The ERP table does not store `prd_key`, but instead uses `category`, so adjustments may be needed.
-- 3. If `prd_cost` is NULL, it should be set to 0 to ensure data consistency.
-- 4. The `prd_line` values need to be standardized for uniformity.
-- 5. If `prd_end_date` is NULL in middle records, it should be corrected based on logical business rules.
SELECT *
FROM bronze.crm_prd_info
WHERE prd_key = 'AC-HE-HL-U509-R'

SELECT *
FROM bronze.crm_prd_info
WHERE prd_id = 212

/*  ####################################################################################*/
SELECT sls_ord_num
	,sls_prd_key
	,count(*)
FROM bronze.crm_sales_details
GROUP BY sls_ord_num
	,sls_prd_key
HAVING count(*) > 1

SELECT *
FROM bronze.crm_sales_details

SELECT *
FROM bronze.crm_sales_details
WHERE 1 = 1
	AND (
		sls_sales <> sls_quantity * sls_price
		OR sls_sales IS NULL
		OR sls_price IS NULL
		OR sls_quantity IS NULL
		OR sls_sales < 0
		OR sls_price < 0
		OR sls_quantity < 0
		)
-- Observations:
-- Fix date formatting issues by ensuring values are correctly converted to a proper date type.
-- Ensure price, sales, and quantity calculations are consistent and aligned.
-- Validate that price, sales, and quantity values are not negative or NULL to maintain data integrity.
/*  ####################################################################################*/
SELECT *
FROM bronze.erp_cust_az12

SELECT *
FROM bronze.erp_cust_az12
WHERE cid IS NULL -- none

-- Standardize gender values to maintain consistency across records.
-- Validate date fields to ensure correctness and proper formatting.
-- Remove occurrences of 'NAS' from `cid` to maintain data integrity.

/*  ####################################################################################*/

SELECT *
FROM bronze.erp_loc_a101

-- Format `cid` values to ensure consistency in structure.
-- Remove hyphens (`-`) from `cid` to maintain uniform formatting.
-- Standardize `cntry` values to ensure consistency across records.

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101

/*  ####################################################################################*/
SELECT *
FROM bronze.erp_px_cat_g1v2
	--- no changes required

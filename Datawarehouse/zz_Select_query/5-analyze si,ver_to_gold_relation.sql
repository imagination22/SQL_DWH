-- Count total records from each table in the silver layer
Select count(*) from silver.crm_cust_info  -- Customer info table
Select count(*) from silver.crm_prd_info  -- Product info table
Select count(*) from silver.crm_sales_details  -- Sales details table
Select count(*) from silver.erp_cust_az12  -- ERP customer-related data
Select count(*) from silver.erp_loc_a101  -- Location details from ERP
Select count(*) from silver.erp_px_cat_g1v2  -- Product category and metadata

-- Retrieve all records from each silver-layer table for analysis
Select * from silver.crm_cust_info  
Select * from silver.crm_prd_info  
Select * from silver.crm_sales_details  
Select * from silver.erp_cust_az12  
Select * from silver.erp_loc_a101  
Select * from silver.erp_px_cat_g1v2  

-- Customer data processing:
-- We need to combine erp_loc_a101, erp_cust_az12  , and crm_cust_info.

-- Sales data processing:

-- Product information processing:
-- We can enhance product info by adding category details.

-- Implementing a Star Schema:
-- Dimensions:
-- dim_customer  -- Contains standardized customer details.
-- dim_product  -- Contains product details along with categories.
-- Fact Table:
-- fact_sales  -- Centralized sales data connected to customers and products.

-- Master table joins with child tables for data enrichment
Select top 3 * from silver.crm_cust_info  -- Master table containing all customer records
Select top 3 * from silver.erp_cust_az12  -- Contains gender and birth date information
Select top 3 * from silver.erp_loc_a101  -- Location details such as country

Select top 3 * from silver.crm_prd_info  -- Master product table
Select top 3 * from silver.erp_px_cat_g1v2  -- Contains category, subcategory, and maintenance details

-- Analyzing data correctness:
Select cci.*, ela.cntry, eca.gen, eca.bdate 
from silver.crm_cust_info as cci  -- Customer master table
left join silver.erp_cust_az12 as eca 
on cci.cst_key = eca.cid  -- Join to retrieve gender and birth date
left join silver.erp_loc_a101 as ela 
on cci.cst_key = ela.cid  -- Join to retrieve country information

-- Query execution time: 1 min 35 sec

-- Observation: 
-- Gender exists in both `crm_cust_info` and `erp_cust_az12`, causing ambiguity.

-- Identifying inconsistent gender values:
Select cci.cst_gndr, eca.gen, eca.bdate  
from silver.crm_cust_info as cci  -- Master table
left join silver.erp_cust_az12 as eca 
on cci.cst_key = eca.cid  
where cci.cst_gndr <> eca.gen  -- Find mismatched gender values

-- Decision:
-- If `cst_gndr` (master table) is not null, it is considered the correct value.

-- Enriching product information:
Select cpi.*, epcg.cat, epcg.subcat, epcg.maintenance  
from silver.crm_prd_info as cpi  -- Master product table
left join silver.erp_px_cat_g1v2 as epcg 
on cpi.cat_id = epcg.id  -- Join to fetch category details

-- Optimize data by removing historical records from views.

-- Sales data processing:
-- Using `dim_product`, `dim_customer`, and the silver-layer sales table,
-- we can construct the `fact_sales` table for accurate reporting.
--execute bronze.load_bronze

Select count(*) from bronze.crm_cust_info
Select count(*) from bronze.crm_prd_info
Select count(*) from bronze.crm_sales_details
Select count(*) from bronze.erp_cust_az12
Select count(*) from bronze.erp_loc_a101
Select count(*) from bronze.erp_px_cat_g1v2

-- Analyzing
Select * from bronze.crm_cust_info -- 18493

Select cst_firstname, cst_lastname, cst_gndr , count(*) as cnt
from bronze.crm_cust_info
group by  cst_firstname, cst_lastname, cst_gndr 
having  count(*)  >1 --44

Select cst_firstname, cst_lastname, cst_gndr , cst_marital_status ,count(*) as cnt
from bronze.crm_cust_info
group by  cst_firstname, cst_lastname, cst_gndr ,cst_marital_status
having  count(*)  >1 --16
/*
this stores customer info 
1. name , gender , maritial status
2. id , pk 
3. when the customer cretaed

cst_id -> auto increment
cst_key -> AW00 + cst_id   ( key column , PK)
cst_firstname   ->
cst_lastname
cst_marital_status
cst_gndr
cst_create_date

*/


-----------------------------------------------------------------------------------
Select *  from bronze.crm_prd_info
/*
product information 
prd_id			 ->
prd_key			 ->
prd_nm			 -> product name 
prd_cost		 ->	 cost of the product per unit
prd_line		 ->	 same prodcut line mean related product 
					NULL
					M 
					R 
					S 
					T 
prd_start_dt	 ->
prd_end_dt		 ->
*/
Select distinct prd_line from bronze.crm_prd_info


Select * from bronze.crm_sales_details

/*
Sales of product to customer
Event nd transactional table
sls_ord_num		 -> ORDER NUMBER , 1 order can have multiple product and 1 customer
sls_prd_key		 -> product 's key whcih got sold
sls_cust_id		 ->	id of customer who purches this product
sls_order_dt	 ->	date when order was placed
sls_ship_dt		 ->	date when order was deliverd
sls_due_dt		 ->	date by whihc order should get deliverd
sls_sales		 ->	sales price per unit
sls_quantity	 ->	qunatity of unit sold
sls_price		 ->	sale price per unit * qty
*/

select sls_ord_num , count( distinct sls_prd_key) , count( distinct  sls_cust_id) from bronze.crm_sales_details
where 1=1
and sls_ord_num ='SO55367'
group by sls_ord_num  

---------------------------------------------------------------------------------------------------------------------
Select * from bronze.erp_cust_az12
/*
cid    ->	 NAS + AW00 + cst_id
bdate  ->	birthdate of the customer
gen	   ->	gender of the customer
*/
Select * from bronze.erp_loc_a101
/*
cid			 ->	AS + - +000 + cust_id
cntry		 ->	 country wher customer live
*/
Select * from bronze.erp_px_cat_g1v2
/*
id			 ->	
cat			 ->	category
subcat		 ->	sub category
maintenance	 ->	
*/


-- conculsion
Select * from bronze.crm_cust_info
Select * from bronze.crm_prd_info
Select * from bronze.crm_sales_details
/*
bronze.crm_cust_info  (cst_id)
+	 bronze.crm_prd_info (prd_key)
=	bronze.crm_sales_details  (cst_id,prd_key)


*/
Select *   from bronze.erp_cust_az12
Select * from bronze.erp_loc_a101
Select * from bronze.erp_px_cat_g1v2
/*
bronze.crm_cust_info  (cst_id) ~ bronze.erp_cust_az12 (c_id)
bronze.crm_cust_info  (cst_id) ~ bronze.erp_loc_a101 (c_id)
bronze.erp_px_cat_g1v2  
*/
Select * from bronze.crm_prd_info
Select * from bronze.erp_px_cat_g1v2
/*
bronze.erp_px_cat_g1v2 (id) ~ bronze.crm_prd_info (prd_id)
*/


/*
											->bronze.crm_cust_info  (cst_id)	|-> bronze.erp_cust_az12 (c_id)
ronze.crm_sales_details  (cst_id,prd_key) -|									|-> bronze.erp_loc_a101 (c_id)
										   |
											-> bronze.crm_prd_info (prd_key)    |-> bronze.crm_prd_info (prd_id)
*/
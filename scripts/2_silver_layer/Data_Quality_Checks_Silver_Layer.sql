--================================================================================--
--========= Data Quality Checks before ETL Operation to the Silver Layer =========--
--================================================================================--


--===================================================================================================================================--
--Checking for nulls or duplicate in primary key of bronze.crm_customer_info tables--
select 
	cst_id,
	COUNT(*) AS duplication
from bronze.crm_customer_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL
--We can use window (ROW_NUMBER()) function and subquesry to exculde the duplicate data before loading--


--Checking the unwanted Spaces in data with NVARCHAR type--
SELECT
	cst_firstname
FROM bronze.crm_customer_info
WHERE cst_firstname != TRIM(cst_firstname) -- Comparing the original firstname with trimed firstname(TRIM function eliminate spaces) --

SELECT
	cst_lastname
FROM bronze.crm_customer_info
WHERE cst_lastname != TRIM(cst_lastname) -- Comparing the original lastname with trimed lastname(TRIM function eliminate spaces) --
--We can use the TRIM function to clean this data fron unwanted spaces--


--===================================================================================================================================--
SELECT *
FROM bronze.crm_products_info

--Checking for nulls or duplicate in primary key of bronze.crm_products_info tables--
select 
	prd_id,
	COUNT(*) AS duplication
from bronze.crm_products_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

/*--Split the prd_key into to elements 'cat_id' for categories and subcat_id for subcategories to match it with bronze.erp_px_cat_g1v2 table
using SUBSTRING function--*/
SELECT
	prd_key,
	SUBSTRING(prd_key, 1, 5) AS cat_id
FROM bronze.crm_products_info
SELECT DISTINCT * from bronze.erp_px_cat_g1v2

SELECT prd_start_date, prd_end_date from bronze.crm_products_info

--Checking the unwanted Spaces in data with NVARCHAR type--
SELECT
	prd_name
FROM bronze.crm_products_info
WHERE prd_name != TRIM(prd_name)

--Checking nulls or negative values
SELECT
	prd_cost
FROM bronze.crm_products_info
WHERE prd_cost < 0 OR prd_cost IS NULL
--We can handle nulls using the ISNULL function to replace it with 0

--Check for invalid date orders
SELECT *
FROM bronze.crm_products_info
WHERE prd_end_date < prd_start_date
 

 --===================================================================================================================================--
 SELECT * FROM bronze.crm_sales_details
 --Checking the unwanted Spaces in data with NVARCHAR type--
SELECT
	sls_product_key
FROM bronze.crm_sales_details
WHERE sls_product_key != TRIM(sls_product_key)

 --Checking the data of the relationship between sales tables and products and customers table in silver schema--
 SELECT *
 FROM bronze.crm_sales_details
 WHERE sls_product_key NOT IN (
	SELECT 
		prd_key
	FROM silver.crm_products_info
	)

SELECT *
 FROM bronze.crm_sales_details
 WHERE sls_customer_id NOT IN (
	SELECT 
		cst_id
	FROM silver.crm_customer_info
	)


--Check for invalid date orders
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_date > sls_due_date OR sls_order_date > sls_ship_date

--Check for sales values
--Sales should be !=0, positive and not NULL--
--Sales = Quantity * Price --
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


 --===================================================================================================================================--
SELECT * FROM bronze.erp_cust_az12
WHERE cid NOT LIKE 'NAS%';

SELECT cst_key FROM bronze.crm_customer_info
--we should normalize the cid (cst_key in crm_customer_info table) and eliminate the 'NAS' letters--

--check strange date values--
SELECT * FROM bronze.erp_cust_az12
WHERE bdate >  GETDATE(); --bdate in futur!! not possible ==> to pass it NULL

--check gen data quality--
SELECT DISTINCT gen FROM bronze.erp_cust_az12


--===================================================================================================================================--
SELECT *
FROM bronze.erp_loc_a101
WHERE  cid NOT IN (select cst_key FROM bronze.crm_customer_info);
SELECT cst_key FROM bronze.crm_customer_info
--We should eliminate '-' from cid to normalize it with cst_key in crm_customer_info--

--check cntry data quality
SELECT DISTINCT cntry FROM bronze.erp_loc_a101


--===================================================================================================================================--
SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2
--WHERE cat != TRIM(cat)

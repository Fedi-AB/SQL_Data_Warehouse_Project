/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Fact Table:
      ** Stores quantitative data (metrics, measures) about business events, e.g., sales amount, quantity, revenue.
      ** Contains foreign keys linking to dimension tables.
      ** Usually large and updated frequently.

Dimension Table:
      ** Stores descriptive attributes (context) about facts, e.g., customer name, product category, region.
      ** Contains primary keys referenced by fact tables.
      ** Usually smaller and changes less often.
In short:
==> Fact = “What happened and how much”
==> Dimension = “Who, what, when, where, why”

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


/*
===============================================================================================================================================
----------------------------------------------------- Creation of gold.dim_customers VIEW -----------------------------------------------------
===============================================================================================================================================
*/
--Checks if we have a view with the same name (Please ensure that you have a backup before excuting this line)--
IF OBJECT_ID('gold.dim_customers', 'U') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

--View Creation--
CREATE VIEW gold.dim_customers AS

SELECT

	-- Generation of surrogate key (System-generated unique id assigned to each record in a table) --
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	-- Selecting the other columns we need it in the gold layer --
	SCI.cst_id AS customer_id,
	SCI.cst_key AS customer_number,
	SCI.cst_firstname AS first_name,
	SCI.cst_lastname AS last_name,
	SELA.cntry AS country,
	SCI.cst_marital_status AS marital_status,
	
	-- gender information --
	CASE
		WHEN SCI.cst_gndr != 'Unknown' THEN SCI.cst_gndr -- CRM is the primary source for gender --
		ELSE COALESCE(SECA.gen, 'Unknown') -- Return to the erp gender column to fill the gender information (we added COALESCE function to handle nulls to 'Unknown') --
	END AS customer_gender,

	SECA.bdate AS birthdate,
	SCI.cst_create_date AS create_date

-- Joining the crm_customer_info table with two erp tables to bring other customer information such us birthdate and country --
FROM silver.crm_customer_info AS SCI
LEFT JOIN silver.erp_cust_az12 AS SECA 
ON		SCI.cst_key = SECA.cid
LEFT JOIN silver.erp_loc_a101 AS SELA
ON		SCI.cst_key = SELA.cid

GO


/*
===============================================================================================================================================
------------------------------------------------------ Creation of gold.dim_products VIEW -----------------------------------------------------
===============================================================================================================================================
*/
--Checks if we have a view with the same name (Please ensure that you have a backup before excuting this line)--
IF OBJECT_ID('gold.dim_products', 'U') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

--View Creation--
CREATE VIEW gold.dim_products AS

SELECT

	-- Generation of surrogate key (System-generated unique id assigned to each record in a table) --
	ROW_NUMBER() OVER(ORDER BY SPI.prd_start_date, SPI.prd_key)AS product_key,
	-- Selecting the other columns we need it in the gold layer --
	SPI.prd_id AS product_id,
	SPI.prd_key AS product_number,
	SPI.prd_name AS product_name,
	SPI.cat_id AS category_id,
	SEPC.cat AS category,
	SEPC.subcat AS subcategory,
	SEPC.maintenance AS maintenance,
	SPI.prd_cost AS product_cost,
	SPI.prd_line AS product_line,
	SPI.prd_start_date AS product_start_date

-- Joining the crm_products_info table with erp_px_cat table to bring other products informations such us subcategory and maintenance --
FROM silver.crm_products_info AS SPI
LEFT JOIN silver.erp_px_cat_g1v2 AS SEPC
ON		SPI.cat_id = SEPC.id

WHERE SPI.prd_end_date IS NULL -- Exclude historical data (let only current data) --

GO


/*
===============================================================================================================================================
------------------------------------------------------- Creation of gold.fact_sales VIEW ------------------------------------------------------
===============================================================================================================================================
*/
--Checks if we have a view with the same name (Please ensure that you have a backup before excuting this line)--
IF OBJECT_ID('gold.fact_sales', 'U') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

--View Creation--
CREATE VIEW gold.fact_sales AS

SELECT
	
	-- Selecting the other columns we need it in the gold layer --
	SSD.sls_order_num AS order_number,
	GDP.product_key AS product_key,
	GDC.customer_key AS customer_key,
	SSD.sls_order_date AS order_date,
	SSD.sls_ship_date AS ship_date,
	SSD.sls_due_date AS due_date,
	SSD.sls_sales AS sales,
	SSD.sls_quantity AS quantity,
	SSD.sls_price AS price

FROM silver.crm_sales_details AS SSD
LEFT JOIN gold.dim_products AS GDP
ON		SSD.sls_product_key = GDP.product_number
LEFT JOIN gold.dim_customers AS GDC
ON		SSD.sls_customer_id = GDC.customer_id

GO


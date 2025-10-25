/*
===============================================================================
DDL_TABLES_BRONZE Script: We are creating the Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


/*
===============================================================================
--------------------Creation of crm_customer_info table------------------------
===============================================================================
*/
--Checks if we have a table with the same name (Please ensure that you have a backup before excuting this line)--

if OBJECT_ID('bronze.crm_customer_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_customer_info
GO


--Table Creation--
CREATE TABLE bronze.crm_customer_info (

    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);
GO


/*
===============================================================================
---------------------Creation of crm_products_info table------------------------
===============================================================================
*/
--Checks if we have a table with the same name (Please ensure that you have a backup before excuting this line)--

if OBJECT_ID('bronze.crm_products_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_products_info
GO


--Table Creation--
CREATE TABLE bronze.crm_products_info (

    prd_id INT,
    prd_key NVARCHAR(50),
    prd_name NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_date DATE,
    prd_end_date DATE
);
GO


/*
===============================================================================
---------------------Creation of crm_sales_details table------------------------
===============================================================================
*/
--Checks if we have a table with the same name (Please ensure that you have a backup before excuting this line)--

if OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details
GO


--Table Creation--
CREATE TABLE bronze.crm_sales_details (

    sls_order_num NVARCHAR(50),
    sls_product_key NVARCHAR(50),
    sls_customer_id INT,
    sls_order_date INT,
    sls_ship_date INT,
    sls_due_date INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT

);
GO


/*
===============================================================================
---------------------Creation of erp_cust_az12 table------------------------
===============================================================================
*/
--Checks if we have a table with the same name (Please ensure that you have a backup before excuting this line)--

if OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12
GO


--Table Creation--
CREATE TABLE bronze.erp_cust_az12 (

    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)

);
GO


/*
===============================================================================
---------------------Creation of erp_loc_a101 table------------------------
===============================================================================
*/
--Checks if we have a table with the same name (Please ensure that you have a backup before excuting this line)--

if OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101
GO


--Table Creation--
CREATE TABLE bronze.erp_loc_a101 (

    cid NVARCHAR(50),
    cntry NVARCHAR(50)
 
);
GO


/*
===============================================================================
---------------------Creation of erp_px_cat_g1v2 table------------------------
===============================================================================
*/
--Checks if we have a table with the same name (Please ensure that you have a backup before excuting this line)--

if OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2
GO


--Table Creation--
CREATE TABLE bronze.erp_px_cat_g1v2 (

    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)

);
GO

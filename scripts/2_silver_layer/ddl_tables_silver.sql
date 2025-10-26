/*
===============================================================================
DDL_TABLES_SILVER Script: We are creating the Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/


/*
===============================================================================
--------------------Creation of crm_customer_info table------------------------
===============================================================================
*/
--Checks if we have a table with the same name (Please ensure that you have a backup before excuting this line)--

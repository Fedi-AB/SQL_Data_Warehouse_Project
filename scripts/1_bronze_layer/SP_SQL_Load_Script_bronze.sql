/*
======================================================================================
Stored Procedure: SP_SQL_Load_Script_bronze layer (Source -> Bronze)
======================================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates (make the tables empty) the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
        
        ==> The SQL command BULK INSERT is used to quickly load large amounts of data 
        from a file(example : .csv file) into a database table.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
======================================================================================
*/

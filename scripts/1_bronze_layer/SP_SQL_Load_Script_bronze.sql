/*
======================================================================================
Stored Procedure: SP_SQL_Load_Script_bronze_layer (Source -> Bronze)
======================================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates (make the tables empty) the bronze tables before loading data 
    to avoid duplications.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
        
        ==> The SQL command BULK INSERT is used to quickly load large amounts of data 
        from a file(example : .csv file) into a database table.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
======================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    --Declaring the datetime variable to Track the ETL Duration--
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
--------------------------------------------------------------------------------------------------------
--Adding TRY...CATCH for ensuring error handling, data integrity and issue logging for esier debugging--
----------SQL runs the TRY block and if it fails, it runs the CATCH block to handle the error-----------
--------------------------------------------------------------------------------------------------------
    BEGIN TRY
        --Seting the starting time of whole batch--
        SET @batch_start_time = GETDATE();
        PRINT'<<<<<---------------------------------------------------------------------------->>>>>';
        PRINT'======================= The loading of the bronze layer is starting ==================';
        PRINT'<<<<<---------------------------------------------------------------------------->>>>>';

        PRINT'======================================================================================';
        PRINT'                                 Loading Bronze Layer                                 ';
        PRINT'======================================================================================';
        PRINT'<<<<<---------------------------------------------------------------------------->>>>>';
        PRINT'================================= Loading CRM Tables =================================';
        PRINT'<<<<<---------------------------------------------------------------------------->>>>>';
        
        --Seting the starting time of crm_customer_info loading--
        SET @start_time = GETDATE();
        PRINT'======================================================================================';
        PRINT'>>>>>>>>> Truncate the crm_customer_info table before proceeding the loading operation';
        PRINT'======================================================================================';
        TRUNCATE TABLE bronze.crm_customer_info;

        PRINT'======================================================================================';
        PRINT'----------------------- crm_customer_info table Loading operation --------------------';
        PRINT'======================================================================================';
        BULK INSERT bronze.crm_customer_info
        FROM 'D:\Certification & Formation\Data Engineering\SQL\Project\SQL_Data_Warehouse_Project\datasets\source_crm\cust_info.csv'
        WITH (
        --= Declaring that the firstrow to start loading is the 2nd row because the 1st is column head =--
            FIRSTROW = 2,
        --= Declaring that the seperator between filds is a comma ',' =--
            FIELDTERMINATOR = ',',
        --= Lock the table while processing the load operation to optimize the performance of the loading =--
            TABLOCK
        );

        --Seting the ending time of crm_customer_info loading--
        SET @end_time = GETDATE();
        --Calculation of crm_customer_info loading duration--
        PRINT'                 *** crm_customer_info Load Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds ***'

			
        --Seting the starting time of crm_products_info loading--
        SET @start_time = GETDATE();
        PRINT'======================================================================================';
        PRINT'>>>>>>>>> Truncate the crm_products_info table before proceeding the loading operation';
        PRINT'======================================================================================';
        TRUNCATE TABLE bronze.crm_products_info;

        PRINT'======================================================================================';
        PRINT'----------------------- crm_products_info table Loading operation --------------------';
        PRINT'======================================================================================';
        BULK INSERT bronze.crm_products_info
        FROM 'D:\Certification & Formation\Data Engineering\SQL\Project\SQL_Data_Warehouse_Project\datasets\source_crm\prd_info.csv'
        WITH (
        --= Declaring that the firstrow to start loading is the 2nd row because the 1st is column head =--
            FIRSTROW = 2,
        --= Declaring that the seperator between filds is a comma ',' =--
            FIELDTERMINATOR = ',',
        --= Lock the table while processing the load operation to optimize the performance of the loading =--
            TABLOCK
        );

        --Seting the ending time of crm_products_info loading--
        SET @end_time = GETDATE();
        --Calculation of crm_products_info loading duration--
        PRINT'                 *** crm_products_info Load Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds ***'

			
        --Seting the starting time of crm_sales_details loading--
        SET @start_time = GETDATE();
        PRINT'======================================================================================';
        PRINT'>>>>>>>>> Truncate the crm_sales_details table before proceeding the loading operation';
        PRINT'======================================================================================';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT'======================================================================================';
        PRINT'----------------------- crm_sales_details table Loading operation --------------------';
        PRINT'======================================================================================';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\Certification & Formation\Data Engineering\SQL\Project\SQL_Data_Warehouse_Project\datasets\source_crm\sales_details.csv'
        WITH (
        --= Declaring that the firstrow to start loading is the 2nd row because the 1st is column head =--
            FIRSTROW = 2,
        --= Declaring that the seperator between filds is a comma ',' =--
            FIELDTERMINATOR = ',',
        --= Lock the table while processing the load operation to optimize the performance of the loading =--
            TABLOCK
        );

        --Seting the ending time of crm_sales_details loading--
        SET @end_time = GETDATE();
        --Calculation of crm_sales_details loading duration--
        PRINT'                 *** crm_sales_details Load Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds ***'

        PRINT'<<<<<---------------------------------------------------------------------------->>>>>';
        PRINT'================================= Loading ERP Tables =================================';
        PRINT'<<<<<---------------------------------------------------------------------------->>>>>';


        --Seting the starting time of erp_cust_az12 loading--
        SET @start_time = GETDATE();
        PRINT'======================================================================================';
        PRINT'>>>>>>>>>>>>> Truncate the erp_cust_az12 table before proceeding the loading operation';
        PRINT'======================================================================================';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT'======================================================================================';
        PRINT'------------------------- erp_cust_az12 table Loading operation ----------------------';
        PRINT'======================================================================================';
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\Certification & Formation\Data Engineering\SQL\Project\SQL_Data_Warehouse_Project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
        --= Declaring that the firstrow to start loading is the 2nd row because the 1st is column head =--
            FIRSTROW = 2,
        --= Declaring that the seperator between filds is a comma ',' =--
            FIELDTERMINATOR = ',',
        --= Lock the table while processing the load operation to optimize the performance of the loading =--
            TABLOCK
        );

        --Seting the ending time of erp_cust_az12 loading--
        SET @end_time = GETDATE();
        --Calculation of erp_cust_az12 loading duration--
        PRINT'                 *** erp_cust_az12 Load Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds ***'

        
        --Seting the starting time of erp_loc_a101 loading--
        SET @start_time = GETDATE();
        PRINT'======================================================================================';
        PRINT'>>>>>>>>>>>>> Truncate the erp_loc_a101 table before proceeding the loading operation ';
        PRINT'======================================================================================';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT'======================================================================================';
        PRINT'-------------------------- erp_loc_a101 table Loading operation ----------------------';
        PRINT'======================================================================================';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\Certification & Formation\Data Engineering\SQL\Project\SQL_Data_Warehouse_Project\datasets\source_erp\LOC_A101.csv'
        WITH (
        --= Declaring that the firstrow to start loading is the 2nd row because the 1st is column head =--
            FIRSTROW = 2,
        --= Declaring that the seperator between filds is a comma ',' =--
            FIELDTERMINATOR = ',',
        --= Lock the table while processing the load operation to optimize the performance of the loading =--
            TABLOCK
        );

        --Seting the ending time of erp_loc_a101 loading--
        SET @end_time = GETDATE();
        --Calculation of erp_loc_a101 loading duration--
        PRINT'                 *** erp_loc_a101 Load Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds ***'


        --Seting the starting time of erp_px_cat_g1v2 loading--
        SET @start_time = GETDATE();
        PRINT'======================================================================================';
        PRINT'>>>>>>>>>>> Truncate the erp_px_cat_g1v2 table before proceeding the loading operation';
        PRINT'======================================================================================';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT'======================================================================================';
        PRINT'------------------------ erp_px_cat_g1v2 table Loading operation ---------------------';
        PRINT'======================================================================================';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\Certification & Formation\Data Engineering\SQL\Project\SQL_Data_Warehouse_Project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
        --= Declaring that the firstrow to start loading is the 2nd row because the 1st is column head =--
            FIRSTROW = 2,
        --= Declaring that the seperator between filds is a comma ',' =--
            FIELDTERMINATOR = ',',
        --= Lock the table while processing the load operation to optimize the performance of the loading =--
            TABLOCK
        );

        --Seting the ending time of erp_px_cat_g1v2 loading--
        SET @end_time = GETDATE();
        --Calculation of erp_px_cat_g1v2 loading duration--
        PRINT'                 *** erp_px_cat_g1v2 Load Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds ***'
        PRINT'======================================================================================';


        --Seting the ending time of whole batch loading--
        SET @batch_end_time = GETDATE();
        PRINT'======================================================================================';
        PRINT'====================== The loading of the bronze layer is completed ==================';
        --Calculation of the whole batch loading duration--
        PRINT'                     *** Total duration of loading is: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds ***'
        PRINT'======================================================================================';


    END TRY
    BEGIN CATCH
        PRINT'======================================================================================';
        PRINT'!!!!!!!!!!!!!!!!!!!!! ERROR OCCURED DURING LOADING BRONZE LAYER !!!!!!!!!!!!!!!!!!!!!!';
        PRINT'Error message : ' + ERROR_MESSAGE();
        PRINT'Error message : ' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT'Error message : ' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT'======================================================================================';
    END CATCH
END

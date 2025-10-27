/*===============================================================================
        Stored Procedure: SP_SQL_Load_Script_silver_layer (Bronze -> Silver)
=================================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.(make the tables empty) 
		the silver tables before loading data to avoid duplications.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

================================================================================*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
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
		PRINT'======================= The loading of the Silver layer is starting ==================';
		PRINT'<<<<<---------------------------------------------------------------------------->>>>>';

		PRINT'======================================================================================';
		PRINT'                                 Loading Silver Layer                                 ';
		PRINT'======================================================================================';
		PRINT'<<<<<---------------------------------------------------------------------------->>>>>';
		PRINT'================================= Loading CRM Tables =================================';
		PRINT'<<<<<---------------------------------------------------------------------------->>>>>';

		--Seting the starting time of silver.crm_customer_info loading--
        SET @start_time = GETDATE();
		PRINT'======================================================================================';
		PRINT'>>>>>>>>> Truncate the crm_customer_info table before proceeding the loading operation';
		PRINT'======================================================================================';
		TRUNCATE TABLE silver.crm_customer_info;

		PRINT'======================================================================================';
		PRINT'>>>>>>>>> Inserting clean data to the silver.crm_customer_info table                  ';
		PRINT'======================================================================================';
		INSERT INTO silver.crm_customer_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)

		/*
		Eliminates Nulls and duplication in the Primary key 
		of bronze.crm_customer_info and cleaning data before loading it into the silver table 
		*/

		--Selecting all data without their duplications (last created ones)--
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname, --Cleaning data fron unwanted Spaces--
			TRIM(cst_lastname) AS cst_lastname, --Cleaning data fron unwanted Spaces--

			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'Unknown'
			END AS cst_marital_status, -- Normalizing the cst_marital_status data to readable format --
	
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'Unknown'
			END AS cst_gndr, -- Normalizing the cst_gndr data to readable format --
			cst_create_date
		FROM(
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS last_ranked
			FROM bronze.crm_customer_info
			WHERE cst_id IS NOT NULL
			)t --Removing duplicated data using the window function of runking ROW_NUMBER and sorted the data to exclude th old duplicate data--

		WHERE last_ranked = 1 -- Select the most recent record per customer
		--Seting the ending time of silver.crm_customer_info loading--
        SET @end_time = GETDATE();
        --Calculation of silver.crm_customer_info loading duration--
        PRINT'             *** silver.crm_customer_info Load Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds ***'


	
		/*
		DROP and create the silver.crm_products_info table after metadata modification
		*/
		/*IF OBJECT_ID('silver.crm_products_info', 'U') IS NOT NULL
			DROP TABLE silver.crm_products_info;
		CREATE TABLE silver.crm_products_info (
			prd_id INT,
			cat_id NVARCHAR(50),
			prd_key NVARCHAR(50),
			prd_name NVARCHAR(50),
			prd_cost INT,
			prd_line NVARCHAR(50),
			prd_start_date DATE,
			prd_end_date DATE,
			dwh_create_date DATETIME2 DEFAULT GETDATE()
		);
		GO
		*/
		--Seting the starting time of silver.crm_products_info loading--
        SET @start_time = GETDATE();
		PRINT'======================================================================================';
		PRINT'>>>>>>>>> Truncate the crm_products_info table before proceeding the loading operation';
		PRINT'======================================================================================';
		TRUNCATE TABLE silver.crm_products_info;

		PRINT'======================================================================================';
		PRINT'>>>>>>>>> Inserting clean data to the silver.crm_products_info table                  ';
		PRINT'======================================================================================';
		INSERT INTO silver.crm_products_info (
			prd_id,
			cat_id,
			prd_key,
			prd_name,
			prd_cost,
			prd_line,
			prd_start_date,
			prd_end_date
		)

		/*
		Eliminates Nulls and duplication in the Primary key 
		of bronze.crm_products_info before loading it into the silver table 
		*/
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,-- Extract category ID and normalize the data format with erp_px_cat_g1v2 table --
			SUBSTRING(prd_key, 7, len(prd_key))AS prd_key, -- Extract the product key --
			prd_name,
			ISNULL(prd_cost, 0) AS prd_cost, -- Handle the nulls --
			CASE
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'Unknown'
			END AS prd_line, -- Normalizing the prd_line data to readable format --
			prd_start_date,
			LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date) AS prd_end_date -- Calculate end date as one day before the next start date--
		FROM bronze.crm_products_info

		--Seting the ending time of silver.crm_products_info loading--
        SET @end_time = GETDATE();
        --Calculation of silver.crm_products_info loading duration--
        PRINT'             *** silver.crm_products_info Load Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds ***'



		/*
		DROP and create the silver.crm_sales_details table after metadata modification
		*/
		/*IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
			DROP TABLE silver.crm_sales_details;

		CREATE TABLE silver.crm_sales_details (
			sls_order_num NVARCHAR(50),
			sls_product_key NVARCHAR(50),
			sls_customer_id INT,
			sls_order_date DATE,
			sls_ship_date DATE,
			sls_due_date DATE,
			sls_sales INT,
			sls_quantity INT,
			sls_price INT,
			dwh_create_date DATETIME2 DEFAULT GETDATE()
		);
		GO
		*/

		--Seting the starting time of silver.crm_sales_details loading--
        SET @start_time = GETDATE();
		PRINT'======================================================================================';
		PRINT'>>>>>>>>> Truncate the crm_sales_details table before proceeding the loading operation';
		PRINT'======================================================================================';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT'======================================================================================';
		PRINT'>>>>>>>>> Inserting clean data to the silver.crm_sales_details table                  ';
		PRINT'======================================================================================';
		INSERT INTO silver.crm_sales_details (
			sls_order_num,
			sls_product_key,
			sls_customer_id,
			sls_order_date,
			sls_ship_date,
			sls_due_date,
			sls_sales,
			sls_quantity,
			sls_price
		)


		/*
		Eliminates Nulls and duplication in the Primary key 
		of bronze.crm_sales_details and cleaning data before loading it into the silver table 
		*/
		SELECT
			sls_order_num,
			sls_product_key,
			sls_customer_id,
			CASE
				WHEN sls_order_date = 0 OR LEN(sls_order_date) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_date AS NVARCHAR) AS DATE) -- we cannot convert value fron INT to DATE directly
			END AS sls_order_date, -- Cleaning the sls_order_date column by handling stange date values (0 or len(sls_order_date) > or < 8) --

			CASE
				WHEN sls_ship_date = 0 OR LEN(sls_ship_date) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_date AS NVARCHAR) AS DATE) -- we cannot convert value fron INT to DATE directly
			END AS sls_ship_date, -- Cleaning the sls_ship_date column by handling stange date values (0 or len(sls_ship_date) > or < 8) --

			CASE
				WHEN sls_due_date = 0 OR LEN(sls_due_date) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_date AS NVARCHAR) AS DATE) -- we cannot convert value fron INT to DATE directly
			END AS sls_due_date, -- Cleaning the sls_ship_date column by handling stange date values (0 or len(sls_due_date) > or < 8) --

			--Sales should be !=0, positive and not NULL--
			--Sales = Quantity * Price --
			CASE
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price) 
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect --

			sls_quantity,
	
			CASE
				WHEN sls_price IS NULL OR sls_price <=0
					THEN sls_sales / NULLIF(sls_quantity, 0) --replace nulls with zero for quantity
				ELSE sls_price
			END AS sls_price -- Derive price if original value is invalid --

		FROM bronze.crm_sales_details	

		--Seting the ending time of silver.crm_sales_details loading--
        SET @end_time = GETDATE();
        --Calculation of silver.crm_sales_details loading duration--
        PRINT'             *** silver.crm_sales_details Load Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds ***'



		PRINT'<<<<<---------------------------------------------------------------------------->>>>>';
		PRINT'================================= Loading ERP Tables =================================';
		PRINT'<<<<<---------------------------------------------------------------------------->>>>>';

		--Seting the starting time of silver.erp_cust_az12 loading--
        SET @start_time = GETDATE();
		PRINT'======================================================================================';
		PRINT'>>>>>>>>> Truncate the erp_cust_az12 table before proceeding the loading operation    ';
		PRINT'======================================================================================';
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT'======================================================================================';
		PRINT'>>>>>>>>> Inserting clean data to the silver.erp_cust_az12 table                      ';
		PRINT'======================================================================================';
		INSERT INTO silver.erp_cust_az12 (
	
			cid,
			bdate,
			gen
	
		)

		/*
		Eliminates Nulls and duplication in the Primary key  and cleaning data
		of bronze.erp_cust_az12 before loading it into the silver table 
		*/

		SELECT

			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			END AS cid, -- Normalize the cid (cst_key in crm_customer_info table) by eliminating the 'NAS' letters when finded--

			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, --Pass bithdate in the futur to NULL--

			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'Unknown'
			END AS gen -- Normalizing the cst_gndr data to readable format --
		FROM bronze.erp_cust_az12

		--Seting the ending time of silver.erp_cust_az12 loading--
        SET @end_time = GETDATE();
        --Calculation of silver.erp_cust_az12 loading duration--
        PRINT'             *** silver.erp_cust_az12 Load Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds ***'


		--Seting the starting time of silver.erp_loc_a101 loading--
        SET @start_time = GETDATE();
		PRINT'======================================================================================';
		PRINT'>>>>>>>>> Truncate the erp_loc_a101 table before proceeding the loading operation     ';
		PRINT'======================================================================================';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT'======================================================================================';
		PRINT'>>>>>>>>> Inserting clean data to the silver.erp_loc_a101 table                       ';
		PRINT'======================================================================================';
		INSERT INTO silver.erp_loc_a101 (
	
			cid,
			cntry
		)

		/*
		Eliminates Nulls and duplication in the Primary key  and cleaning data
		of bronze.erp_loc_a101 before loading it into the silver table 
		*/

		SELECT

			replace(cid, '-', '') AS cid,--We should eliminate '-' from cid to normalize it with cst_key in crm_customer_info--
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes --

		FROM bronze.erp_loc_a101

		--Seting the ending time of silver.erp_loc_a101 loading--
        SET @end_time = GETDATE();
        --Calculation of silver.erp_loc_a101 loading duration--
        PRINT'             *** silver.erp_loc_a101 Load Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds ***'



		--Seting the starting time of silver.erp_px_cat_g1v2 loading--
        SET @start_time = GETDATE();
		PRINT'======================================================================================';
		PRINT'>>>>>>>>> Truncate the erp_px_cat_g1v2 table before proceeding the loading operation  ';
		PRINT'======================================================================================';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT'======================================================================================';
		PRINT'>>>>>>>>> Inserting clean data to the silver.erp_px_cat_g1v2 table                    ';
		PRINT'======================================================================================';
		INSERT INTO silver.erp_px_cat_g1v2(
	
			id,
			cat,
			subcat,
			maintenance
		)

		/*
		Eliminates Nulls and duplication in the Primary key  and cleaning data
		of bronze.erp_px_cat_g1v2 before loading it into the silver table 
		*/

		SELECT
			id,
			cat,
			subcat,
			maintenance

		FROM bronze.erp_px_cat_g1v2

		--Seting the ending time of silver.erp_px_cat_g1v2 loading--
        SET @end_time = GETDATE();
        --Calculation of silver.erp_px_cat_g1v2 loading duration--
        PRINT'            *** silver.erp_px_cat_g1v2 Load Duration is: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds ***'


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

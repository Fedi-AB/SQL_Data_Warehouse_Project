/*
=============================================================
----------------Create Database and Schemas------------------
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


--===========================================--
--Creation of new database 'SQLDataWarehouse'--
--===========================================--
USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'SQLDataWarehouse')
BEGIN
    DROP DATABASE SQLDataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE SQLDataWarehouse;
GO

USE SQLDataWarehouse;
GO


----Switch to the new DB 'SQLDataWarehouse'----
USE SQLDataWarehouse;
GO

--===========================================--
------------Creation of Schemas----------------
--===========================================--
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

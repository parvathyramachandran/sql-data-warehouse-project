/*
===============================================================
Create Database and Schemas
===============================================================
Script Purpose:
   This script creates a new database named 'DataWarehouse' after checking if it already exists 
   If the database exists, it is dropped and recreated. 
   Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'
WARNING:
   Running this script will drop the entire 'DataWarehouse' database if it exists. 
   All data in the database will be permanently deleted. 
   Proceed with caution and ensure you have proper backups before running this script.
*/

USE Master;
GO
--Drop and recreate the'DataWarehouse' Database

IF EXISTS(SELECT 1 FROM Sys.database WHERE name ='Datawarehouse')
BEGIN
     ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
     DROP DATABASE Datawarehouse;
END;
GO

--Create the 'Datawarehouse' database
CREATE DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO

--Create Schemas
CREATE SCHEMA Bronze;
GO

CREATE SCHEMA Silver;
Go

CREATE SCHEMA Gold;
GO

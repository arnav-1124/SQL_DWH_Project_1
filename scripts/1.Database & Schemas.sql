/*
-------------------------------------
CREATING DATABASE AND SCHEMAS
-------------------------------------

Script Purpose: Create a new database name 'DataWarehouse' and within that creating
				3 schemas ('bronze', 'silver' and 'gold').

*/

USE master;
GO

-- Creating a new Database named "DataWarehouse"
CREATE DATABASE DataWarehouse;

-- Creating all 3 Schemas:
CREATE SCHEMA bronze; 
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;


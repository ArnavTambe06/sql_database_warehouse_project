/*
Creating database called DataWarehouse and Schemas
Purpose: To Store the data and the three layer that are bronze, silver and gold.
*/



--Creating in Database in master
use master;
go
  
--Creating Database
create database DataWarehouse;
go

--Using the DataWarehouse to build Schemas in it
use DataWarehouse;
go

--Creating Bronze layer(Schema).
create schema bronze;
go

--Creating Silver layer(Schema).
create schema silver;
go

--Creating Gold layer(Schema).
create schema gold;

/*
=======================================================================================
  Create Database and Schemas
=======================================================================================
This script create a database and 3 schemas Bronze, Silver and Gold. It fitst check if the dabase exitsts
if it dose then we go to single_user mode This kicks out any other users or connections
to drop our database saftly. Then we create our databse
*/

--get to the master
use master;
GO
--check if database name is already in use
IF EXISTS(SELECT 1 FROM sys.database where name = 'DataWarehouse')
  begin
  alter database DataWarehouse set SINGLE_USER WITH ROLLBACK immediate;
  DROP database DataWarehouse;
  end;
GO
--create the database
create database DataWarehouse;
GO
--use it to put our schemas
use DataWarehouse;
GO
--add our schemas
create schema bronze;
GO
create schema silver;
GO
create schema gold;

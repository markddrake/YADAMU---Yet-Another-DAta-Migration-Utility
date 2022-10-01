--
:setvar DATABASE Northwind    
-- 
DROP DATABASE $(DATABASE)$(ID)
go
create DATABASE $(DATABASE)$(ID)
go
--
:setvar DATABASE AdventureWorksDW
--
DROP DATABASE $(DATABASE)$(ID)
go
create DATABASE $(DATABASE)$(ID)
go
--
:setvar DATABASE AdventureWorks  
--
DROP DATABASE $(DATABASE)$(ID)
go
create DATABASE $(DATABASE)$(ID)
go
--
use $(DATABASE)$(ID)
go
:setvar SCHEMA HumanResources
--
CREATE SCHEMA $(SCHEMA)
go
--
:setvar SCHEMA Person
--
CREATE SCHEMA $(SCHEMA)
go
--
:setvar SCHEMA Production
--
CREATE SCHEMA $(SCHEMA)
go
--
:setvar SCHEMA Purchasing
--
CREATE SCHEMA $(SCHEMA)
go
--
:setvar SCHEMA Sales
--
CREATE SCHEMA $(SCHEMA)
go
--
/*
**
** Dissable Command Line Testing for WorldWideImporters and WorldWideImportersDW
**
*/
quit
--
:setvar DATABASE WorldWideImporters  
--
DROP DATABASE $(DATABASE)$(ID)
go
create DATABASE $(DATABASE)$(ID)
go
--
use $(DATABASE)$(ID)
go
:setvar SCHEMA Application
--
CREATE SCHEMA $(SCHEMA)
go
--
:setvar SCHEMA Purchasing
--
CREATE SCHEMA $(SCHEMA)
go
--
:setvar SCHEMA Sales
--
CREATE SCHEMA $(SCHEMA)
go
--
:setvar SCHEMA Warehouses
--
CREATE SCHEMA $(SCHEMA)
go
--
:setvar DATABASE WorldWideImportersDW  
--
DROP DATABASE $(DATABASE)$(ID)
go
create DATABASE $(DATABASE)$(ID)
go
--
use $(DATABASE)$(ID)
go
:setvar SCHEMA Dimension
--
CREATE SCHEMA $(SCHEMA)
go
--
:setvar SCHEMA Fact
--
CREATE SCHEMA $(SCHEMA)
go
--
:setvar SCHEMA Integration
--
CREATE SCHEMA $(SCHEMA)
go
--
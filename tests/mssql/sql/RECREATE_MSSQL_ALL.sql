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

:setvar SCHEMA "HR1"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)')
SELECT @DDL_STATEMENT
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)
go
CREATE SCHEMA $(SCHEMA)
go
:setvar SCHEMA "SH1"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)')
SELECT @DDL_STATEMENT
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)
go
CREATE SCHEMA $(SCHEMA)
go
:setvar SCHEMA "OE1"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)')
SELECT @DDL_STATEMENT
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)
go
CREATE SCHEMA $(SCHEMA)
go
:setvar SCHEMA "PM1"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)')
SELECT @DDL_STATEMENT
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)
go
CREATE SCHEMA $(SCHEMA)
go
:setvar SCHEMA "IX1"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)')
SELECT @DDL_STATEMENT
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)
go
CREATE SCHEMA $(SCHEMA)
go
:setvar SCHEMA "BI1"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SELECT @DDL_STATEMENT
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)')
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)
go
CREATE SCHEMA $(SCHEMA)
go
:setvar SCHEMA "Northwind"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SELECT @DDL_STATEMENT
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)')
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)
go
CREATE SCHEMA $(SCHEMA)
go

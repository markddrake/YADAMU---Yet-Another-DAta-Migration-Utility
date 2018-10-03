:setvar SCHEMA "HR"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)$(ID)')
SELECT @DDL_STATEMENT
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)$(ID)
go
CREATE SCHEMA $(SCHEMA)$(ID)
go
:setvar SCHEMA "SH"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)$(ID)')
SELECT @DDL_STATEMENT
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)$(ID)
go
CREATE SCHEMA $(SCHEMA)$(ID)
go
:setvar SCHEMA "OE"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)$(ID)')
SELECT @DDL_STATEMENT
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)$(ID)
go
CREATE SCHEMA $(SCHEMA)$(ID)
go
:setvar SCHEMA "PM"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)$(ID)')
SELECT @DDL_STATEMENT
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)$(ID)
go
CREATE SCHEMA $(SCHEMA)$(ID)
go
:setvar SCHEMA "IX"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)$(ID)')
SELECT @DDL_STATEMENT
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)$(ID)
go
CREATE SCHEMA $(SCHEMA)$(ID)
go
:setvar SCHEMA "BI"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SELECT @DDL_STATEMENT
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)$(ID)')
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)$(ID)
go
CREATE SCHEMA $(SCHEMA)$(ID)
go

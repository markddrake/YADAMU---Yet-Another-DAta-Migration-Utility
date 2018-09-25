:setvar SCHEMA "HR"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '${SCHEMA)')
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)
go
CREATE SCHEMA $(SCHEMA)
go
:setvar SCHEMA "SH"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '${SCHEMA)')
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)
go
CREATE SCHEMA $(SCHEMA)
go
:setvar SCHEMA "OE"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '${SCHEMA)')
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)
go
CREATE SCHEMA $(SCHEMA)
go
:setvar SCHEMA "PM"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '${SCHEMA)')
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)
go
CREATE SCHEMA $(SCHEMA)
go
:setvar SCHEMA "IX"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '${SCHEMA)')
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)
go
CREATE SCHEMA $(SCHEMA)
go
:setvar SCHEMA "BI"
DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '${SCHEMA)')
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)
go
CREATE SCHEMA $(SCHEMA)
go

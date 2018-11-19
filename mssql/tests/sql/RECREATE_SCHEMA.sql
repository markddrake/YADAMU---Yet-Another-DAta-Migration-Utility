DECLARE @DDL_STATEMENT VARCHAR(MAX)
SET @DDL_STATEMENT = (select 'DROP TABLE ' + STRING_AGG('"' + table_schema + '"."' + table_name + '"',',') from information_schema.TABlES where table_schema = '$(SCHEMA)$(ID)')
SELECT @DDL_STATEMENT
EXEC (@DDL_STATEMENT);
go
DROP SCHEMA $(SCHEMA)$(ID)
go
CREATE SCHEMA $(SCHEMA)$(ID)
go

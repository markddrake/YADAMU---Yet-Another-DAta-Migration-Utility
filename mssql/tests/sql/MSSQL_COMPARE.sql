--
use $(database)$(id1)
exec $(database)$(id2).dbo.COMPARE_SCHEMA @SOURCE_DATABASE, @SOURCE_SCHEMA, @TARGET_DATABASE, @TARGET_SCHEMA, '$(METHOD)'
go
--
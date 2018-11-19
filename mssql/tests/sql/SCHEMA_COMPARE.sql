--
:setvar Timestampwidth 48
select concat( FORMAT(sysutcdatetime(),'yyyy-MM-dd"T"HH:mm:ss.fffff"Z"'),': "$(SCHEMA)$(ID1)", "$(SCHEMA)$(ID2)", "$(METHOD)"') "Timestamp";
go
--
exec COMPARE_SCHEMA '$(SCHEMA)$(ID1)','$(SCHEMA)$(ID2)'
go
--
select * 
  from SCHEMA_COMPARE_RESULTS 
 order by SOURCE_SCHEMA, TABLE_NAME;
go
--
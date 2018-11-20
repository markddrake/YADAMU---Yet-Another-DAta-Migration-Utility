--
select concat( FORMAT(sysutcdatetime(),'yyyy-MM-dd"T"HH:mm:ss.fffff"Z"'),': "',@SOURCE_DATABASE,'"."',@SOURCE_SCHEMA,'", "',@TARGET_DATABASE,'"."',@TARGET_SCHEMA,'", "$(METHOD)"') "Timestamp";
--
exec COMPARE_SCHEMA @SOURCE_DATABASE, @SOURCE_SCHEMA, @TARGET_DATABASE, @TARGET_SCHEMA
go
--
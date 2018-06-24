--
select TABLE_NAME
  from ALL_ALL_TABLES
 where OWNER = '&SCHEMA'
   and aat.STATUS = 'VALID'
   and aat.DROPPED = 'NO'
   and aat.TEMPORARY = 'N'
   and aat.EXTERNAL = 'NO'
   and aat.NESTED = 'NO'
   and aat.SECONDARY = 'N'
   and (aat.IOT_TYPE is NULL or aat.IOT_TYPE = 'IOT')
/
begin 
  select COLUMN_VALUE
    into :JSON
	from TABLE(JSON_EXPORT.EXPORT_SCHEMA('&SCHEMA'));
end;
/
select JSON_EXPORT.DUMP_SQL_STATEMENT SQL_STATEMENT
  from DUAL
/
--
@@DUMP_JSON &SCHEMA
--

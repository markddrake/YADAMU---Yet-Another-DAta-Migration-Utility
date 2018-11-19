--
SET SESSION SQL_MODE=ANSI_QUOTES;
--
select concat(DATE_FORMAT(CONVERT_TZ(current_timestamp,@@session.time_zone,'+00:00'),'%Y-%m-%dT%TZ'),': "',@SCHEMA,@ID1,'", "',@SCHEMA,@ID2,'", "',@METHOD,'"') "Timestamp";
--
call COMPARE_SCHEMAS(CONCAT(@SCHEMA,@ID1),CONCAT(@SCHEMA,@ID2));
--
select * 
  from SCHEMA_COMPARE_RESULTS 
 order by SOURCE_SCHEMA, TABLE_NAME;
--

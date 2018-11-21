--

select to_char (now()::timestamptz  at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') || ': "' || :'SCHEMA' || :'ID1' || '", "' || :'SCHEMA' || :'ID2' || '", "' || :'METHOD' || '"' "Timestamp";
--
call COMPARE_SCHEMA(:'SCHEMA' || :'ID1',:'SCHEMA' || :'ID2');
--
select * 
  from SCHEMA_COMPARE_RESULTS 
 order by SOURCE_SCHEMA, TABLE_NAME;
--

--
select to_char (now()::timestamptz  at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') || ': "' || :'SCHEMA' || :'ID1' || '", "' || :'SCHEMA' || :'ID2' || '", "' || :'METHOD' || '"' "Timestamp";
--
call COMPARE_SCHEMA(:'SCHEMA' || :'ID1',:'SCHEMA' || :'ID2',FALSE,FALSE,18);
--
select 'SUCCESSFUL' "RESULTS", SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_SCHEMA = :'SCHEMA' || :'ID1'
   and SOURCE_ROW_COUNT = TARGET_ROW_COUNT
   and MISSING_ROWS = 0
   and EXTRA_ROWS = 0
   and SQLERRM is NULL
order by TABLE_NAME;
--
select 'FAILED' "RESULTS", SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, SQLERRM "NOTES"
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_SCHEMA = :'SCHEMA' || :'ID1'
   and (
            SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
         or MISSING_ROWS <> 0
         or EXTRA_ROWS <> 0
         or SQLERRM is NOT NULL
       )
 order by TABLE_NAME;
--
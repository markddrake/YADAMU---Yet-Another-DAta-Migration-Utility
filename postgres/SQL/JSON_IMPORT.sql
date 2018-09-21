/*
**
** Postgress JSON_IMPORT Function.
**
*/
create or replace function map_oracle_data_type(P_DATA_TYPE VARCHAR, P_SIZE_CONSTRAINT VARCHAR) 
returns VARCHAR
as $$
declare
  V_DATA_TYPE  VARCHAR(128);
begin
  V_DATA_TYPE := trim( BOTH '"' from P_DATA_TYPE);

  case V_DATA_TYPE
	when 'VARCHAR2' 
	  then return 'VARCHAR(' || trim(BOTH '"' from P_SIZE_CONSTRAINT) || ')';
	when 'NUMBER'
      then return 'NUMERIC(' || trim(BOTH '"' from P_SIZE_CONSTRAINT) || ')';
	when 'NVARCHAR2'
      then return 'VARCHAR(' || trim(BOTH '"' from P_SIZE_CONSTRAINT) || ')';
	when 'RAW'
      then return 'BYTEA';
	when 'BLOB'
      then return 'BYTEA';
	when 'CLOB'
      then return 'TEXT';
	when 'NCLOB'
      then return 'TEXT';
	when 'BFILE'
      then return 'VARCHAR';
	when 'ROWID'
      then return 'VARCHAR';
	else
   	  if (strpos(V_DATA_TYPE,'LOCAL TIME ZONE') > 0) then
	    return replace(V_DATA_TYPE,'LOCAL TIME ZONE','TIME ZONE');	
      end if;
   	  if ((strpos(V_DATA_TYPE,'TIMESTAMP') = 1) and (strpos(V_DATA_TYPE,'LOCAL TIME ZONE') > 0)) then
	    return replace(V_DATA_TYPE,'LOCAL TIME ZONE','TIME ZONE');	
      end if;
   	  if ((strpos(V_DATA_TYPE,'INTERVAL') = 1) and (strpos(V_DATA_TYPE,'YEAR') > 0) and (strpos(V_DATA_TYPE,'TO MONTH') > 0)) then
	    return 'INTERVAL YEAR TO MONTH';
      end if;
	  if (strpos(V_DATA_TYPE,'"."XMLTYPE"'::varchar) > 0) then 
	    return 'XML';
	  end if;
	  if (strpos(V_DATA_TYPE,'"."'::varchar) > 0) then 
	    return 'TEXT';
	  end if;
  end case;
  return V_DATA_TYPE;
end;
$$ LANGUAGE plpgsql;
--
create or replace function import_json(P_JSON jsonb,P_SCHEMA VARCHAR) 
returns jsonb
as $$
declare
  V_COLUMN_NAME_ARRAY VARCHAR[];
  V_DATA_TYPE_ARRAY   VARCHAR[];
  V_DATA_SIZE_ARRAY   VARCHAR[];
  V_STATEMENT         TEXT;
  T                   RECORD;

  V_ROW_COUNT        INTEGER;
  V_START_TIME       TIMESTAMPTZ;
  V_END_TIME         TIMESTAMPTZ;
  V_ELAPSED_TIME     DOUBLE PRECISION;

  V_SUMMARY          JSONB;
  V_TIMING_REPORT    JSONB;
begin
  V_SUMMARY := jsonb_build_array();
  for t in select "owner", "tableName", "columns", "dataTypes","dataTypeSizing"
             from JSONB_EACH(P_JSON -> 'metadata')  CROSS JOIN LATERAL JSONB_TO_RECORD(value) as METADATA("owner" TEXT, "tableName" TEXT, "columns" TEXT, "dataTypes" TEXT, "dataTypeSizing" TEXT) loop

    V_COLUMN_NAME_ARRAY := string_to_array(t."columns",','); 
    V_DATA_TYPE_ARRAY := string_to_array(t."dataTypes",',');
  
    select ARRAY_AGG(value)
	  into V_DATA_SIZE_ARRAY
	  from JSON_ARRAY_ELEMENTS_TEXT(('[' || t."dataTypeSizing" || ']')::json);
	   
    V_STATEMENT := 'CREATE TABLE IF NOT EXISTS "' || P_SCHEMA || '"."' || t."tableName" || '"(';		
	for c in 1 .. array_upper(V_COLUMN_NAME_ARRAY,1) loop
      if (c > 1) then
	    V_STATEMENT := V_STATEMENT || ',';
      end if;
	  V_STATEMENT := V_STATEMENT || ' ' || V_COLUMN_NAME_ARRAY[c] ||  ' ' || MAP_ORACLE_DATA_TYPE(V_DATA_TYPE_ARRAY[c], V_DATA_SIZE_ARRAY[c]);
	end loop;
	V_STATEMENT := V_STATEMENT || ')';
	EXECUTE V_STATEMENT;

	V_STATEMENT := 'INSERT into "' || P_SCHEMA || '"."' || t."tableName" || '"(' || t.columns || ') select ';
	for c in 1 .. array_upper(V_COLUMN_NAME_ARRAY,1) loop
	  if (c > 1) then
	    V_STATEMENT := V_STATEMENT || ',';
      end if;
      V_STATEMENT := V_STATEMENT || 'cast( value ->> ' || c-1 || ' as ' ||  MAP_ORACLE_DATA_TYPE(V_DATA_TYPE_ARRAY[c], V_DATA_SIZE_ARRAY[c]) || ') ' || V_COLUMN_NAME_ARRAY[c];
	end loop;
	V_STATEMENT := V_STATEMENT ||  ' from jsonb_array_elements($1 -> ''data'' -> ''' || t."tableName" || ''')';
	V_START_TIME := clock_timestamp();
	EXECUTE V_STATEMENT  using  P_JSON;
	V_END_TIME := clock_timestamp();
	GET DIAGNOSTICS V_ROW_COUNT := ROW_COUNT;
	V_ELAPSED_TIME := 1000 * ( extract(epoch from V_END_TIME) - extract(epoch from V_START_TIME) );
	V_TIMING_REPORT := jsonb_build_object('tableName',t."tableName",'rowCount',V_ROW_COUNT,'elapsedTime',V_ELAPSED_TIME);
	V_SUMMARY := jsonb_insert(V_SUMMARY,'{0}',V_TIMING_REPORT);

  end loop;
  return V_SUMMARY;
end;  
$$ LANGUAGE plpgsql;

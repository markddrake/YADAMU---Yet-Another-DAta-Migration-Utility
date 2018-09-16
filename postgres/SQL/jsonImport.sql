create or replace function mapOracleDataType(P_DATA_TYPE VARCHAR, P_SIZE_CONSTRAINT VARCHAR) 
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
create or replace function createTables(P_JSON jsonb,P_SCHEMA VARCHAR) 
returns void
as $$
declare
  V_DATA_TYPE_SIZING VARCHAR[];
  V_STATEMENT        TEXT;
  T                  RECORD;
begin
  for t in select "owner", "tableName", string_to_array("columns",',') "columns", string_to_array("dataTypes",',') "dataTypes", '[' || "dataTypeSizing" || ']' "dataTypeSizing"
             from JSONB_EACH(P_JSON -> 'metadata')  CROSS JOIN LATERAL JSONB_TO_RECORD(value) as METADATA("owner" TEXT, "tableName" TEXT, "columns" TEXT, "dataTypes" TEXT, "dataTypeSizing" TEXT) loop
  
    select ARRAY_AGG(value)
	  into V_DATA_TYPE_SIZING
	  from JSON_ARRAY_ELEMENTS_TEXT(t."dataTypeSizing"::json);
	   
	raise notice '%', P_SCHEMA;
	raise notice '%',  t."tableName";
    V_STATEMENT := 'CREATE TABLE IF NOT EXISTS "' || P_SCHEMA || '"."' || t."tableName" || '"(';		
	raise notice '%', V_STATEMENT;
	for c in 1 .. array_upper(t."columns",1) loop
      if (c > 1) then
	    V_STATEMENT := V_STATEMENT || ',';
      end if;
	  V_STATEMENT := V_STATEMENT || ' ' || t."columns"[c] ||  ' ' || mapOracleDataType(t."dataTypes"[c], V_DATA_TYPE_SIZING[c]);
	end loop;
	V_STATEMENT := V_STATEMENT || ')';
	raise notice '%', V_STATEMENT;
	EXECUTE V_STATEMENT;
  end loop;
end;  
$$ LANGUAGE plpgsql;

create or replace function loadData(P_JSON jsonb,P_SCHEMA VARCHAR) 
returns jsonb
as $$
declare
  V_DATA_TYPE_SIZING VARCHAR[];
  V_COLUMN_LIST      VARCHAR(128)[];
  V_STATEMENT        TEXT;
  V_ROW_COUNT        INTEGER;
  T                  RECORD;
  
  V_START_TIME       TIMESTAMPTZ;
  V_END_TIME         TIMESTAMPTZ;
  V_ELAPSED_TIME     DOUBLE PRECISION;

  V_SUMMARY          JSONB;
  V_TIMING_REPORT    JSONB;
begin
  V_SUMMARY := jsonb_build_array();
  for t in select "owner", "tableName", "columns",  string_to_array("dataTypes",',') "dataTypes", '[' || "dataTypeSizing" || ']' "dataTypeSizing"
             from JSONB_EACH(P_JSON -> 'metadata')  CROSS JOIN LATERAL JSONB_TO_RECORD(value) as METADATA("owner" TEXT, "tableName" TEXT, "columns" TEXT, "dataTypes" TEXT, "dataTypeSizing" TEXT) loop
  
    select ARRAY_AGG(value)
      into V_DATA_TYPE_SIZING
	  from JSON_ARRAY_ELEMENTS_TEXT(t."dataTypeSizing"::json);
	 
	V_COLUMN_LIST := string_to_array(t."columns",','); 

	V_STATEMENT := 'INSERT into "' || P_SCHEMA || '"."' || t."tableName" || '"(' || t.columns || ') select ';
	for c in 1 .. array_upper(V_COLUMN_LIST,1) loop
	  if (c > 1) then
	    V_STATEMENT := V_STATEMENT || ',';
      end if;
      V_STATEMENT := V_STATEMENT || 'cast( value ->> ' || c-1 || ' as ' ||  mapOracleDataType(t."dataTypes"[c], V_DATA_TYPE_SIZING[c]) || ') ' || V_COLUMN_LIST[c];
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

create or replace function jsonImport(P_JSON jsonb,P_SCHEMA VARCHAR) 
returns jsonb
as $$ 
declare
begin
  perform createTables(P_JSON,P_SCHEMA);
  return loadData(P_JSON,P_SCHEMA);
end; 
$$ LANGUAGE plpgsql;

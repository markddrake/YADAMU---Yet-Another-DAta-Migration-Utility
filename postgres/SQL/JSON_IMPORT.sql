/*
**
** Postgress JSON_IMPORT Function.
**
*/
create or replace function MAP_FOREIGN_DATA_TYPE(P_DATA_TYPE VARCHAR, P_DATA_TYPE_LENGTH INT, P_DATA_TYPE_SCALE INT) 
returns VARCHAR
as $$
declare
  V_DATA_TYPE  VARCHAR(128);
begin
  V_DATA_TYPE := trim( BOTH '"' from P_DATA_TYPE);

  case V_DATA_TYPE
    -- Oracle DirectMappings
    when 'VARCHAR2' 
      then return 'varchar';
    when 'NUMBER'
      then return 'numeric';
    when 'NVARCHAR2'
      then return 'varchar';
    when 'RAW'
      then return 'bytea';
    when 'BLOB'
      then return 'bytea';
    when 'CLOB'
      then return 'text';
    when 'NCLOB'
      then return 'text';
    when 'BFILE'
      then return 'varchar';
    when 'ROWID'
      then return 'varchar';
    when 'ANYDATA'
      then return 'text';
    when 'XMLTYPE'
      then return 'xml';
    -- MySQL Direct Mappings
    when 'binary'
      then return 'bytea';
    when 'bit'
      then return 'boolean';
    when 'datetime'
      then return 'timestamp';
    when 'double'
      then return 'double precision';
    when  'enum'
      then return 'varchar(255)';   
    when 'float'
      then return 'real';
    when 'geometry'
      then return 'json';
    when 'mediumint'
      then return 'integer';
    when 'tinyblob'
      then return 'bytea';
    when 'blob'
      then return 'bytea';
    when 'mediumblob'
      then return 'bytea';
    when 'longblob'
      then return 'bytea';
    when  'set'
      then return 'varchar(255)';   
    when 'tinyint'
      then return 'smallint';
    when 'tinytext'
      then return 'text';
    when 'text'
      then return 'text';
    when 'mediumtext'
      then return 'text';
    when 'longtext'
      then return 'text';
    when 'varbinary'
      then return 'bytea';
    when 'hierarchyid'
      then return 'bytea';
    when 'year'
      then return 'smallint';
    -- MSSQL Direct Mappings
    when 'datetime2'
      then return 'timestamp';
    when 'datetimeoffset' 
      then return 'timestamp with time zone';
    when 'image'
      then return 'bytea';
    when 'nchar'
      then return 'char';
    when 'ntext'
      then return'text';
    when 'nvarchar'
      then case P_DATA_TYPE_LENGTH 
             when -1 
               then return 'text';
             else
               return 'varchar'; 
           end case;
    when 'varchar'
      then case P_DATA_TYPE_LENGTH 
             when -1 
               then return 'text';
             else
               return 'varchar';
           end case;
    when 'rowversion'
      then return 'bytea';
    when 'smalldatetime'
      then return'timestamp(0)';
    when 'smallmoney'
      then return'money';
    when 'tinyint'
      then return 'smallint';
    when 'uniqueidentifier'
      then return 'varchar(36)';
    when 'varbinary'
      then return 'bytea';
    when 'geography'
      then return 'jsonb';
    else
      -- Oracle complex mappings
      if (strpos(V_DATA_TYPE,'LOCAL TIME ZONE') > 0) then
        return lower(replace(V_DATA_TYPE,'LOCAL TIME ZONE','TIME ZONE'));  
      end if;
      if ((strpos(V_DATA_TYPE,'TIMESTAMP') = 1) and (strpos(V_DATA_TYPE,'LOCAL TIME ZONE') > 0)) then
        return lower(replace(V_DATA_TYPE,'LOCAL TIME ZONE','TIME ZONE'));  
      end if;
      if ((strpos(V_DATA_TYPE,'INTERVAL') = 1) and (strpos(V_DATA_TYPE,'YEAR') > 0) and (strpos(V_DATA_TYPE,'TO MONTH') > 0)) then
        return 'interval year to month';
      end if;
      if (strpos(V_DATA_TYPE,'"."XMLTYPE"') > 0) then 
        return 'xml';
      end if;
      if (strpos(V_DATA_TYPE,'"."') > 0) then 
        -- Map all object types to text - Store the Oracle serialized format. 
        -- When Oracle Objects are mapped to JSON change the mapping to JSON.
        return 'text';
      end if;
  end case;
  return lower(V_DATA_TYPE);
end;
$$ LANGUAGE plpgsql;
--
create or replace function GENERATE_STATEMENTS(P_SCHEMA VARCHAR, P_TABLE_NAME VARCHAR, P_COLUMNS TEXT, P_DATA_TYPES TEXT, P_SIZE_CONSTRAINTS TEXT,P_BINARY_JSON BOOLEAN)
returns text[]
as $$
declare
  V_COLUMNS_CLAUSE     TEXT;
  V_INSERT_SELECT_LIST TEXT;
  V_COLUMN_COUNT       TEXT;
  V_STATEMENTS         TEXT[] := '{}';
begin

  with
  SOURCE_TABLE_DEFINITIONS
  as (
    select c.IDX
          ,c.VALUE COLUMN_NAME
          ,t.VALUE DATA_TYPE
          ,case
             when s.VALUE = ''
               then NULL
             when strpos(s.VALUE,',') > 0
               then SUBSTR(s.VALUE,1,strpos(s.VALUE,',')-1)
             else
               s.VALUE
            end DATA_TYPE_LENGTH
          ,case
             when strpos(s.VALUE,',') > 0
               then SUBSTR(s.VALUE, strpos(s.VALUE,',')+1)
              else
                NULL
           end DATA_TYPE_SCALE
      from JSON_ARRAY_ELEMENTS_TEXT(('[' || P_COLUMNS || ']')::json)  WITH ORDINALITY as c(VALUE, IDX)
          ,JSON_ARRAY_ELEMENTS_TEXT(('[' || REPLACE(P_DATA_TYPES,'"."','\".\"') || ']')::json) WITH ORDINALITY t(VALUE, IDX)
          ,JSON_ARRAY_ELEMENTS_TEXT(('[' || P_SIZE_CONSTRAINTS || ']')::json) WITH ORDINALITY s(VALUE, IDX)
     where (c.IDX = t.IDX) and (c.IDX = s.IDX)
  ),
  TARGET_TABLE_DEFINITIONS
  as (
    select std.*,
           MAP_FOREIGN_DATA_TYPE(DATA_TYPE,DATA_TYPE_LENGTH::INT,DATA_TYPE_SCALE::INT) TARGET_DATA_TYPE
      from SOURCE_TABLE_DEFINITIONS std
  ) 
  select STRING_AGG('"' || COLUMN_NAME || '" ' || TARGET_DATA_TYPE || 
                    case 
                      when TARGET_DATA_TYPE like '%(%)' 
                        then ''
                      when TARGET_DATA_TYPE in ('smallint', 'mediumint', 'int', 'bigint','real','text','bytea','integer','money','xml','json','jsonb','image')
                        then ''
                      when TARGET_DATA_TYPE like 'interval%'
                        then ''
                      when DATA_TYPE_LENGTH is NOT NULL and DATA_TYPE_SCALE IS NOT NULL
                        then '(' || DATA_TYPE_LENGTH || ',' || DATA_TYPE_SCALE || ')'
                      when DATA_TYPE_LENGTH is NOT NULL 
                        then '(' || DATA_TYPE_LENGTH  || ')'
                      else
                        ''
                    end
                   ,CHR(13) || '  ,'
                   ) COLUMNS_CLAUSE
        ,STRING_AGG('cast( value ->> ' || IDX-1 || ' as ' || TARGET_DATA_TYPE || ') "' || COLUMN_NAME || '"', CHR(13) || '  ,') INSERT_SELECT_LIST
        ,CAST(COUNT(*) AS TEXT) COLUMN_COUNT
    into V_COLUMNS_CLAUSE, V_INSERT_SELECT_LIST, V_COLUMN_COUNT
    from TARGET_TABLE_DEFINITIONS;

  V_STATEMENTS[1] := 'CREATE TABLE IF NOT EXISTS "' || P_SCHEMA || '"."' || P_TABLE_NAME || '"(' || CHR(13) || '   ' || V_COLUMNS_CLAUSE || CHR(13) || ')';        
  V_STATEMENTS[2] := 'INSERT into "' || P_SCHEMA || '"."' || P_TABLE_NAME || '"(' || P_COLUMNS || ')' || CHR(13) || 'select ' || V_INSERT_SELECT_LIST || CHR(13) || '  from ' || case WHEN P_BINARY_JSON then 'jsonb_array_elements' else 'json_array_elements' end || '($1 -> ''data'' -> ''' || P_TABLE_NAME || ''')';
  V_STATEMENTS[3] := V_COLUMN_COUNT;
  return V_STATEMENTS;
end;  
$$ LANGUAGE plpgsql;
--
create or replace function IMPORT_JSONB(P_JSON jsonb,P_SCHEMA VARCHAR) 
returns jsonb[]
as $$
declare
  R                  RECORD;
  V_RESULTS          JSONB[] = '{}';
  V_STATEMENTS       TEXT[];

  V_ROW_COUNT        INTEGER;
  V_START_TIME       TIMESTAMPTZ;
  V_END_TIME         TIMESTAMPTZ;
  V_ELAPSED_TIME     DOUBLE PRECISION;

  PLPGSQL_CTX        TEXT;
begin
  for r in select "owner", "tableName", "columns", "dataTypes","dataTypeSizing"
             from JSONB_EACH(P_JSON -> 'metadata')  CROSS JOIN LATERAL JSONB_TO_RECORD(value) as METADATA("owner" VARCHAR, "tableName" VARCHAR, "columns" TEXT, "dataTypes" TEXT, "dataTypeSizing" TEXT) loop
             
    V_STATEMENTS := GENERATE_STATEMENTS(P_SCHEMA,r."tableName",r."columns",r."dataTypes",r."dataTypeSizing",TRUE);
  
    begin
      EXECUTE V_STATEMENTS[1];
      V_RESULTS := array_append(V_RESULTS, jsonb_build_object('ddl', jsonb_build_object('tableName',r."tableName",'sqlStatement',V_STATEMENTS[1])));
    exception 
      when others then
        GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
        V_RESULTS := array_append(V_RESULTS, jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName',r."tableName",'sqlStatement',V_STATEMENTS[1],'code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)));
    end; 
  
    begin
      V_START_TIME := clock_timestamp();
      EXECUTE V_STATEMENTS[2]  using  P_JSON;
      V_END_TIME := clock_timestamp();
      GET DIAGNOSTICS V_ROW_COUNT := ROW_COUNT;
      V_ELAPSED_TIME := 1000 * ( extract(epoch from V_END_TIME) - extract(epoch from V_START_TIME) );
      V_RESULTS := array_append(V_RESULTS, jsonb_build_object('dml', jsonb_build_object('tableName',r."tableName",'rowCount',V_ROW_COUNT,'elapsedTime',V_ELAPSED_TIME,'sqlStatement',V_STATEMENTS[2])));
    exception
      when others then
        GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
        V_RESULTS := array_append(V_RESULTS, jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName',r."tableName",'sqlStatement',V_STATEMENTS[2],'code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)));
    end;

  end loop;
  return V_RESULTS;
exception
  when others then
    GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
    V_RESULTS := array_append(V_RESULTS, jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName','','sqlStatement','IMPORT_JSONB(P_JSON jsonb,P_SCHEMA VARCHAR)','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)));
    return V_RESULTS;
end;  
$$ LANGUAGE plpgsql;
--
create or replace function GENERATE_SQL(P_JSON json, P_SCHEMA VARCHAR)
returns SETOF jsonb
as $$
declare
begin
  RETURN QUERY
  select jsonb_object_agg("tableName",GENERATE_STATEMENTS(P_SCHEMA,"tableName","columns","dataTypes","dataTypeSizing",FALSE))
    from JSON_EACH(P_JSON -> 'metadata')  CROSS JOIN LATERAL JSON_TO_RECORD(value) as METADATA("owner" VARCHAR, "tableName" VARCHAR, "columns" TEXT, "dataTypes" TEXT, "dataTypeSizing" TEXT);
end;
$$ LANGUAGE plpgsql;
--
create or replace function IMPORT_JSON(P_JSON json,P_SCHEMA VARCHAR) 
returns jsonb[]
as $$
declare
  R                  RECORD;
  V_RESULTS          JSONB[] = '{}';
  V_STATEMENTS       TEXT[];

  V_ROW_COUNT        INTEGER;
  V_START_TIME       TIMESTAMPTZ;
  V_END_TIME         TIMESTAMPTZ;
  V_ELAPSED_TIME     DOUBLE PRECISION;

  PLPGSQL_CTX        TEXT;
begin
  for r in select "owner", "tableName", "columns", "dataTypes","dataTypeSizing"
             from JSON_EACH(P_JSON -> 'metadata')  CROSS JOIN LATERAL JSON_TO_RECORD(value) as METADATA("owner" VARCHAR, "tableName" VARCHAR, "columns" TEXT, "dataTypes" TEXT, "dataTypeSizing" TEXT) loop

    V_STATEMENTS := GENERATE_STATEMENTS(P_SCHEMA,r."tableName",r."columns",r."dataTypes",r."dataTypeSizing",FALSE);
  
    begin
      EXECUTE V_STATEMENTS[1];
      V_RESULTS := array_append(V_RESULTS, jsonb_build_object('ddl', jsonb_build_object('tableName',r."tableName",'sqlStatement',V_STATEMENTS[1])));
    exception 
      when others then
        GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
        V_RESULTS := array_append(V_RESULTS, jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName',r."tableName",'sqlStatement',V_STATEMENTS[1],'code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)));
    end; 
  
    begin
      V_START_TIME := clock_timestamp();
      EXECUTE V_STATEMENTS[2]  using  P_JSON;
      V_END_TIME := clock_timestamp();
      GET DIAGNOSTICS V_ROW_COUNT := ROW_COUNT;
      V_ELAPSED_TIME := 1000 * ( extract(epoch from V_END_TIME) - extract(epoch from V_START_TIME) );
      V_RESULTS := array_append(V_RESULTS, jsonb_build_object('dml', jsonb_build_object('tableName',r."tableName",'rowCount',V_ROW_COUNT,'elapsedTime',V_ELAPSED_TIME,'sqlStatement',V_STATEMENTS[2] )));
    exception
      when others then
        GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
        V_RESULTS := array_append(V_RESULTS, jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName',r."tableName",'sqlStatement',V_STATEMENTS[2] ,'code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)));
    end;

  end loop;
  return V_RESULTS;
exception
  when others then
    GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
    V_RESULTS := array_append(V_RESULTS, jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName','','sqlStatement','IMPORT_JSON(P_JSON json,P_SCHEMA VARCHAR)','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)));
    return V_RESULTS;
end;  
$$ LANGUAGE plpgsql;

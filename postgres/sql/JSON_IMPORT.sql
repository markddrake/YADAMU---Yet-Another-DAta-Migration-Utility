/*
**
** Postgress JSON_IMPORT Function.
**
*/
create or replace function MAP_FOREIGN_DATA_TYPE(P_SOURCE_VENDOR  VARCHAR, P_DATA_TYPE VARCHAR, P_DATA_TYPE_LENGTH INT, P_DATA_TYPE_SCALE INT) 
returns VARCHAR
as $$
declare
  V_DATA_TYPE  VARCHAR(128);
begin
  V_DATA_TYPE := P_DATA_TYPE;

  case P_SOURCE_VENDOR 
    when 'Oracle' then
      case V_DATA_TYPE
        when 'VARCHAR2' then
          return 'varchar';
        when 'NUMBER' then
           return 'numeric';
        when 'NVARCHAR2' then
           return 'varchar';
        when 'RAW' then
           return 'bytea';
        when 'BLOB' then
           return 'bytea';
        when 'CLOB' then
           return 'text';
        when 'NCLOB' then
           return 'text';
        when 'BFILE' then
           return 'varchar';
        when 'ROWID' then
           return 'varchar';
        when 'ANYDATA' then
           return 'text';
        when 'XMLTYPE'then
           return 'xml';
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
          if (V_DATA_TYPE like '"%"."%"') then
             -- Map all object types to text - Store the Oracle serialized format. 
             -- When Oracle Objects are mapped to JSON change the mapping to JSON.
             return 'text';
          end if;
          return lower(V_DATA_TYPE);
      end case;
    when 'MySQL' then
      -- Also MariaDB ????
      case V_DATA_TYPE
        -- MySQL Direct Mappings
        when 'binary' then
           return 'bytea';
        when 'bit' then
           return 'boolean';
        when 'datetime' then
           return 'timestamp';
        when 'double' then
           return 'double precision';
        when  'enum' then
           return 'varchar(255)';   
        when 'float' then
           return 'real';
        when 'geometry' then
           return 'jsonb';
        when 'geography' then
           return 'jsonb';
        when 'tinyint' then
           return 'smallint';
        when 'mediumint' then
           return 'integer';
        when 'tinyblob' then
           return 'bytea';
        when 'blob' then
           return 'bytea';
        when 'mediumblob' then
           return 'bytea';
        when 'longblob' then
           return 'bytea';
        when 'set' then
           return 'varchar(255)';   
        when 'tinyint' then
           return 'smallint';
        when 'tinytext' then
           return 'text';
        when 'text' then
           return 'text';
         when 'mediumtext' then
           return 'text';
        when 'longtext' then
           return 'text';
        when 'varbinary' then
           return 'bytea';
        when 'year' then
           return 'smallint';
        else
          return lower(V_DATA_TYPE);
      end case;
    when 'MSSQLSERVER'  then 
      case V_DATA_TYPE         
        -- MSSQL Direct Mappings
        when 'datetime' then
          return 'timestamp';
        when 'datetime2' then
          return 'timestamp';
        when 'datetimeoffset' then 
          return 'timestamp with time zone';
        when 'image'then 
          return 'bytea';
        when 'nchar'then
          return 'char';
        when 'ntext' then 
          return'text';
        when 'nvarchar' then 
          case P_DATA_TYPE_LENGTH 
             when -1 
               then return 'text';
             else
               return 'varchar'; 
          end case;
        when 'varchar' then
          case P_DATA_TYPE_LENGTH 
            when -1 
              then return 'text';
            else
              return 'varchar';
          end case;
        when 'rowversion' then
          return 'bytea';
        when 'smalldatetime' then
          return'timestamp(0)';
        when 'smallmoney' then
          return'money';
        when 'tinyint' then
          return 'smallint';
        when 'hierarchyid' then
           return 'varchar(4000)';
        when 'uniqueidentifier' then
          return 'varchar(36)';
        when 'varbinary' then
          return 'bytea';
        when 'geography' then
          return 'jsonb';
        else
          return lower(V_DATA_TYPE);
      end case;
    else 
      return lower(V_DATA_TYPE);
  end case;
end;
$$ LANGUAGE plpgsql;
--
create or replace function GENERATE_STATEMENTS(P_SOURCE_VENDOR VARCHAR, P_SCHEMA VARCHAR, P_TABLE_NAME VARCHAR, P_COLUMNS TEXT, P_DATA_TYPES JSONB, P_SIZE_CONSTRAINTS JSONB,P_BINARY_JSON BOOLEAN)
returns JSONB
as $$
declare
  V_COLUMNS_CLAUSE     TEXT;
  V_INSERT_SELECT_LIST TEXT;
  V_COLUMN_COUNT       TEXT;
  V_TARGET_DATA_TYPES  JSONB;
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
          ,JSONB_ARRAY_ELEMENTS_TEXT(P_DATA_TYPES) WITH ORDINALITY t(VALUE, IDX)
          ,JSONB_ARRAY_ELEMENTS_TEXT(P_SIZE_CONSTRAINTS) WITH ORDINALITY s(VALUE, IDX)
     where (c.IDX = t.IDX) and (c.IDX = s.IDX)
  ),
  TARGET_TABLE_DEFINITIONS
  as (
    select st.*,
           MAP_FOREIGN_DATA_TYPE(P_SOURCE_VENDOR,DATA_TYPE,DATA_TYPE_LENGTH::INT,DATA_TYPE_SCALE::INT) TARGET_DATA_TYPE
      from SOURCE_TABLE_DEFINITIONS st
  ) 
  select STRING_AGG('"' || COLUMN_NAME || '" ' || TARGET_DATA_TYPE || 
                    case 
                      when TARGET_DATA_TYPE like '%(%)' 
                        then ''
                      when TARGET_DATA_TYPE in ('smallint', 'mediumint', 'int', 'bigint','real','text','bytea','integer','money','xml','json','jsonb','image','date','double precision')
                        then ''
                      when (TARGET_DATA_TYPE = 'time' and DATA_TYPE_LENGTH::INT > 6)
                        then '(6)'
                      when TARGET_DATA_TYPE like 'interval%'
                        then ''
                      when DATA_TYPE_LENGTH is NOT NULL and DATA_TYPE_SCALE IS NOT NULL
                        then '(' || DATA_TYPE_LENGTH || ',' || DATA_TYPE_SCALE || ')'
                      when DATA_TYPE_LENGTH is NOT NULL 
                        then '(' || DATA_TYPE_LENGTH  || ')'
                      else
                        ''
                    end
                   ,CHR(10) || '  ,'
                   ) COLUMNS_CLAUSE
        ,STRING_AGG(case 
                      when TARGET_DATA_TYPE = 'time' then  
                       'cast( value ->> ' || IDX-1 || ' as timestamp)::' || TARGET_DATA_TYPE
                      when TARGET_DATA_TYPE = 'bit' then  
                        'case when value ->> ' || IDX-1 || ' = ''true'' then B''1'' when value ->> ' || IDX-1 || ' = ''false''  then B''0'' else cast( value ->> ' || IDX-1 || ' as ' || TARGET_DATA_TYPE || ') end'
                      else
                       'cast( value ->> ' || IDX-1 || ' as ' || TARGET_DATA_TYPE || ')'
                    end 
                    || ' "' || COLUMN_NAME || '"', CHR(10) || '  ,'
                   ) INSERT_SELECT_LIST
        ,JSONB_AGG(TARGET_DATA_TYPE) TARGET_DATA_TYPES
    into V_COLUMNS_CLAUSE, V_INSERT_SELECT_LIST, V_TARGET_DATA_TYPES
    from TARGET_TABLE_DEFINITIONS;

  return JSONB_BUILD_OBJECT(
                    'ddl', 'CREATE TABLE IF NOT EXISTS "' || P_SCHEMA || '"."' || P_TABLE_NAME || '"(' || CHR(10) || '   ' || V_COLUMNS_CLAUSE || CHR(10) || ')',       
                    'dml', 'INSERT into "' || P_SCHEMA || '"."' || P_TABLE_NAME || '"(' || P_COLUMNS || ')' || CHR(10) || 'select ' || V_INSERT_SELECT_LIST || CHR(10) || '  from ' || case WHEN P_BINARY_JSON then 'jsonb_array_elements' else 'json_array_elements' end || '($1 -> ''data'' -> ''' || P_TABLE_NAME || ''')',
                    'targetDataTypes', V_TARGET_DATA_TYPES 
               );
end;  
$$ LANGUAGE plpgsql;
--
create or replace function IMPORT_JSONB(P_JSON jsonb,P_SCHEMA VARCHAR) 
returns JSONB
as $$
declare
  R                  RECORD;
  V_RESULTS          JSONB = '[]';
  V_ROW_COUNT        INTEGER;
  V_START_TIME       TIMESTAMPTZ;
  V_END_TIME         TIMESTAMPTZ;
  V_ELAPSED_TIME     DOUBLE PRECISION;

  PLPGSQL_CTX        TEXT;
begin
  for r in select "tableName"
                 ,GENERATE_STATEMENTS(P_JSON #> '{systemInformation}' ->> 'vendor',P_SCHEMA,"tableName","columns","dataTypes","sizeConstraints",TRUE) "TABLE_INFO"
             from JSONB_EACH(P_JSON -> 'metadata')  
                  CROSS JOIN LATERAL JSONB_TO_RECORD(value) as METADATA(
                                                                 "owner" VARCHAR, 
                                                                 "tableName" VARCHAR, 
                                                                 "columns" TEXT, 
                                                                 "dataTypes" JSONB, 
                                                                 "sizeConstraints" JSONB
                                                              ) 
  loop
                                                              
    begin
      EXECUTE r."TABLE_INFO" ->> 'ddl';
      V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('ddl', jsonb_build_object('tableName',r."tableName",'sqlStatement',r."TABLE_INFO"->>'ddl')), true);
    exception 
      when others then
        GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
        V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName',r."tableName",'sqlStatement',r."TABLE_INFO"->>'ddl','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    end; 
  
    begin
      V_START_TIME := clock_timestamp();
      EXECUTE r."TABLE_INFO" ->> 'dml'  using  P_JSON;
      V_END_TIME := clock_timestamp();
      GET DIAGNOSTICS V_ROW_COUNT := ROW_COUNT;
      V_ELAPSED_TIME := 1000 * ( extract(epoch from V_END_TIME) - extract(epoch from V_START_TIME) );
      V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS)  || '}' as TEXT[]), jsonb_build_object('dml', jsonb_build_object('tableName',r."tableName",'rowCount',V_ROW_COUNT,'elapsedTime',V_ELAPSED_TIME,'sqlStatement',r."TABLE_INFO"->>'dml')), true);
    exception
      when others then
        GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
        V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName',r."tableName",'sqlStatement',r."TABLE_INFO"->>'dml','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    end;

  end loop;
  return V_RESULTS;
exception
  when others then
    GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
    V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName','','sqlStatement','call IMPORT_JSONB(P_JSON jsonb,P_SCHEMA VARCHAR)','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    return V_RESULTS;
end;  
$$ LANGUAGE plpgsql;
--
create or replace function IMPORT_JSON(P_JSON json,P_SCHEMA VARCHAR) 
returns JSONB
as $$
declare
  R                  RECORD;
  V_RESULTS          JSONB = '[]';
  V_ROW_COUNT        INTEGER;
  V_START_TIME       TIMESTAMPTZ;
  V_END_TIME         TIMESTAMPTZ;
  V_ELAPSED_TIME     DOUBLE PRECISION;

  PLPGSQL_CTX        TEXT;
begin

  for r in select "tableName"
                 ,GENERATE_STATEMENTS(P_JSON #> '{systemInformation}' ->> 'vendor',P_SCHEMA,"tableName","columns","dataTypes","sizeConstraints",FALSE) "TABLE_INFO"
             from JSON_EACH(P_JSON -> 'metadata')  
                  CROSS JOIN LATERAL JSON_TO_RECORD(value) as METADATA(
                                                                "owner" VARCHAR, 
                                                                "tableName" VARCHAR, 
                                                                "columns" TEXT, 
                                                                "dataTypes" JSONB, 
                                                                "sizeConstraints" JSONB
                                                             ) 
  loop
  
    begin
      EXECUTE r."TABLE_INFO" ->> 'ddl';
      V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('ddl', jsonb_build_object('tableName',r."tableName",'sqlStatement',r."TABLE_INFO" ->> 'ddl')), true);
    exception 
      when others then
        GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
        V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName',r."tableName",'sqlStatement',r."TABLE_INFO" ->> 'ddl','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    end; 
  
    begin
      V_START_TIME := clock_timestamp();
      EXECUTE r."TABLE_INFO" ->> 'dml'  using  P_JSON;
      V_END_TIME := clock_timestamp();
      GET DIAGNOSTICS V_ROW_COUNT := ROW_COUNT;
      V_ELAPSED_TIME := 1000 * ( extract(epoch from V_END_TIME) - extract(epoch from V_START_TIME) );
      V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS)  || '}' as TEXT[]),  jsonb_build_object('dml', jsonb_build_object('tableName',r."tableName",'rowCount',V_ROW_COUNT,'elapsedTime',V_ELAPSED_TIME,'sqlStatement',r."TABLE_INFO" ->> 'dml' )), true);
    exception
      when others then
        GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
        V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName',r."tableName",'sqlStatement',r."TABLE_INFO" ->> 'dml' ,'code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    end;

  end loop;
  return V_RESULTS;
exception
  when others then
    GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
    V_RESULTS := jsonb_insert(V_RESULTS, CAST('{' || jsonb_array_length(V_RESULTS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName','','sqlStatement','call IMPORT_JSON(P_JSON json,P_SCHEMA VARCHAR)','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    return V_RESULTS;
end;  
$$ LANGUAGE plpgsql;
---
create or replace function GENERATE_SQL(P_JSON jsonb, P_SCHEMA VARCHAR)
returns SETOF jsonb
as $$
declare
begin
  RETURN QUERY
  select jsonb_object_agg(
           "tableName",
           GENERATE_STATEMENTS(P_JSON #> '{systemInformation}' ->> 'vendor',P_SCHEMA,"tableName","columns","dataTypes","sizeConstraints",FALSE)
         )
    from JSONB_EACH(P_JSON -> 'metadata')  
         CROSS JOIN LATERAL JSONB_TO_RECORD(value) as METADATA(
                                                       "owner" VARCHAR, 
                                                       "tableName" VARCHAR, 
                                                       "columns" TEXT, 
                                                       "dataTypes" JSONB, 
                                                       "sizeConstraints" JSONB
                                                     );
end;
$$ LANGUAGE plpgsql;
--
create or replace procedure COMPARE_SCHEMA(P_SOURCE_SCHEMA VARCHAR,P_TARGET_SCHEMA VARCHAR)
as $$
declare
  R RECORD;
  V_SQL_STATEMENT TEXT;
  C_NEWLINE CHAR(1) = CHR(10);
  
  V_SOURCE_COUNT INT;
  V_TARGET_COUNT INT;
  V_ERROR        TEXT;
begin
  create temporary table if not exists SCHEMA_COMPARE_RESULTS (
    SOURCE_SCHEMA    VARCHAR(128)
   ,TARGET_SCHEMA    VARCHAR(128)
   ,TABLE_NAME       VARCHAR(128)
   ,SOURCE_ROW_COUNT INT
   ,TARGET_ROW_COUNT INT
   ,MISSINGS_ROWS    INT
   ,EXTRA_ROWS       INT
   ,ERROR            TEXT
  );
  
  for r in select t.table_name
	             ,string_agg(
                    case 
                      when data_type in ('json','xml')  then
                        '"' || column_name || '"::text' 
                      else 
                        '"' || column_name || '"' 
                    end
                   ,',' 
                   order by ordinal_position
                  ) COLUMN_LIST
             from information_schema.columns c, information_schema.tables t
            where t.table_name = c.table_name 
              and t.table_schema = c.table_schema
	          and t.table_type = 'BASE TABLE'
              and t.table_schema = P_SOURCE_SCHEMA
            group by t.table_schema, t.table_name 
  loop
    begin
      V_SQL_STATEMENT = 'insert into SCHEMA_COMPARE_RESULTS ' || C_NEWLINE
                      || ' select ''' || P_SOURCE_SCHEMA  || ''' ' || C_NEWLINE
                      || '       ,''' || P_TARGET_SCHEMA  || ''' ' || C_NEWLINE
                      || '       ,'''  || r.TABLE_NAME || ''' ' || C_NEWLINE
                      || '       ,(select count(*) from "' || P_SOURCE_SCHEMA  || '"."' || r.TABLE_NAME || '")'  || C_NEWLINE
                      || '       ,(select count(*) from "' || P_TARGET_SCHEMA  || '"."' || r.TABLE_NAME || '")'  || C_NEWLINE
                      || '       ,(select count(*) from (SELECT ' || r.COLUMN_LIST || ' from "' || P_SOURCE_SCHEMA  || '"."' || r.TABLE_NAME || '" EXCEPT SELECT ' || r.COLUMN_LIST || ' from  "' || P_TARGET_SCHEMA  || '"."' || r.TABLE_NAME || '") T1) '  || C_NEWLINE
                      || '       ,(select count(*) from (SELECT ' || r.COLUMN_LIST || ' from "' || P_TARGET_SCHEMA  || '"."' || r.TABLE_NAME || '" EXCEPT SELECT ' || r.COLUMN_LIST || ' from  "' || P_SOURCE_SCHEMA  || '"."' || r.TABLE_NAME || '") T1) '  || C_NEWLINE
                      || '       ,NULL';
      EXECUTE V_SQL_STATEMENT;               
    exception  
      when others then
        V_ERROR = SQLERRM;
        V_SOURCE_COUNT = -1;
        V_TARGET_COUNT = -1;

        begin 
          EXECUTE 'select count(*) from "' || P_SOURCE_SCHEMA  || '"."' || r.TABLE_NAME || '"' into V_SOURCE_COUNT;
        exception 
          when others then
            null;
        end;
         
        begin 
          EXECUTE 'select count(*) from "' || P_TARGET_SCHEMA  || '"."' || r.TABLE_NAME || '"' into V_TARGET_COUNT;
        exception 
          when others then
            null;
        end;
		
        insert into SCHEMA_COMPARE_RESULTS VALUES (P_SOURCE_SCHEMA, P_TARGET_SCHEMA, r.TABLE_NAME, V_SOURCE_COUNT, V_TARGET_COUNT, -1, -1, V_ERROR);            
    end;               
                    
  end loop;
end;
$$ LANGUAGE plpgsql;
--
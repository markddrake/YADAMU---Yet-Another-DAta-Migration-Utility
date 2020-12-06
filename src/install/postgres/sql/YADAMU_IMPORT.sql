/*
**
** Drop functions regardless of signatute
**
*/
do 
$$
declare
   _count numeric;
   _sql text;
begin
   SELECT count(*)::int
        , 'DROP FUNCTION ' || string_agg(oid::regprocedure::text, '; DROP FUNCTION ')
   FROM   pg_proc
   WHERE  UPPER(proname) in ('EXPORT_JSON','MAP_FOREIGN_DATA_TYPE','GENERATE_STATEMENTS','IMPORT_JSON','IMPORT_JSONB','GENERATE_SQL')
   INTO   _count, _sql;  -- only returned if trailing DROPs succeed

   if _count > 0 then
     execute _sql;
   end if;
end
$$ 
language plpgsql;
/*
**
** Postgress EXPORT_JSON Function.
**
*/
--
create or replace function EXPORT_JSON(P_SCHEMA VARCHAR, P_SPATIAL_FORMAT VARCHAR) 
returns TABLE ( TABLE_SCHEMA VARCHAR, TABLE_NAME VARCHAR, COLUMN_NAME_ARRAY JSONB, DATA_TYPE_ARRAY JSONB, SIZE_CONSTRAINT_ARRAY JSONB, CLIENT_SELECT_LIST TEXT, ERRORS JSONB)
as $$
declare
  R                  RECORD;
  V_SQL_STATEMENT    TEXT = NULL;
  PLPGSQL_CTX        TEXT;
  
  V_SIZE_CONSTRAINTS JSONB;
begin

  for r in select t.table_schema "TABLE_SCHEMA"
                 ,t.table_name "TABLE_NAME"
	             ,jsonb_agg(column_name order by ordinal_position) "COLUMN_NAME_ARRAY"
	             ,jsonb_agg(case 
                              when data_type = 'USER-DEFINED' then
                                udt_name 
                              when data_type = 'interval' then
                                data_type || ' ' || lower(interval_type) 
                              else 
                                data_type 
                            end 
                            order by ordinal_position
                           ) "DATA_TYPE_ARRAY"
                 ,jsonb_agg(case
                              when (numeric_precision is not null) and (numeric_scale is not null) then
                                cast(numeric_precision as varchar) || ',' || cast(numeric_scale as varchar)
                              when (numeric_precision is not null) then 
                                cast(numeric_precision as varchar)
                              when (character_maximum_length is not null) then 
                                cast(character_maximum_length as varchar)
                              when (datetime_precision is not null) then 
                                cast(datetime_precision as varchar)
                            end
                            order by ordinal_position
                          ) "SIZE_CONSTRAINT_ARRAY"
	             ,string_agg(case 
                               when data_type = 'xml' then
                                  '"' || column_name || '"' || '::text'
                               when ((data_type = 'USER-DEFINED') and (udt_name in ('geometry','geography'))) then
                                  case 
                                    when P_SPATIAL_FORMAT = 'WKB' then
                                      'ST_AsBinary("' || column_name || '") "' || COLUMN_NAME || '"'
                                    when P_SPATIAL_FORMAT = 'WKT' then
                                      'ST_AsText("' || column_name || '",18) "' || COLUMN_NAME || '"'
                                    when P_SPATIAL_FORMAT = 'EWKB' then
                                      'ST_AsEWKB("' || column_name || '") "' || COLUMN_NAME || '"'
                                    when P_SPATIAL_FORMAT = 'EWKT' then
                                      'ST_AsEWKT("' || column_name || '") "' || COLUMN_NAME || '"'
                                    when P_SPATIAL_FORMAT = 'GeoJSON' then
                                      'ST_AsGeoJSON("' || column_name || '") "' || COLUMN_NAME || '"'
                                    else
                                      'ST_AsEWKB("' || column_name || '")) "' || COLUMN_NAME || '"'
                                  end
                               when data_type like 'interval%' then
                                  '"' || COLUMN_NAME ||'"::varchar "' || COLUMN_NAME || '"'
                               when data_type in ('date','timestamp without time zone') then
                                  'to_char ("' || COLUMN_NAME ||'" at time zone ''UTC'', ''YYYY-MM-DD"T"HH24:MI:SS.US"Z"'') "' || COLUMN_NAME || '"'
                               when data_type in ('money') then
                                 /* Suppress printing Currency Symbols */
                                 /* then '("' || column_name || '"/1::money) */
                                 '("' || column_name || '"::numeric) "' || COLUMN_NAME || '"'
                               when data_type in ('time', 'time with time zone','time without time zone') then 
                                 /*  '''1971-01-01T'' || to_char("' || COLUMN_NAME || '",''HH24:MI:SS.US"Z"'')  "' || COLUMN_NAME || '"' */
                                 '''1971-01-01T'' || CAST("' || COLUMN_NAME || '" as CHARACTER VARYING(16))  "' || COLUMN_NAME || '"'
                                when data_type like 'timestamp%' then 
                                  'to_char ("' || COLUMN_NAME ||'" at time zone ''UTC'', ''YYYY-MM-DD"T"HH24:MI:SS.US"Z"'') "' || COLUMN_NAME || '"'
                               else
                                 '"' || column_name || '"'
                             end,
			                 ',' order by ordinal_position
                               ) "CLIENT_SELECT_LIST"
            from information_schema.columns c, information_schema.tables t
           where t.table_name = c.table_name 
	         and t.table_schema = c.table_schema
	         and t.table_type = 'BASE TABLE'
             and t.table_schema = P_SCHEMA
        group by t.table_schema, t.table_name
    
  loop

    /*
    **
    ** Calculate the max length of bytea fields, since Postgres does not maintain a maximum length in the dictionary.
    **
    */
  
    select 'select jsonb_build_array(' 
        || string_agg(
             case
               when value = 'bytea' 
                 then 'to_char(max(octet_length("' || (r."COLUMN_NAME_ARRAY" ->> cast((idx -1) as int)) || '")),''FM999999999999999999'')'
               else
                 '''' || COALESCE ((r."SIZE_CONSTRAINT_ARRAY" ->> cast((idx -1) as int)),'') || ''''
             end
            ,','
           ) 
        || ') FROM "' || r."TABLE_SCHEMA" || '"."' || r."TABLE_NAME" || '"'
      from  jsonb_array_elements_text(r."DATA_TYPE_ARRAY") WITH ORDINALITY as c(value, idx)
      into V_SQL_STATEMENT
     where r."DATA_TYPE_ARRAY" ? 'bytea';

    if (V_SQL_STATEMENT is not NULL) then
      execute V_SQL_STATEMENT into V_SIZE_CONSTRAINTS;
    else
      V_SIZE_CONSTRAINTS := r."SIZE_CONSTRAINT_ARRAY";
    end if;

    TABLE_SCHEMA          := r."TABLE_SCHEMA";
    TABLE_NAME            := r."TABLE_NAME";
    COLUMN_NAME_ARRAY     := r."COLUMN_NAME_ARRAY";
    DATA_TYPE_ARRAY       := r."DATA_TYPE_ARRAY";
    SIZE_CONSTRAINT_ARRAY := V_SIZE_CONSTRAINTS;
    CLIENT_SELECT_LIST    := r."CLIENT_SELECT_LIST";
    return NEXT;
  end loop;
  return;
exception
  when others then
    GET STACKED DIAGNOSTICS PLPGSQL_CTX = PG_EXCEPTION_CONTEXT;
    ERRORS := '[]';
    ERRORS := jsonb_insert(ERRORS, CAST('{' || jsonb_array_length(ERRORS) || '}' as TEXT[]), jsonb_build_object('error', jsonb_build_object('severity','FATAL','tableName','','sqlStatement','call EXPORT_JSON(P_SCHEMA VARCHAR)','code',SQLSTATE,'msg',SQLERRM,'details',PLPGSQL_CTX)), true);
    return NEXT;
    return;
end;  
$$ LANGUAGE plpgsql;
--
/*
**
** Postgress IMPORT_JSON Function.
**
*/
--
/*
**
** TODO: Add support for specifying whether to map JSON to JSON or JSONB
**
*/
create or replace function MAP_FOREIGN_DATA_TYPE(P_SOURCE_VENDOR  VARCHAR, P_DATA_TYPE VARCHAR, P_DATA_TYPE_LENGTH BIGINT, P_DATA_TYPE_SCALE INT, P_GEOMETRY_TYPE VARCHAR) 
returns VARCHAR
as $$
declare
  V_DATA_TYPE                    VARCHAR(128);
  C_JSON_TYPE                    VARCHAR(5)  = 'jsonb';
  C_CHARACTER_VARYING_MAX_LENGTH INT         = 10 * 1024 * 1024;
  C_CHARACTER_VARYING_MAX        VARCHAR(32) = 'character varying(' || C_CHARACTER_VARYING_MAX_LENGTH || ')';
begin
  V_DATA_TYPE := P_DATA_TYPE;

  case P_SOURCE_VENDOR 
    when 'Postgres' then
      case V_DATA_TYPE 
        when 'timestamp with time zone' then
          return 'timestamp(' || P_DATA_TYPE_LENGTH || ') with time zone';
        when 'timestamp without time zone' then
          return 'timestamp(' || P_DATA_TYPE_LENGTH || ') without time zone';
        when 'time with time zone' then
          return 'time(' || P_DATA_TYPE_LENGTH || ') with time zone';
        when 'time without time zone' then
          return 'time(' || P_DATA_TYPE_LENGTH || ') without time zone';
        else
          return lower(V_DATA_TYPE);
      end case;
    when 'Oracle' then
      case V_DATA_TYPE
        when 'VARCHAR2' then
          return 'character varying';
        when 'NUMBER' then
           return 'numeric';
        when 'BINARY_FLOAT' then
           return 'float4';
        when 'BINARY_DOUBLE' then
           return 'float8';
        when 'NVARCHAR2' then
           return 'character varying';
        when 'RAW' then
           return 'bytea';
        when 'BLOB' then
           return 'bytea';
        when 'CLOB' then
           return C_CHARACTER_VARYING_MAX;
        when 'NCLOB' then
           return C_CHARACTER_VARYING_MAX;
        when 'TIMESTAMP' then
          case
            when P_DATA_TYPE_LENGTH > 6 then 
			  return 'timestamp(6)';
            else 
              return 'timestamp';
          end case;
        when 'BFILE' then
           return 'character varying';
        when 'ROWID' then
           return 'character varying';
        when 'ANYDATA' then
           return C_CHARACTER_VARYING_MAX;
        when 'XMLTYPE'then
           return 'xml';
        when '"MDSYS"."SDO_GEOMETRY"' then
           return P_GEOMETRY_TYPE;
        else
          -- Oracle complex mappings
          if (strpos(V_DATA_TYPE,'LOCAL TIME ZONE') > 0) then
            return lower(replace(V_DATA_TYPE,'LOCAL TIME ZONE','TIME ZONE'));  
          end if;
          if ((strpos(V_DATA_TYPE,'INTERVAL') = 1) and (strpos(V_DATA_TYPE,'YEAR') > 0) and (strpos(V_DATA_TYPE,'TO MONTH') > 0)) then
            return 'interval year to month';
          end if;
          if ((strpos(V_DATA_TYPE,'INTERVAL') = 1) and (strpos(V_DATA_TYPE,'DAY') > 0) and (strpos(V_DATA_TYPE,'TO SECOND') > 0)) then
            return 'interval day to second';
          end if;
          if (strpos(V_DATA_TYPE,'"."XMLTYPE"') > 0) then 
            return 'xml';
          end if;
          if (V_DATA_TYPE like '"%"."%"') then
             -- Map all object types to text - Store the Oracle serialized format. 
             -- When Oracle Objects are mapped to JSON change the mapping to JSON.
             return C_CHARACTER_VARYING_MAX;
          end if;
          return lower(V_DATA_TYPE);
      end case;
    when  'MySQL' then
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
           return P_GEOMETRY_TYPE;
        when 'geography' then
           return P_GEOMETRY_TYPE;
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
           return 'jsonb';   
        when 'tinyint' then
           return 'smallint';
        when 'tinytext' then
           return 'character varying(256)';
        when 'text' then
           return 'character varying (65536)';
         when 'mediumtext' then
           return C_CHARACTER_VARYING_MAX;
        when 'longtext' then
           return C_CHARACTER_VARYING_MAX;
        when 'varbinary' then
           return 'bytea';
        when 'year' then
           return 'smallint';
        else
          return lower(V_DATA_TYPE);
      end case;
    when  'MariaDB' then
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
           return P_GEOMETRY_TYPE;
        when 'geography' then
           return P_GEOMETRY_TYPE;
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
           return 'jsonb';   
        when 'tinyint' then
           return 'smallint';
        when 'tinytext' then
           return 'character varying (256)';
        when 'text' then
           return 'character varying (65536)';
         when 'mediumtext' then
           return C_CHARACTER_VARYING_MAX;
        when 'longtext' then
           return C_CHARACTER_VARYING_MAX;
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
        when 'bit' then
           return 'boolean';
        when 'datetime' then
          case
            when P_DATA_TYPE_LENGTH > 6 then 
			  return 'timestamp(6)';
            else 
              return 'timestamp';
          end case;
        when 'datetime2' then
          case
            when P_DATA_TYPE_LENGTH > 6 
              then return 'timestamp(6)';
            else 
              return 'timestamp';
          end case;
        when 'datetimeoffset' then 
          case
            when P_DATA_TYPE_LENGTH > 6 then 
			  return 'timestamp(6) with time zone';
            else 
              return 'timestamp with time zone';
          end case;
        when 'image'then 
          return 'bytea';
        when 'nchar'then
          return 'char';
        when 'ntext' then 
          return C_CHARACTER_VARYING_MAX;
        when 'nvarchar' then 
          case 
		    when P_DATA_TYPE_LENGTH = -1 then
			  return C_CHARACTER_VARYING_MAX;
            else
              return 'character varying'; 
          end case;
        when 'varchar' then
          case 
		    when P_DATA_TYPE_LENGTH = -1 then
			  return C_CHARACTER_VARYING_MAX;
            else
              return 'character varying'; 
          end case;
        when 'rowversion' then
          return 'bytea';
        when 'smalldatetime' then
          return'timestamp(0)';
        -- Do not use Postgres Money Type due to precision issues. E.G. with Locale USD Postgres only provides 2 digit precison.
        when 'money' then
          return 'numeric(19,4)';
        when 'smallmoney' then
          return 'numeric(10,4)';
        when 'tinyint' then
          return 'smallint';
        when 'hierarchyid' then
           return 'varchar(4000)';
        when 'uniqueidentifier' then
          return 'varchar(36)';
        when 'varbinary' then
          return 'bytea';
        when 'geometry' then
           return P_GEOMETRY_TYPE;
           -- return 'jsonb';
        when 'geography' then
           return P_GEOMETRY_TYPE;
           -- return 'jsonb';
        else
          return lower(V_DATA_TYPE);
      end case;
	when 'MongoDB' then
      -- MongoDB typing based on BSON type model and the aggregation $type operator
      -- ### No support for depricated Data types undefined, dbPointer, symbol
      case V_DATA_TYPE
        when 'double' then
           return 'double precision';
        when 'string' then
          case
		    when P_DATA_TYPE_LENGTH >  C_CHARACTER_VARYING_MAX_LENGTH then
			  return C_CHARACTER_VARYING_MAX;
			else
  			  return 'character varying';            
          end case;
        when 'object' then
		  return C_JSON_TYPE;
        when 'array' then
		  return C_JSON_TYPE;
        when 'binData' then
		  return 'bytea';
		when 'objectId' then
	      return 'bytea';
        when 'boolean' then
           return 'bool';
        when 'null' then
           return 'character varying(2048)';
        when 'regex' then
           return 'character varying(4000)';
        when 'javascript' then
           return C_CHARACTER_VARYING_MAX;
        when 'javascriptWithScope' then
           return C_CHARACTER_VARYING_MAX;
        when 'int' then
           return 'int';
        when 'long' then
           return 'bigint';
        when 'decimal' then
           return 'numeric';
        when 'date' then
           return 'timestamp';
        when 'timestamp' then
           return 'timestamp';
        when 'minkey' then
		  return C_JSON_TYPE;
        when 'maxkey' then
		  return C_JSON_TYPE;
        else 
           return lower(P_DATA_TYPE);
      end case;    
    when 'SNOWFLAKE' then
      case V_DATA_TYPE
        when 'TEXT' then
		  case
		    when P_DATA_TYPE_LENGTH >  C_CHARACTER_VARYING_MAX_LENGTH then
			  return C_CHARACTER_VARYING_MAX;
			else
  			  return 'character varying';            
		  end case;
        when 'NUMBER' then
           return 'numeric';
        when 'BINARY' then
           return 'bytea';
        when 'XML' then
           return 'xml';
        when 'TIME' then
          case
            when P_DATA_TYPE_LENGTH > 6 then 
              return 'time(6)';
            else 
              return 'time';
          end case;
        when 'TIMESTAMP_LTZ' then
          case
            when P_DATA_TYPE_LENGTH > 6 then 
              return 'timestamp(6) with time zone';
            else 
              return 'timestamp(' || P_DATA_TYPE_LENGTH || ') without time zone';
          end case;
        when 'TIMESTAMP_NTZ' then
          case
            when P_DATA_TYPE_LENGTH > 6 then 
              return 'timestamp(6) with time zone';
            else 
              return 'timestamp(' || P_DATA_TYPE_LENGTH || ') without time zone';
          end case;
        when 'VARIANT'  then
           return 'bytea';
        else 
           return lower(P_DATA_TYPE);
      end case;	
    else 
      return lower(V_DATA_TYPE);
  end case;
end;
$$ LANGUAGE plpgsql;
--
create or replace function GENERATE_STATEMENTS(P_SOURCE_VENDOR VARCHAR, P_SCHEMA VARCHAR, P_TABLE_NAME VARCHAR, P_SPATIAL_FORMAT VARCHAR, P_COLUMN_NAME_ARRAY JSONB, P_DATA_TYPE_ARRAY JSONB, P_SIZE_CONSTRAINT_ARRAY JSONB, P_BINARY_JSON BOOLEAN)
returns JSONB
as $$
declare
  V_COLUMN_LIST        TEXT;
  V_COLUMNS_CLAUSE     TEXT;
  V_INSERT_SELECT_LIST TEXT;
  V_TARGET_DATA_TYPES  JSONB;
  V_GEOMETRY_TYPE      VARCHAR(32);
  V_POSTGIS_VERSION    VARCHAR(512);
begin

  begin
    SELECT PostGIS_full_version() into V_POSTGIS_VERSION;
    V_GEOMETRY_TYPE := 'geography';
  exception
    when undefined_function then
      V_GEOMETRY_TYPE := 'text';
    when others then
      RAISE;
  end;

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
      from JSONB_ARRAY_ELEMENTS_TEXT(P_COLUMN_NAME_ARRAY)     WITH ORDINALITY as c(VALUE, IDX)
          ,JSONB_ARRAY_ELEMENTS_TEXT(P_DATA_TYPE_ARRAY)       WITH ORDINALITY as t(VALUE, IDX)
          ,JSONB_ARRAY_ELEMENTS_TEXT(P_SIZE_CONSTRAINT_ARRAY) WITH ORDINALITY as s(VALUE, IDX)
     where (c.IDX = t.IDX) and (c.IDX = s.IDX)
  ),
  TARGET_TABLE_DEFINITIONS
  as (
    select st.*,
           MAP_FOREIGN_DATA_TYPE(P_SOURCE_VENDOR,DATA_TYPE,DATA_TYPE_LENGTH::BIGINT,DATA_TYPE_SCALE::INT, V_GEOMETRY_TYPE) TARGET_DATA_TYPE
      from SOURCE_TABLE_DEFINITIONS st
  ) 
  select STRING_AGG('"' || COLUMN_NAME || '"',',') COLUMN_NAME_LIST,
         STRING_AGG('"' || COLUMN_NAME || '" ' || TARGET_DATA_TYPE || 
                    case 
                      when TARGET_DATA_TYPE like '%(%)' then 
                        ''
                      when TARGET_DATA_TYPE like '%with time zone' then 
                        ''
                      when TARGET_DATA_TYPE like '%without time zone' then 
                        ''
                      when TARGET_DATA_TYPE in ('boolean','smallint', 'mediumint', 'int', 'bigint','real','text','bytea','integer','money','xml','json','jsonb','image','date','double precision','geography','geometry') then 
                        ''
                      when (TARGET_DATA_TYPE = 'time' and DATA_TYPE_LENGTH::INT > 6) then 
                        '(6)'
                      when TARGET_DATA_TYPE like 'interval%' then 
                        ''
                      when DATA_TYPE_LENGTH is NOT NULL and DATA_TYPE_SCALE IS NOT NULL then 
                        '(' || DATA_TYPE_LENGTH || ',' || DATA_TYPE_SCALE || ')'
                      when DATA_TYPE_LENGTH is NOT NULL then 
                        '(' || DATA_TYPE_LENGTH  || ')'
                      else
                        ''
                    end
                   ,CHR(10) || '  ,'
                   ) COLUMNS_CLAUSE
        ,STRING_AGG(case 
                      when TARGET_DATA_TYPE = 'bytea' then  
                        'decode( value ->> ' || IDX-1 || ',''hex'')'
                      when TARGET_DATA_TYPE in ('geometry','geography') then 
                        case 
                          when P_SPATIAL_FORMAT = 'WKB' then
                            'ST_GeomFromWKB(decode( value ->> ' || IDX-1 || ',''hex''))'
                          when P_SPATIAL_FORMAT = 'EWKB' then
                            'ST_GeomFromEWKB(decode( value ->> ' || IDX-1 || ',''hex''))'
                          when P_SPATIAL_FORMAT = 'WKT' then
                            'ST_GeomFromText( value ->> ' || IDX-1 || ')'
                          when P_SPATIAL_FORMAT = 'EWKT' then
                            'ST_GeomFromEWKT( value ->> ' || IDX-1 || ')'
                          when P_SPATIAL_FORMAT = 'GeoJSON' then
                            'ST_GeomFromGeoJSON( value ->> ' || IDX-1 || ')'
                          end
                      when TARGET_DATA_TYPE in ('time', 'time with time zone','time without time zone')  then  
                        'cast( value ->> ' || IDX-1 || ' as timestamp)::' || TARGET_DATA_TYPE
                      when TARGET_DATA_TYPE like 'time%time zone'  then  
                        'cast( value ->> ' || IDX-1 || ' as timestamp)::' || TARGET_DATA_TYPE
                      when TARGET_DATA_TYPE in ('time', 'time with time zone','time without time zone')  then  
                        'cast( value ->> ' || IDX-1 || ' as timestamp)::' || TARGET_DATA_TYPE
                      when TARGET_DATA_TYPE = 'bit' then  
                        'case when value ->> ' || IDX-1 || ' = ''true'' then B''1'' when value ->> ' || IDX-1 || ' = ''false''  then B''0'' else cast( value ->> ' || IDX-1 || ' as ' || TARGET_DATA_TYPE || ') end'
                      else
                       'cast( value ->> ' || IDX-1 || ' as ' || TARGET_DATA_TYPE || ')'
                    end 
                    || ' "' || COLUMN_NAME || '"', CHR(10) || '  ,'
                   ) INSERT_SELECT_LIST
        ,JSONB_AGG(TARGET_DATA_TYPE) TARGET_DATA_TYPES
    into V_COLUMN_LIST, V_COLUMNS_CLAUSE, V_INSERT_SELECT_LIST, V_TARGET_DATA_TYPES
    from TARGET_TABLE_DEFINITIONS;

  return JSONB_BUILD_OBJECT(
                    'ddl', 'CREATE TABLE IF NOT EXISTS "' || P_SCHEMA || '"."' || P_TABLE_NAME || '"(' || CHR(10) || '   ' || V_COLUMNS_CLAUSE || CHR(10) || ')'
                    ,'dml', 'INSERT into "' || P_SCHEMA || '"."' || P_TABLE_NAME || '"(' || V_COLUMN_LIST || ')' || CHR(10) || 'select ' || V_INSERT_SELECT_LIST || CHR(10) || '  from ' || case WHEN P_BINARY_JSON then 'jsonb_array_elements' else 'json_array_elements' end || '($1 -> ''data'' -> ''' || P_TABLE_NAME || ''')'
                    ,'targetDataTypes', V_TARGET_DATA_TYPES
               );
end;  
$$ LANGUAGE plpgsql;
--
create or replace function IMPORT_JSONB(P_JSON jsonb, P_SCHEMA VARCHAR) 
returns JSONB
as $$
declare
  R                  RECORD;
  V_RESULTS          JSONB = '[]';
  V_ROW_COUNT        INTEGER;
  V_START_TIME       TIMESTAMPTZ;
  V_END_TIME         TIMESTAMPTZ;
  V_ELAPSED_TIME     NUMERIC;

  PLPGSQL_CTX        TEXT;
begin
  for r in select "tableName"
                 ,GENERATE_STATEMENTS(P_JSON #> '{systemInformation}' ->> 'vendor', P_SCHEMA,"tableName", P_JSON #> '{systemInformation}' ->> 'spatialFormat', "columnNames", "dataTypes", "sizeConstraints", TRUE) "TABLE_INFO"
             from JSONB_EACH(P_JSON -> 'metadata')  
                  CROSS JOIN LATERAL JSONB_TO_RECORD(value) as METADATA(
                                                                "tableSchema"      VARCHAR, 
                                                                "tableName"        VARCHAR, 
                                                                "columnsNames"     JSONB, 
                                                                "dataTypes"        JSONB, 
                                                                "sizeConstraints"  JSONB
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
      V_ELAPSED_TIME :=  round(((extract(epoch from V_END_TIME) - extract(epoch from V_START_TIME)) * 1000)::NUMERIC,4);
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
  V_ELAPSED_TIME     NUMERIC;

  PLPGSQL_CTX        TEXT;
begin

  for r in select "tableName"
                 ,GENERATE_STATEMENTS(P_JSON #> '{systemInformation}' ->> 'vendor',P_SCHEMA,"tableName", P_JSON #> '{systemInformation}' ->> 'spatialFormat',"columnNames","dataTypes","sizeConstraints",FALSE) "TABLE_INFO"
             from JSON_EACH(P_JSON -> 'metadata')  
                  CROSS JOIN LATERAL JSON_TO_RECORD(value) as METADATA(
                                                               "tableSchema"      VARCHAR, 
                                                               "tableName"        VARCHAR, 
                                                               "columnNames"      JSONB,
                                                               "dataTypes"        JSONB, 
                                                               "sizeConstraints"  JSONB
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
      V_ELAPSED_TIME :=  round(((extract(epoch from V_END_TIME) - extract(epoch from V_START_TIME)) * 1000)::NUMERIC,4);
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
create or replace function GENERATE_SQL(P_JSON jsonb, P_SCHEMA VARCHAR, P_SPATIAL_FORMAT VARCHAR)
returns SETOF jsonb
as $$
declare
begin
  RETURN QUERY
    select jsonb_object_agg(
           "tableName",
           GENERATE_STATEMENTS("vendor",P_SCHEMA,"tableName",P_SPATIAL_FORMAT,"columnNames","dataTypes","sizeConstraints",FALSE)
         )
    from JSONB_EACH(P_JSON -> 'metadata')  
         CROSS JOIN LATERAL JSONB_TO_RECORD(value) as METADATA(
                                                       "vendor"           VARCHAR,
                                                       "tableSchema"      VARCHAR, 
                                                       "tableName"        VARCHAR, 
                                                       "columnNames"      JSONB, 
                                                       "dataTypes"        JSONB, 
                                                       "sizeConstraints"  JSONB
                                                     );
end;
$$ LANGUAGE plpgsql;
--
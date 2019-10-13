create or replace procedure COMPARE_SCHEMA(P_SOURCE_SCHEMA VARCHAR,P_TARGET_SCHEMA VARCHAR,P_EMPTY_STRING_IS_NULL BOOLEAN,P_STRIP_XML_DECLARATION BOOLEAN, P_SPATIAL_PRECISION INT)
as $$
declare
  R RECORD;
  V_SQL_STATEMENT TEXT;
  C_NEWLINE CHAR(1) = CHR(10);
  
  V_SOURCE_COUNT INT;
  V_TARGET_COUNT INT;
  V_SQLERRM        TEXT;
begin
  create temporary table if not exists SCHEMA_COMPARE_RESULTS (
    SOURCE_SCHEMA    VARCHAR(128)
   ,TARGET_SCHEMA    VARCHAR(128)
   ,TABLE_NAME       VARCHAR(128)
   ,SOURCE_ROW_COUNT INT
   ,TARGET_ROW_COUNT INT
   ,MISSING_ROWS     INT
   ,EXTRA_ROWS       INT
   ,SQLERRM          TEXT
  );
  
  for r in select t.table_name
	             ,string_agg(
                    case 
                      when data_type in ('character varying') then
                        case 
                          when P_EMPTY_STRING_IS_NULL then
                            'case when"' || column_name || '" = '''' then NULL else "' || column_name || '" end "' || column_name || '"' 
                          else 
                            '"' || column_name || '"' 
                        end
                      when data_type = 'json'  then
                        '"' || column_name || '"::text' 
                      when data_type = 'xml'  then
                        case  
                          when P_STRIP_XML_DECLARATION then
                           'regexp_replace(regexp_replace("' || column_name || '"::text,''<\?xml.*?\?>'',''''),''&apos;'','''''''',''g'')'
                          else
                            '"' || column_name || '"::text' 
                        end
                      when ((data_type = 'USER-DEFINED') and (udt_name = 'geometry')) then
                       case 
                         when P_SPATIAL_PRECISION < 18 then
                           'ST_AsText("' || column_name || '",' || P_SPATIAL_PRECISION || ')' 
                         else
                           'ST_AsEWKB("' || column_name || '")' 
                        end
                      when ((data_type = 'USER-DEFINED') and (udt_name = 'geography')) then
                       case 
                         when P_SPATIAL_PRECISION < 18 then
                           'ST_AsText("' || column_name || '",' || P_SPATIAL_PRECISION || ')' 
                         else
                           'ST_AsBinary("' || column_name || '")' 
                        end
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
      -- RAISE INFO 'SQL: %', V_SQL_STATEMENT;
      EXECUTE V_SQL_STATEMENT;               
    exception  
      when others then
        V_SQLERRM = SQLERRM;
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
		
        insert into SCHEMA_COMPARE_RESULTS VALUES (P_SOURCE_SCHEMA, P_TARGET_SCHEMA, r.TABLE_NAME, V_SOURCE_COUNT, V_TARGET_COUNT, -1, -1, V_SQLERRM);            
    end;               
                    
  end loop;
end;
$$ LANGUAGE plpgsql;
--

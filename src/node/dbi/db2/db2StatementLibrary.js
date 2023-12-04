class DB2StatementLibrary {

  static get SQL_CONFIGURE_CONNECTION()                       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()                         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_SCHEMA_METADATA()                            { return _SQL_SCHEMA_METADATA }
  static get SQL_GENERATE_STATEMENTS()                        { return _SQL_GENERATE_STATEMENTS }

  static get SQL_BEGIN_TRANSACTION()                          { return _SQL_BEGIN_TRANSACTION }  
  static get SQL_COMMIT_TRANSACTION()                         { return _SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()                       { return _SQL_ROLLBACK_TRANSACTION }

}

const _SQL_CONFIGURE_CONNECTION = `SELECT service_level AS DATABASE_VERSION, fixpack_num as FIXPACK_NUMBER FROM TABLE(sysproc.env_get_inst_info()) as INSTANCEINFO`

const _SQL_SYSTEM_INFORMATION   = `select HOST_NAME as "hostName", CURRENT SERVER as "dbName", CURRENT TIMEZONE as "sessionTimeZone", SESSION_USER as "sessionUser", YADAMU.YADAMU_INSTANCE_ID() as "yadamuInstanceID", YADAMU.YADAMU_INSTALLATION_TIMESTAMP() as "yadamuInstallationTimestamp" from table(SYSPROC.DB_MEMBERS()) as members`

const _SQL_SCHEMA_METADATA      = `select trim(trailing ' ' from t."TABSCHEMA")   "TABLE_SCHEMA"
                                         ,t."TABNAME"   "TABLE_NAME"
                                         ,'[' || listagg('"' || c."COLNAME" || '"',',') within group (order by c."COLNO") || ']' "COLUMN_NAME_ARRAY"
                                         ,'[' || listagg('"' || case 
                                                                  when (UPPER(ck.TEXT) like '%BSON_VALIDATE%') then 
                                                                    'JSON' 
                                                                  else 
                                                                    c."TYPENAME" 
                                                                end 
                                                             || '"',',') within group (order by c."COLNO") || ']' "DATA_TYPE_ARRAY"
                                         ,'[' || listagg(
                                                   case
                                                     when (c.TYPENAME = 'DECFLOAT') then
													   case 
													     when (c.LENGTH = 8) then
														   '[16]'
														 else
														   '[34]'
													   end   
                                                     when (c.TYPENAME = 'TIMESTAMP') then
                                                       '[' || "SCALE" || ']'
                                                     when ("LENGTH" is not null) and ("SCALE" is not null) and ("SCALE" <> 0) then
                                                       '[' || "LENGTH" || ',' || "SCALE" || ']'
                                                     when ("LENGTH" is not null) then
                                                       '[' || "LENGTH" || ']'
                                                     else
                                                      '[]'
                                                   end 
                                                   ,','                   
                                                 ) 
                                             within group (order by "COLNO")
                                          || ']'  "SIZE_CONSTRAINT_ARRAY"
                                         ,'[' || listagg(
                                                   case
                                                     when (UPPER(ck.TEXT) like '%BSON_VALIDATE%') then
                                                       '"JSON"'
                                                     when (UPPER(ck.TEXT) like '%IS_JSON%') then
                                                       '"JSON"'
                                                     when (c.TYPENAME in 'XML') then
                                                       '"XML"'
                                                     when (UPPER(ck.TEXT) like '%IS_XML%') then
                                                       '"XML"'
                                                     when (UPPER(ck.TEXT) like '%IS_WKT%') then
                                                       '"WKT"'
                                                     when (UPPER(ck.TEXT) like '%IS_WKB%') then
                                                       '"WKB"'
                                                     when (UPPER(ck.TEXT) like '%IS_GEOJSON%') then
                                                       '"GeoJSON"'
                                                     else
                                                      '""'
                                                   end 
                                                   ,','                   
                                                 ) 
                                             within group (order by "COLNO")
                                          || ']' "EXTENDED_TYPE_ARRAY"
                                         ,listagg(
                                            case 
                                              when (UPPER(ck.TEXT) like '%BSON_VALIDATE%') then
                                                'SYSTOOLS.BSON2JSON("' || c."COLNAME" || '") "' || c."COLNAME" || '"'
                                              when (c.TYPENAME in ('BIGINT','DECFLOAT','NUMERIC','DECIMAL')) then
                                                'cast("' || c."COLNAME" || '" as VARCHAR) "' || c."COLNAME" || '"'
                                              when (c.TYPENAME in ('XML')) then
                                                'XMLSERIALIZE("' || c."COLNAME" || '"  AS BLOB(2G) EXCLUDING XMLDECLARATION) "' || c."COLNAME" || '"'
                                              else
                                                '"' || c."COLNAME" || '"'
                                            end
                                            ,',') within group (order by c."COLNO") "CLIENT_SELECT_LIST"
                 from "SYSCAT"."COLUMNS" c
                 join "SYSCAT"."TABLES" t on t."TABSCHEMA" = c."TABSCHEMA" and t."TABNAME" = c."TABNAME"
                 left outer join "SYSCAT"."COLCHECKS" cc on c."TABSCHEMA" = cc."TABSCHEMA" and c."TABNAME" = cc."TABNAME" and c."COLNAME" = cc."COLNAME"
                 left outer join "SYSCAT"."CHECKS" ck on cc."CONSTNAME" = ck.CONSTNAME 
                where t."TABSCHEMA" = ?
                  and t."TYPE" = 'T'
                group by t."TABSCHEMA", t."TABNAME"`;

const _SQL_GENERATE_STATEMENTS  = `-- SQL TO GENERATE DDL AND DML STATEMENTS`

const _SQL_BEGIN_TRANSACTION    = `BEGIN TRANSACTION`;

const _SQL_COMMIT_TRANSACTION   = `COMMIT TRANSACTION`;

const _SQL_ROLLBACK_TRANSACTION = `ROLLBACK TRANSACTION`;

export { DB2StatementLibrary as default }

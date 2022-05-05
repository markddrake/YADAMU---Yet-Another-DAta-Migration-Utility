/*
**
** TODO: Add support for specifying whether to map JSON to JSON or JSONB
**
*/

create or replace function MAP_FOREIGN_DATA_TYPE(P_SOURCE_VENDOR  VARCHAR, P_DATA_TYPE VARCHAR, P_DATA_TYPE_LENGTH BIGINT, P_DATA_TYPE_SCALE INT, P_JSON_DATA_TYPE VARCHAR, P_POSTGIS_INSTALLED BOOLEAN) 
returns VARCHAR
as $$
declare
  C_CHAR_TYPE                         VARCHAR(32) = 'character';
  C_CLOB_TYPE                         VARCHAR(32) = 'text';
  
  C_GEOMETRY_TYPE                     VARCHAR(32) 
  = CASE WHEN P_POSTGIS_INSTALLED THEN 'geometry' ELSE 'JSON' END;
  C_GEOGRAPHY_TYPE                    VARCHAR(32) = CASE WHEN P_POSTGIS_INSTALLED THEN 'geography' ELSE 'JSON' END;
  C_MAX_CHARACTER_VARYING_TYPE_LENGTH INT         = 10 * 1024 * 1024;
  C_MAX_CHARACTER_VARYING_TYPE        VARCHAR(32) = 'character varying(' || C_MAX_CHARACTER_VARYING_TYPE_LENGTH || ')';
  C_BFILE_TYPE                        VARCHAR(32) = 'character varying(2048)';
  C_ROWID_TYPE                        VARCHAR(32) = 'character varying(18)';
  C_MYSQL_TINY_TEXT_TYPE              VARCHAR(32) = 'character varying(256)';
  C_MYSQL_TEXT_TYPE                   VARCHAR(32) = 'character varying(65536)';
  C_ENUM_TYPE                         VARCHAR(32) = 'character varying(255)';
  C_MSSQL_MONEY_TYPE                  VARCHAR(32) = 'numeric(19,4)';
  C_MSSQL_SMALL_MONEY_TYPE            VARCHAR(32) = 'numeric(10,4)';
  C_HIERARCHY_TYPE                    VARCHAR(32) = 'character varying(4000)';
  C_INET_ADDR_TYPE                    VARCHAR(32) = 'character varying(39)';
  C_MAC_ADDR_TYPE                     VARCHAR(32) = 'character varying(23)';
  C_UNSIGNED_INT_TYPE                 VARCHAR(32) = 'decimal(10,0)';
  C_PGSQL_IDENTIFIER                  VARCHAR(32) = 'binary(4)';
  C_MONGO_OBJECT_ID                   VARCHAR(32) = 'binary(12)';
  C_MONGO_UNKNOWN_TYPE                VARCHAR(32) = 'character varying(2048)';
  C_MONGO_REGEX_TYPE                  VARCHAR(32) = 'character varying(2048)';

  V_DATA_TYPE                         VARCHAR(128);
end;
$$ LANGUAGE plpgsql;
--
     
begin
  V_DATA_TYPE := P_DATA_TYPE;

  case P_SOURCE_VENDOR 
    when 'Postgres' then
      case V_DATA_TYPE 
	    when 'character'                                                                         then return case when P_DATA_TYPE_LENGTH is NULL then 'bpchar' else C_CHAR_TYPE end;                                                
        when 'timestamp with time zone'                                                          then return 'timestamp(' || P_DATA_TYPE_LENGTH || ') with time zone';
        when 'timestamp without time zone'                                                       then return 'timestamp(' || P_DATA_TYPE_LENGTH || ') without time zone';
        when 'time with time zone'                                                               then return 'time(' || P_DATA_TYPE_LENGTH || ') with time zone';
        when 'time without time zone'                                                            then return 'time(' || P_DATA_TYPE_LENGTH || ') without time zone';
                                                                                                 else return lower(V_DATA_TYPE);																					
      end case;
    when 'Oracle' then
      case V_DATA_TYPE
        when 'CHAR'                                                                              then return C_CHAR_TYPE;
        when 'NCHAR'                                                                             then return C_CHAR_TYPE;
        when 'VARCHAR2'                                                                          then return 'character varying';
		when 'NVARCHAR2'                                                                         then return 'character varying';
        when 'CLOB'                                                                              then return C_CLOB_TYPE;
        when 'NCLOB'                                                                             then return C_CLOB_TYPE;
		when 'NUMBER'                                                                            then return 'numeric';
        when 'BINARY_FLOAT'                                                                      then return 'float4';
        when 'BINARY_DOUBLE'                                                                     then return 'float8';
        when 'RAW'                                                                               then return 'bytea';
        when 'BLOB'                                                                              then return 'bytea';
        when 'TIMESTAMP'                                                                         then return case when P_DATA_TYPE_LENGTH > 6  then 'timestamp(6)' else 'timestamp' end;
        when 'BFILE'                                                                             then return C_BFILE_TYPE;
        when 'ROWID'                                                                             then return C_ROWID_TYPE;
        when 'ANYDATA'                                                                           then return C_CLOB_TYPE;
        when 'XMLTYPE'                                                                           then return 'xml';
        when '"MDSYS"."SDO_GEOMETRY"'                                                            then return C_GEOMETRY_TYPE;
        else
		  case 
		    when (strpos(V_DATA_TYPE,'LOCAL TIME ZONE') > 0)                                     then return lower(replace(V_DATA_TYPE,'LOCAL TIME ZONE','TIME ZONE'));  
            when (strpos(V_DATA_TYPE,'INTERVAL') = 1) 
			 and (strpos(V_DATA_TYPE,'YEAR') > 0) 
			 and (strpos(V_DATA_TYPE,'TO MONTH') > 0)                                            then return 'interval year to month';
          when (strpos(V_DATA_TYPE,'INTERVAL') = 1) 
		   and (strpos(V_DATA_TYPE,'DAY') > 0) 
		   and (strpos(V_DATA_TYPE,'TO SECOND') > 0)                                             then return 'interval day to second';
		  when (strpos(V_DATA_TYPE,'"."XMLTYPE"') > 0)                                           then return 'xml';
          -- Map all object types to text - Store the Oracle serialized format. 
          -- When Oracle Objects are mapped to JSON change the mapping to JSON.
          when(V_DATA_TYPE like '"%"."%"')                                                       then return C_CLOB_TYPE;
                                                                                                 else return lower(V_DATA_TYPE);
		 end case;
      end case;
    when  'MySQL' then
      case V_DATA_TYPE
        -- MySQL Direct Mappings
        when 'binary'                                                                            then return 'bytea';
        when 'bit'                                                                               then return 'boolean';
        when 'time'                                                                              then return 'time without time zone';
        when 'datetime'                                                                          then return 'timestamp';
        when 'double'                                                                            then return 'double precision';
        when 'enum'                                                                              then return C_ENUM_TYPE;   
        when 'float'                                                                             then return 'real';
        when 'point'                                                                             then return 'point';
        when 'linestring'                                                                        then return 'path';
        when 'polygon'                                                                           then return 'polygon';
        when 'geometry'                                                                          then return C_GEOMETRY_TYPE;
        when 'multipoint'                                                                        then return C_GEOMETRY_TYPE;
        when 'multilinestring'                                                                   then return C_GEOMETRY_TYPE;
        when 'multipolygon'                                                                      then return C_GEOMETRY_TYPE;
        when 'geometrycollection'                                                                then return C_GEOMETRY_TYPE;
        when 'geomcollection'                                                                    then return C_GEOMETRY_TYPE;
        when 'tinyint'                                                                           then return 'smallint';
        when 'mediumint'                                                                         then return 'integer';
        when 'int unsigned'                                                                      then return 'oid';
        when 'tinyblob'                                                                          then return 'bytea';
        when 'blob'                                                                              then return 'bytea';
        when 'mediumblob'                                                                        then return 'bytea';
        when 'longblob'                                                                          then return 'bytea';
        when 'set'                                                                               then return 'jsonb';   
        when 'tinyint'                                                                           then return 'smallint';
        when 'tinytext'                                                                          then return C_MYSQL_TINY_TEXT_TYPE;
        when 'text'                                                                              then return C_MYSQL_TEXT_TYPE;
        when 'mediumtext'                                                                        then return C_CLOB_TYPE;
        when 'longtext'                                                                          then return C_CLOB_TYPE;
        when 'varbinary'                                                                         then return 'bytea';
        when 'year'                                                                              then return 'smallint';
                                                                                                 else return lower(V_DATA_TYPE);
      end case;
    when  'MariaDB' then
      case V_DATA_TYPE
        -- MySQL Direct Mappings
        when 'binary'                                                                            then return 'bytea';
        when 'bit'                                                                               then return 'boolean';
        when 'datetime'                                                                          then return 'timestamp';
		when 'time'                                                                              then return 'time without time zone';
        when 'double'                                                                            then return 'double precision';
        when 'enum'                                                                              then return 'varchar(255)';   
        when 'float'                                                                             then return 'real';
        when 'point'                                                                             then return 'point';
        when 'linestring'                                                                        then return 'path';
        when 'polygon'                                                                           then return 'polygon';
        when 'geometry'                                                                          then return C_GEOMETRY_TYPE;
        when 'multipoint'                                                                        then return C_GEOMETRY_TYPE;
        when 'multilinestring'                                                                   then return C_GEOMETRY_TYPE;
        when 'multipolygon'                                                                      then return C_GEOMETRY_TYPE;
        when 'geometrycollection'                                                                then return C_GEOMETRY_TYPE;
        when 'geomcollection'                                                                    then return C_GEOMETRY_TYPE;
        when 'tinyint'                                                                           then return 'smallint';
        when 'mediumint'                                                                         then return 'integer';
        when 'tinyblob'                                                                          then return 'bytea';
        when 'blob'                                                                              then return 'bytea';
        when 'mediumblob'                                                                        then return 'bytea';
        when 'longblob'                                                                          then return 'bytea';
        when 'set'                                                                               then return 'jsonb';   
        when 'tinyint'                                                                           then return 'smallint';
        when 'tinytext'                                                                          then return C_MYSQL_TINY_TEXT_TYPE;
        when 'text'                                                                              then return C_MYSQL_TEXT_TYPE;
        when 'mediumtext'                                                                        then return C_CLOB_TYPE;
        when 'longtext'                                                                          then return C_CLOB_TYPE;
        when 'varbinary'                                                                         then return 'bytea';
        when 'year'                                                                              then return 'smallint';
                                                                                                 else return lower(V_DATA_TYPE);
      end case;
    when 'MSSQLSERVER'  then 
      case V_DATA_TYPE         
        -- MSSQL Direct Mappings
        when 'varchar'                                                                           then return case when P_DATA_TYPE_LENGTH = -1 then C_CLOB_TYPE else 'character varying' end;
        when 'nvarchar'                                                                          then return case when P_DATA_TYPE_LENGTH = -1 then C_CLOB_TYPE else 'character varying' end;
        when 'nchar'                                                                             then return 'char';
        when 'ntext'                                                                             then return C_CLOB_TYPE;
        when 'bit'                                                                               then return 'boolean';
        -- Do not use Postgres Money Type due to precision issues. 
		-- E.G. with Locale USD Postgres only provides 2 digit precison.
        when 'money'                                                                             then return C_MSSQL_MONEY_TYPE;
        when 'smallmoney'                                                                        then return C_MSSQL_SMALL_MONEY_TYPE;
        when 'tinyint'                                                                           then return 'smallint';
        when 'time'                                                                              then return 'time without time zone';
        when 'datetime'                                                                          then return case when P_DATA_TYPE_LENGTH > 6 then 'timestamp(6)' else 'timestamp' end;
        when 'datetime2'                                                                         then return case when P_DATA_TYPE_LENGTH > 6 then 'timestamp(6)' else 'timestamp' end;
        when 'datetimeoffset'                                                                    then return case when P_DATA_TYPE_LENGTH > 6 then 'timestamp(6) with time zone' else 'timestamp  with time zone' end;
        when 'smalldatetime'                                                                     then return 'timestamp(0)';
        when 'binary'                                                                            then return 'bytea';
        when 'varbinary'                                                                         then return 'bytea';
        when 'image'                                                                             then return 'bytea';
        when 'geometry'                                                                          then return C_GEOMETRY_TYPE;
        when 'geography'                                                                         then return C_GEOGRAPHY_TYPE;
        when 'rowversion'                                                                        then return 'bytea';
        when 'hierarchyid'                                                                       then return C_HIERARCHY_TYPE;
        when 'uniqueidentifier'                                                                  then return 'uuid';
                                                                                                 else return lower(V_DATA_TYPE);
      end case;
    when  'Vertica' then
/*
           case 'char':
             switch (true) {
               case (length > this.DataTypes.CHAR_LENGTH) :                        return this.DataTypes.CLOB_TYPE
               default:                                                            return this.DataTypes.CHAR_TYPE
             }

           case 'varchar':
           case 'long varchar':
             switch (true) {
               case (length > this.DataTypes.VARCHAR_LENGTH):                      return this.DataTypes.CLOB_TYPE
               default:                                                            return this.DataTypes.VARCHAR_TYPE
             }

           case 'binary':
             switch (true) {
               case (length > this.DataTypes.BINARY_LENGTH):                       return this.DataTypes.BLOB_TYPE
               default:                                                            return this.DataTypes.BINARY_TYPE
             }

           case 'varbinary':
           case 'long varbinary':
             switch (true) {
               case (length > this.DataTypes.VARBINARY_LENGTH):                    return this.DataTypes.BLOB_TYPE
               default:                                                            return this.DataTypes.VARBINARY_TYPE
             }

           case 'numeric':
             switch (true) {
               default:                                                            return this.DataTypes.NUMERIC_TYPE
             }

           case 'boolean':                                                         return this.DataTypes.BOOLEAN_TYPE


           case 'int':                                                             return this.DataTypes.BIGINT_TYPE
           case 'float':                                                           return this.DataTypes.DOUBLE_TYPE

           case 'date':                                                            return this.DataTypes.DATE_TYPE
           case 'time':                                                            return this.DataTypes.TIME_TYPE
           case 'timetz':                                                          return this.DataTypes.TIME_TZ_TYPE
           case 'timestamptz':                                                     return this.DataTypes.TIMESTAMP_TZ_TYPE
           case 'timestamp':                                                       return this.DataTypes.TIMESTAMP_TYPE
           case 'year':                                                            return this.DataTypes.VERTICA_YEAR_TYPE || this.YEAR_TYPE

           case 'xml':                                                             return this.DataTypes.XML_TYPE
           case 'json':                                                            return this.DataTypes.JSON_TYPE
           case 'uuid':                                                            return this.DataTypes.UUID_TYPE
		   
           case 'geometry':                                                        return this.DataTypes.SPATIAL_TYPE
           case 'geography':                                                       return this.DataTypes.GEOGRAPHY_TYPE
		   
           default:
             if (dataType.startsWith('interval')) {
               return this.DataTypes.INTERVAL_TYPE
             }
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
             return dataType.toLowerCase();
         }
*/

      case V_DATA_TYPE
        -- MySQL Direct Mappings
        when 'binary'                                                                            then return 'bytea';
        when 'varbinary'                                                                         then return 'bytea';
		when 'long varbinary'                                                                    then return 'bytea';
		when 'varchar'                                                                           then return 'character varying';
		when 'long varchar'                                                                      then return C_MAX_CHARACTER_VARYING_TYPE;
		when 'int'                                                                               then return 'bigint';
                                                                                                 else return lower(V_DATA_TYPE);
      end case;
	when 'MongoDB' then
      -- MongoDB typing based on BSON type model 
      case V_DATA_TYPE
        when 'double'                                                                            then return 'double precision';
        when 'string'                                                                            then return case when P_DATA_TYPE_LENGTH >  C_MAX_CHARACTER_VARYING_TYPE_LENGTH then C_CLOB_TYPE else 'character varying' end;
        when 'object'                                                                            then return P_JSON_DATA_TYPE;
        when 'array'                                                                             then return P_JSON_DATA_TYPE;
        when 'binData'                                                                           then return 'bytea';
		when 'objectId'                                                                          then return 'bytea';
        when 'boolean'                                                                           then return 'bool';
        when 'null'                                                                              then return C_MONGO_UNKNOWN_TYPE;
        when 'regex'                                                                             then return C_MONGO_REGEX_TYPE;
        when 'javascript'                                                                        then return C_MAX_CHARACTER_VARYING_TYPE;
        when 'javascriptWithScope'                                                               then return C_MAX_CHARACTER_VARYING_TYPE;
        when 'int'                                                                               then return 'int';
        when 'long'                                                                              then return 'bigint';
        when 'decimal'                                                                           then return 'numeric';
        when 'date'                                                                              then return 'timestamp';
        when 'timestamp'                                                                         then return 'timestamp';
        when 'minkey'                                                                            then return P_JSON_DATA_TYPE;
        when 'maxkey'                                                                            then return P_JSON_DATA_TYPE;
                                                                                                 else return lower(P_DATA_TYPE);
      end case;    
    when 'SNOWFLAKE' then
      case V_DATA_TYPE
        when 'TEXT'                                                                              then return case when P_DATA_TYPE_LENGTH >  C_MAX_CHARACTER_VARYING_TYPE_LENGTH then C_CLOB_TYPE else 'character varying' end;
        when 'NUMBER'                                                                            then return 'numeric';
        when 'FLOAT'                                                                             then return 'double precision';
        when 'BINARY'                                                                            then return 'bytea';
        when 'XML'                                                                               then return 'xml';
        when 'TIME'                                                                              then return case when P_DATA_TYPE_LENGTH > 6 then 'time(6) without time zone' else 'time without time zone' end;
        when 'TIMESTAMP_LTZ'                                                                     then return case when P_DATA_TYPE_LENGTH > 6 then 'timestamp(6) with time zone' else 'timestamp(' || P_DATA_TYPE_LENGTH || ')  with time zone' end;
        when 'TIMESTAMP_NTZ'                                                                     then return case when P_DATA_TYPE_LENGTH > 6 then 'timestamp(6) without time zone' else 'timestamp(' || P_DATA_TYPE_LENGTH || ')  without time zone' end;
        when 'VARIANT'                                                                           then return 'bytea';
                                                                                                 else return lower(P_DATA_TYPE);
      end case;	
    else 
      return lower(V_DATA_TYPE);
  end case;
end;
$$ LANGUAGE plpgsql;
--
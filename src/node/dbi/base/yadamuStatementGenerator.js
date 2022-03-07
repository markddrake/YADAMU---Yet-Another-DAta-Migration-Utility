class YadamuStatementGenerator {

  constructor(dbi, targetSchema, metadata, yadamuLogger) {

    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;
  }

  mapForeignDataType(DataTypes, vendor, dataType, dataTypeLength, dataTypeSize) {
     switch (vendor) {
       case 'Oracle':
         switch (dataType) {

           case 'CHAR':                                                            return DataTypes.CHAR_TYPE
           case 'NCHAR':                                                           return DataTypes.NVARCHAR_TYPE
           case 'VARCHAR2':                                                        return DataTypes.VARCHAR_TYPE
           case 'NVARCHAR2':                                                       return DataTypes.NVARCHAR_TYPE
           
		   case 'NUMBER':                                                          return DataTypes.NUMERIC_TYPE
           case 'BINARY_FLOAT':                                                    return DataTypes.FLOAT_TYPE
           case 'BINARY_DOUBLE':                                                   return DataTypes.DOUBLE_TYPE

           case 'CLOB':                                                            return DataTypes.CLOB_TYPE
           case 'NCLOB':                                                           return DataTypes.NCLOB_TYPE

           case 'BOOLEAN':                                                         return DataTypes.BOOLEAN_TYPE
           case 'RAW':                                                             return DataTypes.VARBINARY_TYPE
           case 'BLOB':                                                            return DataTypes.BLOB_TYPE

           case 'DATE':                                                            return DataTypes.DATE_TYPE
           case 'TIMESTAMP':
             switch (true) {
               default:                                                            return DataTypes.TIMESTAMP_TYPE
             }

           case 'BFILE':                                                           return DataTypes.ORACLE_BFILE_TYPE
           case 'ROWID':                                                           return DataTypes.ORACLE_ROWID_TYPE

           case 'JSON':                                                            return DataTypes.JSON_TYPE
           case 'XMLTYPE':                                                         return DataTypes.XML_TYPE
           case '"MDSYS"."SDO_GEOMETRY"':                                          return DataTypes.SPATIAL_TYPE
           case 'ANYDATA':                                                         return DataTypes.CLOB_TYPE;
		   
           default :
             if (dataType.indexOf('LOCAL TIME ZONE') > -1) {
               return DataTypes.TIMESTAMP_LTZ_TYPE || DataTypes.TIMESTAMP_TZ_TYPE || DataTypes.TIMESTAMP_TYPE 
             }
             if (dataType.indexOf('TIME ZONE') > -1) {
               return DataTypes.TIMESTAMP_TZ_TYPE || DataTypes.TIMESTAMP_TYPE
             }
             if (dataType.indexOf('INTERVAL') === 0) {
               return 'VARCHAR(16)';
             }
             if (dataType.indexOf('XMLTYPE') > -1) {
               return DataTypes.XML_TYPE
             }
             if (dataType.indexOf('.') > -1) {
               return DataTypes.CLOB_TYPE
             }
			 this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
             return dataType.toUpperCase();
         }
         break;
       case 'MSSQLSERVER':
         switch (dataType) {
           case 'char':
             switch (true) {
               case (dataTypeLength === -1):                                       return DataTypes.CLOB_TYPE
               case (dataTypeLength > DataTypes.MAX_CHARACTER_SIZE) :              return DataTypes.CLOB_TYPE
               default:                                                            return DataTypes.CHAR_TYPE;
             }
           case 'nchar':
             switch (true) {
               case (dataTypeLength === -1):                                       return DataTypes.NCLOB_TYPE || DataTypes.CLOB_TYPE 
               case (dataTypeLength > DataTypes.MAX_CHARACTER_SIZE) :              return DataTypes.NCLOB_TYPE || DataTypes.CLOB_TYPE 
               default:                                                            return DataTypes.NCHAR_TYPE || DataTypes.CHAR_TYPE 
             }
           case 'varchar':
             switch (true) {
               case (dataTypeLength === -1):                                       return DataTypes.CLOB_TYPE
               case (dataTypeLength > DataTypes.MAX_CHARACTER_SIZE) :              return DataTypes.CLOB_TYPE
               default:                                                            return DataTypes.VARCHAR_TYPE
             }
           case 'nvarchar':
             switch (true) {
               case (dataTypeLength === -1):                                       return DataTypes.NCLOB_TYPE    ||DataTypes.CLOB_TYPE
               case (dataTypeLength > DataTypes.MAX_CHARACTER_SIZE) :              return DataTypes.NCLOB_TYPE    || DataTypes.CLOB_TYPE
			 default:                                                              return DataTypes.NVARCHAR_TYPE ||DataTypes.VARCHAR_TYPE
             }
           case 'text':                                                            return DataTypes.CLOB_TYPE 
           case 'ntext':                                                           return DataTypes.NCLOB_TYPE    ||DataTypes.CLOB_TYPE 


           case 'tinyint':                                                         return DataTypes.TINYINT_TYPE     || DataTypes.INTEGER_TYPE
		   case 'smallint':                                                        return DataTypes.SMALLINT_TYPE    || DataTypes.INTEGER_TYPE
           case 'mediumint':                                                       return DataTypes.MEDIUMINT_TYPE   || DataTypes.INTEGER_TYPE
		   case 'int':
		   case 'integer':                                                         return DataTypes.INTEGER_TYPE
           case 'bigint':                                                          return DataTypes.BIGINT_TYPE      || DataTypes.INTEGER_TYPE
           case 'smallmoney':                                                      return DataTypes.MSSQL_SMALL_MONEY_TYPE 
           case 'money':                                                           return DataTypes.MSSQL_MONEY_TYPE
           case 'real':                                                            return DataTypes.FLOAT_TYPE
           case 'float':                                                           return DataTypes.DOUBLE_TYPE
		   case 'numeric':                                                         return DataTypes.DECIMAL_TYPE

           case 'date':                                                            return DataTypes.DATE_TYPE
           case 'time':                                                            return DataTypes.TIME_TYPE
           case 'smalldate':                                                       return DataTypes.DATETIME_TYPE
           case 'datetime':                                                        return DataTypes.DATETIME_TYPE
           case 'datetime2':
             switch (true) {
                // case (dataTypeLength > 6):                                      return 'datetime(6)';
                default:                                                           return DataTypes.DATETIME_TYPE
             }
           case 'datetimeoffset':                                                  return DataTypes.DATETIME_TYPE

           case 'bit':                                                             return DataTypes.BIT_TYPE;
           case 'binary':
             switch (true) {
               case (dataTypeLength === -1):                                       return DataTypes.BLOB_TYPE
               case (dataTypeLength > DataTypes.MAX_BINARY_SIZE):                  return DataTypes.BLOB_TYPE
               default:                                                            return DataTypes.BINARY_TYPE
             }
           case 'varbinary':
             switch (true) {
               case (dataTypeLength === -1):                                       return DataTypes.BLOB_TYPE
               case (dataTypeLength > DataTypes.MAX_BINARY_SIZE):                  return DataTypes.BLOB_TYPE
               default:                                                            return DataTypes.VARBINARY_TYPE
             }
           case 'image':                                                           return DataTypes.BLOB_TYPE

           case 'rowversion':                                                      return DataTypes.MSSQL_ROWVERSION_TYPE
           case 'hierarchyid':                                                     return DataTypes.MSSQL_HEIRARCHY_TYPE
           case 'uniqueidentifier':                                                return DataTypes.UUID_TYPE

           case 'xml':                                                             return DataTypes.XML_TYPE
           case 'JSON':                                                            return DataTypes.JSON_TYPE
           case 'geography':                                                       return DataTypes.SPATIAL_TYPE;
           case 'geometry':                                                        return DataTypes.SPATIAL_TYPE;


           default:
			 this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
		     return dataType.toUpperCase();
         }
         break;
       case 'Postgres':
	     switch (dataType) {
           case 'character':                                                       
             switch (true) {
               case (dataTypeLength === undefined):                                return DataTypes.CLOB_TYPE
               case (dataTypeLength > DataTypes.MAX_CHARACTER_SIZE) :              return DataTypes.CLOB_TYPE
               default:                                                            return DataTypes.CHAR_TYPE
             }
           case 'character varying':
             switch (true) {
               case (dataTypeLength === undefined):                                return DataTypes.CLOB_TYPE
               case (dataTypeLength > DataTypes.MAX_CHARACTER_SIZE) :              return DataTypes.CLOB_TYPE
               default:                                                            return DataTypes.VARCHAR_TYPE
             }
           case 'name':                                                            return DataTypes.PGSQL_NAME_TYPE
		   
		   case 'smallint':                                                        return DataTypes.SMALLINT_TYPE  || DataTypes.INTEGER_TYPE
           case 'integer':                                                         return DataTypes.INTEGER_TYPE   || DataTypes.INTEGER_TYPE
		   case 'bigint':                                                          return DataTypes.BIGINT_TYPE    || DataTypes.INTEGER_TYPE
           case 'real':                                                            return DataTypes.FLOAT_TYPE
           case 'double precision':                                                return DataTypes.DOUBLE_TYPE
		   case 'numeric':                                                         return DataTypes.NUMERIC_TYPE
		   case 'decimal':                                                         return DataTypes.DECIMAL_TYPE
           case 'money':                                                           return DataTypes.PGSQL_MONEY_TYPE
		   
           case 'date':                                                            return DataTypes.DATE_TYPE
           case 'timestamp':                                                       
           case 'timestamp without time zone':                                     return DataTypes.TIMESTAMP_TYPE
           case 'timestamp with time zone':                                        return DataTypes.TIMESTAMP_TZ_TYPE || DataTypes.TIMESTAMP_TYPE
           case 'time without time zone':                                          return DataTypes.TIME_TYPE
           case 'time with time zone':                                             return DataTypes.TIME_TZ_TYPE

           case 'boolean':                                                         return DataTypes.BOOLEAN_TYPE
           case 'bit':                                                             return DataTypes.BIT_TYPE
           case 'bit varying':                                                     
		     switch (true) {
               case (dataTypeLength === undefined):                                return DataTypes.BLOB_TYPE
               case (dataTypeLength/8 > DataTypes.MAX_BINARY_SIZE):                return DataTypes.BLOB_TYPE
               default:                                                            return DataTypes.VARBIT_TYPE
		     }
		   case 'bytea':
             switch (true) {
               case (dataTypeLength === undefined):                                return DataTypes.BLOB_TYPE
               case (dataTypeLength > DataTypes.MAX_BINARY_SIZE):                  return DataTypes.BLOB_TYPE
               default:                                                            return DataTypes.VARBINARY_TYPE
             }
           
           case 'uuid':                                                            return DataTypes.UUID_TYPE
           case 'text':                                                            return DataTypes.CLOB_TYPE
           case 'xml':                                                             return DataTypes.XML_TYPE
           case 'json':                                                            return DataTypes.JSON_TYPE
           case 'jsonb':                                                           return DataTypes.JSON_TYPE

           case 'point':
           case 'lseg':
           case 'path':
           case 'box':
           case 'polygon':
           case 'geography':                                                       return DataTypes.SPATIAL_TYPE
           case 'line':                                                            return DataTypes.JSON_TYPE
           case 'circle':                                                          return this.dbi.INBOUND_CIRCLE_FORMAT === 'CIRCLE' ? DataTypes.JSON_TYPE : DataTypes.SPATIAL_TYPE;

           case 'cidr':
           case 'inet':                                                            return DataTypes.PGSQL_INET_ADDR_TYPE
           case 'macaddr':
           case 'macaddr8':                                                        return DataTypes.PGSQL_MAC_ADDR_TYPE


		   case 'int4range':
           case 'int8range':
           case 'numrange':
           case 'tsrange':
           case 'tstzrange':
           case 'daterange':                                                       return DataTypes.JSON_TYPE

           case 'tsvector':
           case 'gtsvector':                                                       return DataTypes.JSON_TYPE

           case 'tsquery':                                                         return DataTypes.MAX_VARCHAR_TYPE;

           case 'oid':                                                             return DataTypes.PGSQL_IDENTIFIER

           case 'regcollation':
           case 'regclass':
           case 'regconfig':
           case 'regdictionary':
           case 'regnamespace':
           case 'regoper':
           case 'regoperator':
           case 'regproc':
           case 'regprocedure':
           case 'regrole':
           case 'regtype':                                                         return DataTypes.PGSQL_IDENTIFIER

           case 'tid':
           case 'xid':
           case 'cid':
           case 'txid_snapshot':                                                   return DataTypes.PGSQL_IDENTIFIER

           case 'aclitem':
           case 'refcursor':                                                       return DataTypes.JSON_TYPE

		  default:
             if (dataType.indexOf('interval') === 0) {
               return 'varchar(16)';
             }
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
		     return dataType.toUpperCase();
         }
         break
       case 'MySQL':
       case 'MariaDB':
         switch (dataType.toLowerCase()) {
		   case 'char':  
			 switch (true) {
               case (dataTypeLength > DataTypes.MAX_CHARACTER_SIZE) :              return DataTypes.CLOB_TYPE
               default:                                                            return DataTypes.VARCHAR_TYPE
             }

		   case 'varchar':  
			 switch (true) {
               case (dataTypeLength > DataTypes.MAX_CHARACTER_SIZE) :              return DataTypes.CLOB_TYPE
               default:                                                            return DataTypes.CHAR_TYPE
             }

           case 'longtext':                                                        return DataTypes.CLOB_TYPE;
           case 'mediumtext':                                        
			 switch (true) {
               case (16777215 > DataTypes.MAX_CHARACTER_SIZE) :                    return DataTypes.CLOB_TYPE
               default:                                                            return `${DataTypes.VARCHAR_TYPE}(16777215)`
			 }
           case 'text':                                               
			 switch (true) {
               case (65535 > DataTypes.MAX_CHARACTER_SIZE) :                       return DataTypes.CLOB_TYPE
               default:                                                            return `${DataTypes.VARCHAR_TYPE}(65535)`
			 }
           case 'tinytext':                                                        return `${DataTypes.VARCHAR_TYPE}(256)`
		   
           case 'tinyint':                                                         return DataTypes.TINYINT_TYPE   || DataTypes.INTEGER_TYPE
           case 'smallint':                                                        return DataTypes.SMALLINT_TYPE  || DataTypes.INTEGER_TYPE
           case 'mediumint':                                                       return DataTypes.MEDIUMINT_TYPE || DataTypes.INTEGER_TYPE
           case 'int':                                                             return DataTypes.INTEGER_TYPE   || DataTypes.INTEGER_TYPE
           case 'bigint':                                                          return DataTypes.BIGINT_TYPE    || DataTypes.INTEGER_TYPE        
		   case 'decimal':                                                         return DataTypes.DECIMAL_TYPE
		   case 'float':                                                           return DataTypes.FLOAT_TYPE
		   case 'double':                                                          return DataTypes.DOUBLE_TYPE     

		   case 'boolean':                                                         return DataTypes.BOOLEAN_TYPE

		   case 'date':                                                            return DataTypes.DATE_TYPE
		   case 'time':                                                            return DataTypes.TIME_TYPE
		   case 'datetime':                                                        return DataTypes.DATETIME_TYPE
           case 'year':                                                            return DataTypes.MYSQL_YEAR_TYPE || DataTypes.YEAR_TYPE
           
		   case 'binary':  
			 switch (true) {
               case (dataTypeLength > DataTypes.MAX_BINARY_SIZE) :                 return DataTypes.BLOB_TYPE
               default:                                                            return DataTypes.BINARY_TYPE
             }

		   case 'varbinary':  
			 switch (true) {
               case (dataTypeLength > DataTypes.MAX_BINARY_SIZE) :                 return DataTypes.BLOB_TYPE
               default:                                                            return DataTypes.VARBINARY_TYPE
             }

		   case 'longblob':                                          
           case 'mediumblob':                                                      return DataTypes.BLOB_TYPE;
			 switch (true) {
               case (65535 > DataTypes.MAX_BINARY_SIZE) :                          return DataTypes.BLOB_TYPE
               default:                                                            return `${DataTypes.VARCHAR_TYPE}(65535)`
			 }
		 case 'blob':                                                              return `${DataTypes.VARBINARY_TYPE}(65535)`
		   case 'tinyblob':                                                        return `${DataTypes.VARBINARY_TYPE}(256)`
		   
           case 'json':                                                            return DataTypes.JSON_TYPE;
           case 'xml':                                                             return DataTypes.XML_TYPE;

		   case 'point':
		   case 'linestring':
		   case 'polygon':
		   case 'geometry':
		   case 'multipoint':
		   case 'multilinestring':
		   case 'multipolygon':
		   case 'geometrycollection':
		   case 'geomcollection':                                                 
		   case 'geometry':                                                        return DataTypes.SPATIAL_TYPE

           case 'set':                                                             return DataTypes.MYSQL_SET_TYPE || DataTypes.JSON_TYPE;
           case 'enum':                                                            return DataTypes.MYSQL_ENUM_TYPE || `${DataTypes.VARCHAR_TYPE}(512)`

           default:
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
	         return dataType.toUpperCase();	
         }                                                           
         break;
       case 'SNOWFLAKE':                                             
         switch (dataType.toUpperCase()) {                           
		   case 'TEXT':  
			 switch (true) {
               case (dataTypeLength > DataTypes.MAX_CHARACTER_SIZE) :              return DataTypes.CLOB_TYPE
               default:                                                            return DataTypes.CHAR_TYPE
             }

           case 'JSON':                                                            return DataTypes.JSON_TYPE;
           case 'SET':                                                             return DataTypes.JSON_TYPE;
           case 'XML':                                                             return DataTypes.XML_TYPE;
           case 'XMLTYPE':                                                         return DataTypes.XML_TYPE;
           default:
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
	         return dataType.toUpperCase();	
         }                                                                         
		 break;                                                                    
       case 'MongoDB':                                                             
         switch (dataType.toLowerCase()) {                                         
		   case 'string':  
			 switch (true) {
               case (dataTypeLength > DataTypes.MAX_CHARACTER_SIZE) :              return DataTypes.CLOB_TYPE
               default:                                                            return DataTypes.CHAR_TYPE
             }

           case 'int':                                                             return DataTypes.INTEGER_TYPE;
           case 'long':                                                            return DataTypes.BIGINT_TYPE;
           case 'decimal':                                                         return DataTypes.MONGO_DECIMAL_TYPE;
           case 'bindata':                                                         return DataTypes.VARBINARY_TYPE
		   case 'bool':                                                            return DataTypes.BOOLEAN_TYPE
		   case 'date':                                                            return DataTypes.TIMESTAMP_TZ_TYPE || DataTypes.TIMESTAMP_TYPE
		   case 'timestamp':                                                       return DataTypes.TIMESTAMP_TZ_TYPE || DataTypes.TIMESTAMP_TYPE
           case 'objectid':                                                        return DataTypes.MONGO_OBJECT_ID;
		   case 'array':                                                           
           case 'object':                                                          return DataTypes.JSON_TYPE;
           case 'null':                                                            return DataTypes.JSON_TYPE;
           case 'regex':                                                           return DataTypes.JSON_TYPE;
           case 'javascript':                                                      return DataTypes.JSON_TYPE;
           case 'javascriptWithScope':                                             return DataTypes.JSON_TYPE;
           case 'minkey':                                                          return DataTypes.JSON_TYPE;
           case 'maxKey':                                                          return DataTypes.JSON_TYPE;
           case 'undefined':                                                       
		   case 'dbPointer':                                                       
		   case 'function':                                                        
		   case 'symbol':                                                          return DataTypes.JSON_TYPE;
           // No data in the Mongo Collection                                      
           case 'json':                                                            return DataTypes.JSON_TYPE;
           default:
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
	         return dataType.toUpperCase();	
         }                                                                         
         break;		                                                               
       case 'Vertica':                                                             
         switch (dataType) {                                                       
           case 'char':                                                            
			 switch (true) {
               case (dataTypeLength > DataTypes.MAX_CHARACTER_SIZE) :              return DataTypes.CLOB_TYPE
               default:                                                            return DataTypes.CHAR_TYPE
             }

		   case 'varchar':  
           case 'long varchar':                                                    			 
             switch (true) {                                                       
               case (dataTypeLength > DataTypes.MAX_CHARACTER_SIZE):               return DataTypes.CLOB_TYPE;
               default:                                                            return DataTypes.VARCHAR_TYPE;
             }                    
			 
           case 'binary':                                                          
             switch (true) {                                                       
               case (dataTypeLength > DataTypes.MAX_BINARY_SIZE):                  return DataTypes.BLOB_TYPE;
               default:                                                            return DataTypes.BINARY_TYPE;
             }                                                                     

           case 'varbinary':                                                       
           case 'long varbinary':                                                  
             switch (true) {                                                       
               case (dataTypeLength > this.dbi.MAX_BINARY_SIZE):                   return DataTypes.BLOB_TYPE;
               default:                                                            return DataTypes.VARBINARY_TYPE;
             }                                                                     
			 
           case 'numeric':                                                         
             switch (true) {                                                       
               default:                                                            return DataTypes.DECIMAL_TYPE                                                    
             }                                                                     
           case 'year':                                                            return DataTypes.VERTICA_YEAR_TYPE || DataTypes.YEAR_TYPE
           case 'float':                                                           return DataTypes.DOUBLE_TYPE
           case 'time':                                                            return DataTypes.TIME_TYPE;   
           case 'timetz':                                                          return DataTypes.TIME_TZ_TYPE
           case 'timestamptz':                                                     return DataTypes.TIMESTAMP_TZ_TYPE || DataTypes.TIMESTAMP_TYPE
           case 'timestamp':                                                       return DataTypes.TIMESTAMP_TYPE
           case 'xml':                                                             return DataTypes.XML_TYPE
           case 'json':                                                            return DataTypes.JSON_TYPE
           case 'uuid':                                                            return DataTypes.UUID_TYPE
           case 'geometry':                                                                       
           case 'geography':                                                       return DataTypes.SPATIAL_TYPE
           default:                                                                                 
             if (dataType.indexOf('interval') === 0) {                             
               return DataTypes.INTERVAL_TYPE;                            
             }
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
	         return dataType.toUpperCase();	
         }
         break;
       case 'Teradata':
         switch (dataType) {
           case 'CF':                                                              return DataTypes.CHAR_TYPE
           case 'CV':                                                              return DataTypes.VARCHAR_TYPE
           case 'N':                                                               return DataTypes.NUMERIC_TYPE
           case 'I':                                                               return DataTypes.INTEGER_TYPE
           case 'I2':                                                              return DataTypes.SMALLINT_TYPE || DataTypes.INTEGER_TYPE
           case 'I4':                                                              return DataTypes.INTEGER_TYPE 
           case 'I8':                                                              return DataTypes.BIGINT_TYPE || DataTypes.INTEGER_TYPE
           case 'F':                                                               return DataTypes.DOUBLE_TYPE
           case 'DA':                                                              return DataTypes.DATE_TYPE
           default:                                                                return dataType.toUpperCase()
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
		     return dataType.toUpperCase();
         }
         break
       default:
         this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
	     return dataType.toUpperCase();
    }
  }
  
  getDataTypeMapping(DataTypes,vendor,type,length,scale) {
	  
    const targetDataType = this.mapForeignDataType(DataTypes,vendor,type,length,scale);
	   
	if (targetDataType === undefined) {
	  this.yadamuLogger.logInternalError(['mapForeignDataType()'],`Missing Mapping for "${type}" in mappings for "${vendor}".`)
    }
	
	return targetDataType
  }
	   
}

export { YadamuStatementGenerator as default }
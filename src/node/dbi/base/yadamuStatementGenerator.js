class YadamuStatementGenerator {
	
  addDataTypeDefaults = (superclass) => class extends superclass {
																								       			           
	 static NCHAR_TYPE                       = superclass.hasOwnProperty('NCHAR_TYPE')                    ? superclass.NCHAR_TYPE                      : superclass.CHAR_TYPE
																																			           
	 static NVARCHAR_TYPE                    = superclass.hasOwnProperty('NVARCHAR_TYPE')                 ? superclass.NVARCHAR_TYPE                   : superclass.VARCHAR_TYPE
	 
	 static VARBINARY_TYPE                   = superclass.hasOwnProperty('VARBINARY_TYPE')                ? superclass.VARBINARY_TYPE                  : superclass.BINARY_TYPE

	 static BOOLEAN_TYPE                     = superclass.hasOwnProperty('BOOLEAN_TYPE')                  ? superclass.BOOLEAN_TYPE                    : `${superclass.BINARY_TYPE}(1)`
										     																								           
	 static SINGLE_BIT_TYPE                  = superclass.hasOwnProperty('SINGLE_BIT_TYPE')               ? superclass.SINGLE_BIT_TYPE                 : `${superclass.BINARY_TYPE}(1)`

	 static BIT_STRING_TYPE                  = superclass.hasOwnProperty('BIT_STRING_TYPE')               ? superclass.BIT_STRING_TYPE                 : superclass.BINARY_TYPE

	 static VARBIT_STRING_TYPE               = superclass.hasOwnProperty('VARBIT_STRING_TYPE')            ? superclass.VARBIT_STRING_TYPE              : this.VARBINARY_TYPE
																																			           
	 static NCLOB_TYPE                       = superclass.hasOwnProperty('NCLOB_TYPE')                    ? superclass.NCLOB_TYPE                      : superclass.CLOB_TYPE
																																			           
	 static TINYINT_TYPE                     = superclass.hasOwnProperty('TINYINT_TYPE')                  ? superclass.TINYINT_TYPE                    : superclass.INTEGER_TYPE
																																			           
	 static SMALLINT_TYPE                    = superclass.hasOwnProperty('SMALLINT_TYPE')                 ? superclass.SMALLINT_TYPE                   : superclass.INTEGER_TYPE
																																			           
	 static SMALLINT_TYPE                    = superclass.hasOwnProperty('SMALLINT_TYPE')                 ? superclass.SMALLINT_TYPE                   : superclass.INTEGER_TYPE
																																			           
	 static MEDIUMINT_TYPE                   = superclass.hasOwnProperty('MEDIUMINT_TYPE')                ? superclass.MEDIUMINT_TYPE                  : superclass.INTEGER_TYPE
																																			           
     static BIGINT_TYPE                      = superclass.hasOwnProperty('BIGINT_TYPE')                   ? superclass.BIGINT_TYPE                     : superclass.INTEGER_TYPE
																																			           
	 static REAL_TYPE                        = superclass.hasOwnProperty('REAL_TYPE')                     ? superclass.REAL_TYPE                       : superclass.FLOAT_TYPE
																																			           
	 static DOUBLE_TYPE                      = superclass.hasOwnProperty('REAL_TYPE')                     ? superclass.DOUBLE_TYPE                     : superclass.FLOAT_TYPE
																																			           
     static JSON_TYPE                        = superclass.hasOwnProperty('JSON_TYPE')                     ? superclass.JSON_TYPE                       : superclass.CLOB_TYPE
																																			           
     static XML_TYPE                         = superclass.hasOwnProperty('XML_TYPE')                      ? superclass.XML_TYPE                        : superclass.CLOB_TYPE
	 
     static SPATIAL_TYPE                     = superclass.hasOwnProperty('SPATIAL_TYPE')                  ? superclass.SPATIAL_TYPE                    : this.JSON_TYPE

     static UUID_TYPE                        = superclass.hasOwnProperty('UUID_TYPE')                     ? superclass.UUID_TYPE                       : `${superclass.VARCHAR_TYPE}(36)`
																																			           
     static TIMESTAMP_TZ_TYPE                = superclass.hasOwnProperty('TIMESTAMP_TZ_TYPE')             ? superclass.TIMESTAMP_TZ_TYPE               : superclass.TIMESTAMP_TYPE
																																			           
     static TIMESTAMP_LTZ_TYPE               = superclass.hasOwnProperty('TIMESTAMP_LTZ_TYPE')            ? superclass.TIMESTAMP_LTZ_TYPE              : this.TIMESTAMP_TZ_TYPE
																																			           
     static ORACLE_ROWID_TYPE                = superclass.hasOwnProperty('ORACLE_ROWID_TYPE')             ? superclass.ORACLE_ROWID_TYPE               : `${superclass.VARCHAR_TYPE}(32)`
										     																								           
     static ORACLE_BFILE_TYPE                = superclass.hasOwnProperty('ORACLE_BFILE_TYPE')             ? superclass.ORACLE_BFILE_TYPE               : `${superclass.VARCHAR_TYPE}(2048)`
										     																								           
     static ORACLE_UNBOUNDED_NUMBER_TYPE     = superclass.hasOwnProperty('ORACLE_UNBOUNDED_NUMBER_TYPE')  ? superclass.ORACLE_UNBOUNDED_NUMBER_TYPE    : superclass.MAX_NUMERIC_TYPE
										     																								           
     static MSSQL_MONEY_TYPE                 = superclass.hasOwnProperty('MSSQL_MONEY_TYPE')              ? superclass.MSSQL_MONEY_TYPE                : `${superclass.DECIMAL_TYPE}(19,4)`
										     																								           
     static MSSQL_SMALL_MONEY_TYPE           = superclass.hasOwnProperty('MSSQL_SMALL_MONEY_TYPE')        ? superclass.MSSQL_SMALL_MONEY_TYPE          : `${superclass.DECIMAL_TYPE}(10,4)`
										     																								           
     static MSSQL_ROWVERSION_TYPE            = superclass.hasOwnProperty('MSSQL_ROWVERSION_TYPE')         ? superclass.MSSQL_ROWVERSION_TYPE           : `${superclass.BINARY_TYPE}(8)`
										     																								           
     static MSSQL_HIERARCHY_TYPE             = superclass.hasOwnProperty('MSSQL_HIERARCHY_TYPE')          ? superclass.MSSQL_HIERARCHY_TYPE            : `${superclass.VARCHAR_TYPE}(4000)`
										     																								           
     static PGSQL_MONEY_TYPE                 = superclass.hasOwnProperty('PGSQL_MONEY_TYPE')              ? superclass.PGSQL_MONEY_TYPE                : `${superclass.DECIMAL_TYPE}(21,2)`
										     																								           
     static PGSQL_NAME_TYPE                  = superclass.hasOwnProperty('PGSQL_NAME_TYPE')               ? superclass.PGSQL_NAME_TYPE                 : `${superclass.VARCHAR_TYPE}(64)`
										     																								           
     static PGSQL_SINGLE_CHAR_TYPE           = superclass.hasOwnProperty('PGSQL_SINGLE_CHAR_TYPE')        ? superclass.PGSQL_SINGLE_CHAR_TYPE          : `${superclass.CHAR_TYPE}(1)`
										     																								           
     static PGSQL_INET_ADDR_TYPE             = superclass.hasOwnProperty('PGSQL_INET_ADDR_TYPE')          ? superclass.PGSQL_INET_ADDR_TYPE            : `${superclass.VARCHAR_TYPE}(39)`
										     																								           
     static PGSQL_MAC_ADDR_TYPE              = superclass.hasOwnProperty('PGSQL_MAC_ADDR_TYPE')           ? superclass.PGSQL_MAC_ADDR_TYPE             : `${superclass.VARCHAR_TYPE}(23)`
										     																								           
     static PGSQL_IDENTIFIER                 = superclass.hasOwnProperty('PGSQL_IDENTIFIER')              ? superclass.PGSQL_IDENTIFIER                : `${superclass.BINARY_TYPE}(4)`
										     																								           
     static MYSQL_SET_TYPE                   = superclass.hasOwnProperty('MYSQL_SET_TYPE')                ? superclass.MYSQL_SET_TYPE                  : this.JSON_TYPE
										     																								           
	 static MYSQL_ENUM_TYPE                  = superclass.hasOwnProperty('MYSQL_ENUM_TYPE')               ? superclass.MYSQL_ENUM_TYPE                 : `${superclass.VARCHAR_TYPE}(256)`
										     																								           
     static MONGO_OBJECT_ID                  = superclass.hasOwnProperty('MONGO_OBJECT_ID')               ? superclass.MONGO_OBJECT_ID                 : `${superclass.BINARY_TYPE}(12)`
										     																								           
	 static MONGO_REGEX_TYPE                 = superclass.hasOwnProperty('MONGO_REGEX_TYPE')              ? superclass.MONGO_REGEX_TYPE                : `${superclass.VARCHAR_TYPE}(2048)`

	 static MONGO_DECIMAL_TYPE               = superclass.hasOwnProperty('MONGO_DECIMAL_TYPE')            ? superclass.MONGO_DECIMAL_TYPE              : superclass.MAX_NUMERIC_TYPE
	 
	 static DECIMAL_PRECISION                = superclass.hasOwnProperty('DECIMAL_PRECISION')             ? superclass.DECIMAL_PRECISION               : 38

  }

  constructor(DataTypes, dbi, targetSchema, metadata, yadamuLogger) {

    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;
	this.DataTypes = this.addDataTypeDefaults(DataTypes)
  }

  mapForeignDataType(vendor, dataType, dataTypeLength, dataTypesize) {
     switch (vendor) {
       case 'Oracle':
         switch (dataType) {

           case 'CHAR':                                                            return this.DataTypes.CHAR_TYPE
           case 'NCHAR':                                                           return this.DataTypes.NVARCHAR_TYPE
           case 'VARCHAR2':                                                        return this.DataTypes.VARCHAR_TYPE
           case 'NVARCHAR2':                                                       return this.DataTypes.NVARCHAR_TYPE
           
		   case 'NUMBER':                                                          return this.DataTypes.NUMERIC_TYPE
           case 'BINARY_FLOAT':                                                    return this.DataTypes.FLOAT_TYPE
           case 'BINARY_DOUBLE':                                                   return this.DataTypes.DOUBLE_TYPE

           case 'CLOB':                                                            return this.DataTypes.CLOB_TYPE
           case 'NCLOB':                                                           return this.DataTypes.NCLOB_TYPE

           case 'BOOLEAN':                                                         return this.DataTypes.BOOLEAN_TYPE
           case 'RAW':                                                             return this.DataTypes.VARBINARY_TYPE
           case 'BLOB':                                                            return this.DataTypes.BLOB_TYPE

           case 'DATE':                                                            return this.DataTypes.DATE_TYPE
           case 'TIMESTAMP':
             switch (true) {
               default:                                                            return this.DataTypes.TIMESTAMP_TYPE
             }

           case 'BFILE':                                                           return this.DataTypes.ORACLE_BFILE_TYPE
           case 'ROWID':                                                           return this.DataTypes.ORACLE_ROWID_TYPE

           case 'JSON':                                                            return this.DataTypes.JSON_TYPE
           case 'XMLTYPE':                                                         return this.DataTypes.XML_TYPE
           case '"MDSYS"."SDO_GEOMETRY"':                                          return this.DataTypes.SPATIAL_TYPE
           case 'ANYDATA':                                                         return this.DataTypes.CLOB_TYPE
		   
           default :
             if (dataType.indexOf('LOCAL TIME ZONE') > -1) {
               return this.DataTypes.TIMESTAMP_LTZ_TYPE 
             }
             if (dataType.indexOf('TIME ZONE') > -1) {
               return this.DataTypes.TIMESTAMP_TZ_TYPE 
             }
             if (dataType.indexOf('INTERVAL') === 0) {
               return 'VARCHAR(16)';
             }
             if (dataType.indexOf('XMLTYPE') > -1) {
               return this.DataTypes.XML_TYPE
             }
             if (dataType.indexOf('.') > -1) {
               return this.DataTypes.CLOB_TYPE
             }
			 this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
             return this.DataType.toUpperCase();
         }
         break;
       case 'MSSQLSERVER':
         switch (dataType) {
           case 'char':
             switch (true) {
               case (dataTypeLength === -1):                                       return this.DataTypes.CLOB_TYPE
               case (dataTypeLength > this.DataTypes.MAX_CHARACTER_SIZE) :         return this.DataTypes.CLOB_TYPE
               default:                                                            return this.DataTypes.CHAR_TYPE
             }
           case 'nchar':
             switch (true) {
               case (dataTypeLength === -1):                                       return this.DataTypes.NCLOB_TYPE
               case (dataTypeLength > this.DataTypes.MAX_CHARACTER_SIZE) :         return this.DataTypes.NCLOB_TYPE
               default:                                                            return this.DataTypes.NCHAR_TYPE
             }
           case 'varchar':
             switch (true) {
               case (dataTypeLength === -1):                                       return this.DataTypes.CLOB_TYPE
               case (dataTypeLength > this.DataTypes.MAX_CHARACTER_SIZE) :         return this.DataTypes.CLOB_TYPE
               default:                                                            return this.DataTypes.VARCHAR_TYPE
             }
           case 'nvarchar':
             switch (true) {
               case (dataTypeLength === -1):                                       return this.DataTypes.NCLOB_TYPE
               case (dataTypeLength > this.DataTypes.MAX_CHARACTER_SIZE) :         return this.DataTypes.NCLOB_TYPE
			 default:                                                              return this.DataTypes.NVARCHAR_TYPE
             }
           case 'text':                                                            return this.DataTypes.CLOB_TYPE 
           case 'ntext':                                                           return this.DataTypes.NCLOB_TYPE 


           case 'tinyint':                                                         return this.DataTypes.TINYINT_TYPE 
		   case 'smallint':                                                        return this.DataTypes.SMALLINT_TYPE   
           case 'mediumint':                                                       return this.DataTypes.MEDIUMINT_TYPE   
		   case 'int':
		   case 'integer':                                                         return this.DataTypes.INTEGER_TYPE
           case 'bigint':                                                          return this.DataTypes.BIGINT_TYPE     
           case 'smallmoney':                                                      return this.DataTypes.MSSQL_SMALL_MONEY_TYPE 
           case 'money':                                                           return this.DataTypes.MSSQL_MONEY_TYPE
           case 'real':                                                            return this.DataTypes.FLOAT_TYPE
           case 'float':                                                           return this.DataTypes.DOUBLE_TYPE
		   case 'numeric':                                                         return this.DataTypes.DECIMAL_TYPE

           case 'date':                                                            return this.DataTypes.DATE_TYPE
           case 'time':                                                            return this.DataTypes.TIME_TYPE
           case 'smalldate':                                                       return this.DataTypes.DATETIME_TYPE
           case 'datetime':                                                        return this.DataTypes.DATETIME_TYPE
           case 'datetime2':
             switch (true) {
                // case (dataTypeLength > 6):                                      return 'datetime(6)';
                default:                                                           return this.DataTypes.DATETIME_TYPE
             }
           case 'datetimeoffset':                                                  return this.DataTypes.DATETIME_TYPE

           case 'bit':                                                             return this.DataTypes.SINGLE_BIT_TYPE
           case 'binary':
             switch (true) {
               case (dataTypeLength === -1):                                       return this.DataTypes.BLOB_TYPE
               case (dataTypeLength > this.DataTypes.MAX_BINARY_SIZE):             return this.DataTypes.BLOB_TYPE
               default:                                                            return this.DataTypes.BINARY_TYPE
             }
           case 'varbinary':
             switch (true) {
               case (dataTypeLength === -1):                                       return this.DataTypes.BLOB_TYPE
               case (dataTypeLength > this.DataTypes.MAX_BINARY_SIZE):             return this.DataTypes.BLOB_TYPE
               default:                                                            return this.DataTypes.VARBINARY_TYPE
             }
           case 'image':                                                           return this.DataTypes.BLOB_TYPE

           case 'rowversion':                                                      return this.DataTypes.MSSQL_ROWVERSION_TYPE
           case 'hierarchyid':                                                     return this.DataTypes.MSSQL_HEIRARCHY_TYPE
           case 'uniqueidentifier':                                                return this.DataTypes.UUID_TYPE

           case 'xml':                                                             return this.DataTypes.XML_TYPE
           case 'JSON':                                                            return this.DataTypes.JSON_TYPE
           case 'geography':                                                       return this.DataTypes.SPATIAL_TYPE
           case 'geometry':                                                        return this.DataTypes.SPATIAL_TYPE


           default:
			 this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
		     return this.DataType.toUpperCase();
         }
         break;
       case 'Postgres':
	     switch (dataType) {
           case 'character':                                                       
             switch (true) {
               case (dataTypeLength === undefined):                                return this.DataTypes.CLOB_TYPE
               case (dataTypeLength > this.DataTypes.MAX_CHARACTER_SIZE) :         return this.DataTypes.CLOB_TYPE
               default:                                                            return this.DataTypes.CHAR_TYPE
             }
           case 'character varying':
             switch (true) {
               case (dataTypeLength === undefined):                                return this.DataTypes.CLOB_TYPE
               case (dataTypeLength > this.DataTypes.MAX_CHARACTER_SIZE) :         return this.DataTypes.CLOB_TYPE
               default:                                                            return this.DataTypes.VARCHAR_TYPE
             }
           case 'name':                                                            return this.DataTypes.PGSQL_NAME_TYPE
		   
		   case 'smallint':                                                        return this.DataTypes.SMALLINT_TYPE
           case 'integer':                                                         return this.DataTypes.INTEGER_TYPE
		   case 'bigint':                                                          return this.DataTypes.BIGINT_TYPE
           case 'real':                                                            return this.DataTypes.FLOAT_TYPE
           case 'double precision':                                                return this.DataTypes.DOUBLE_TYPE
		   case 'numeric':                                                         return this.DataTypes.NUMERIC_TYPE
		   case 'decimal':                                                         return this.DataTypes.DECIMAL_TYPE
           case 'money':                                                           return this.DataTypes.PGSQL_MONEY_TYPE
		   
           case 'date':                                                            return this.DataTypes.DATE_TYPE
           case 'timestamp':                                                       
           case 'timestamp without time zone':                                     return this.DataTypes.TIMESTAMP_TYPE
           case 'timestamp with time zone':                                        return this.DataTypes.TIMESTAMP_TZ_TYPE
           case 'time without time zone':                                          return this.DataTypes.TIME_TYPE
           case 'time with time zone':                                             return this.DataTypes.TIME_TZ_TYPE

           case 'boolean':                                                         return this.DataTypes.BOOLEAN_TYPE
           case 'bit':                                                             return this.DataTypes.BIT_TYPE
           case 'bit varying':                                                     
		     switch (true) {
               case (dataTypeLength === undefined):                                return this.DataTypes.BLOB_TYPE
               case (dataTypeLength/8 > this.DataTypes.MAX_BINARY_SIZE):           return this.DataTypes.BLOB_TYPE
               default:                                                            return this.DataTypes.VARBIT_TYPE
		     }
		   case 'bytea':
             switch (true) {
               case (dataTypeLength === undefined):                                return this.DataTypes.BLOB_TYPE
               case (dataTypeLength > this.DataTypes.MAX_BINARY_SIZE):             return this.DataTypes.BLOB_TYPE
               default:                                                            return this.DataTypes.VARBINARY_TYPE
             }
           
           case 'uuid':                                                            return this.DataTypes.UUID_TYPE
           case 'text':                                                            return this.DataTypes.CLOB_TYPE
           case 'xml':                                                             return this.DataTypes.XML_TYPE
           case 'json':                                                            return this.DataTypes.JSON_TYPE
           case 'jsonb':                                                           return this.DataTypes.JSON_TYPE

           case 'point':
           case 'lseg':
           case 'path':
           case 'box':
           case 'polygon':
           case 'geography':                                                       return this.DataTypes.SPATIAL_TYPE
           case 'line':                                                            return this.DataTypes.JSON_TYPE
           case 'circle':                                                          return this.dbi.INBOUND_CIRCLE_FORMAT === 'CIRCLE' ? this.DataTypes.JSON_TYPE : this.DataTypes.SPATIAL_TYPE

           case 'cidr':
           case 'inet':                                                            return this.DataTypes.PGSQL_INET_ADDR_TYPE
           case 'macaddr':
           case 'macaddr8':                                                        return this.DataTypes.PGSQL_MAC_ADDR_TYPE


		   case 'int4range':
           case 'int8range':
           case 'numrange':
           case 'tsrange':
           case 'tstzrange':
           case 'daterange':                                                       return this.DataTypes.JSON_TYPE
           case 'tsvector':
           case 'gtsvector':                                                       return this.DataTypes.JSON_TYPE

           case 'tsquery':                                                         return this.DataTypes.MAX_VARCHAR_TYPE

           case 'oid':                                                             return this.DataTypes.PGSQL_IDENTIFIER

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
           case 'regtype':                                                         return this.DataTypes.PGSQL_IDENTIFIER

           case 'tid':
           case 'xid':
           case 'cid':
           case 'txid_snapshot':                                                   return this.DataTypes.PGSQL_IDENTIFIER

           case 'aclitem':
           case 'refcursor':                                                       return this.DataTypes.JSON_TYPE

		  default:
             if (dataType.indexOf('interval') === 0) {
               return 'varchar(16)';
             }
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
		     return this.DataType.toUpperCase();
         }
         break
       case 'MySQL':
       case 'MariaDB':
         switch (dataType.toLowerCase()) {
		   case 'char':  
			 switch (true) {
               case (dataTypeLength > this.DataTypes.MAX_CHARACTER_SIZE) :         return this.DataTypes.CLOB_TYPE
               default:                                                            return this.DataTypes.VARCHAR_TYPE
             }

		   case 'varchar':  
			 switch (true) {
               case (dataTypeLength > this.DataTypes.MAX_CHARACTER_SIZE) :         return this.DataTypes.CLOB_TYPE
               default:                                                            return this.DataTypes.CHAR_TYPE
             }

           case 'longtext':                                                        return this.DataTypes.CLOB_TYPE
           case 'mediumtext':                                        
			 switch (true) {
               case (16777215 > this.DataTypes.MAX_CHARACTER_SIZE) :               return this.DataTypes.CLOB_TYPE
               default:                                                            return `${DataTypes.VARCHAR_TYPE}(16777215)`
			 }
           case 'text':                                               
			 switch (true) {
               case (65535 > this.DataTypes.MAX_CHARACTER_SIZE) :                  return this.DataTypes.CLOB_TYPE
               default:                                                            return `${DataTypes.VARCHAR_TYPE}(65535)`
			 }
			 
           case 'tinytext':                                                        return `${DataTypes.VARCHAR_TYPE}(256)`
		   
           case 'tinyint':                                                         return this.DataTypes.TINYINT_TYPE
           case 'smallint':                                                        return this.DataTypes.SMALLINT_TYPE 
           case 'mediumint':                                                       return this.DataTypes.MEDIUMINT_TYPE 
           case 'int':                                                             return this.DataTypes.INTEGER_TYPE
           case 'bigint':                                                          return this.DataTypes.BIGINT_TYPE
		   
		   case 'decimal':                                                         
			 switch (true) {
               case (dataTypeLength > this.DataTypes.DECIMAL_PRECISION):           return this.DataTypes.MAX_NUMERIC_TYPE
               default:                                                            return this.DataTypes.DECIMAL_TYPE
			 }
		  
		   case 'float':                                                           return this.DataTypes.FLOAT_TYPE
		   case 'double':                                                          return this.DataTypes.DOUBLE_TYPE     

		   case 'boolean':                                                         return this.DataTypes.BOOLEAN_TYPE

		   case 'date':                                                            return this.DataTypes.DATE_TYPE
		   case 'time':                                                            return this.DataTypes.TIME_TYPE
		   case 'datetime':                                                        return this.DataTypes.DATETIME_TYPE
           case 'year':                                                            return this.DataTypes.MYSQL_YEAR_TYPE || this.DataTypes.YEAR_TYPE
           
		   case 'binary':  
			 switch (true) {
               case (dataTypeLength > this.DataTypes.MAX_BINARY_SIZE) :            return this.DataTypes.BLOB_TYPE
               default:                                                            return this.DataTypes.BINARY_TYPE
             }

		   case 'varbinary':  
			 switch (true) {
               case (dataTypeLength > this.DataTypes.MAX_BINARY_SIZE) :            return this.DataTypes.BLOB_TYPE
               default:                                                            return this.DataTypes.VARBINARY_TYPE
             }

		   case 'longblob':                                          
           case 'mediumblob':                                                      return this.DataTypes.BLOB_TYPE
			 switch (true) {
               case (65535 > this.DataTypes.MAX_BINARY_SIZE) :                     return this.DataTypes.BLOB_TYPE
               default:                                                            return `${DataTypes.VARCHAR_TYPE}(65535)`
			 }
		 case 'blob':                                                              return `${DataTypes.VARBINARY_TYPE}(65535)`
		   case 'tinyblob':                                                        return `${DataTypes.VARBINARY_TYPE}(256)`
		   
           case 'json':                                                            return this.DataTypes.JSON_TYPE 
           case 'xml':                                                             return this.DataTypes.XML_TYPE 

		   case 'point':
		   case 'linestring':
		   case 'polygon':
		   case 'geometry':
		   case 'multipoint':
		   case 'multilinestring':
		   case 'multipolygon':
		   case 'geometrycollection':
		   case 'geomcollection':                                                 
		   case 'geometry':                                                        return this.DataTypes.SPATIAL_TYPE

           case 'set':                                                             return this.DataTypes.MYSQL_SET_TYPE || this.DataTypes.JSON_TYPE
           case 'enum':                                                            return this.DataTypes.MYSQL_ENUM_TYPE || `${DataTypes.VARCHAR_TYPE}(512)`

           default:
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
	         return this.DataType.toUpperCase();	
         }                                                           
         break;
       case 'SNOWFLAKE':                                             
         switch (dataType.toUpperCase()) {                           
		   case 'TEXT':  
			 switch (true) {
               case (dataTypeLength > this.DataTypes.MAX_CHARACTER_SIZE) :         return this.DataTypes.CLOB_TYPE
               default:                                                            return this.DataTypes.CHAR_TYPE
             }

           case 'JSON':                                                            return this.DataTypes.JSON_TYPE
           case 'SET':                                                             return this.DataTypes.JSON_TYPE
           case 'XML':                                                             return this.DataTypes.XML_TYPE
           case 'XMLTYPE':                                                         return this.DataTypes.XML_TYPE 
           default:
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
	         return this.DataType.toUpperCase();	
         }                                                                         
		 break;                                                                    
       case 'MongoDB':                                                             
         switch (dataType.toLowerCase()) {                                         
		   case 'string':  
			 switch (true) {
               case (dataTypeLength > this.DataTypes.MAX_CHARACTER_SIZE) :         return this.DataTypes.CLOB_TYPE
               default:                                                            return this.DataTypes.CHAR_TYPE
             }

           case 'int':                                                             return this.DataTypes.INTEGER_TYPE
           case 'long':                                                            return this.DataTypes.BIGINT_TYPE
           case 'decimal':                                                         return this.DataTypes.MONGO_DECIMAL_TYPE
           case 'double':                                                          return this.DataTypes.DOUBLE_TYPE
           case 'bindata':                                                         return this.DataTypes.VARBINARY_TYPE
		   case 'bool':                                                            return this.DataTypes.BOOLEAN_TYPE
		   case 'bool':                                                            return this.DataTypes.BOOLEAN_TYPE
		   case 'date':                                                            return this.DataTypes.TIMESTAMP_TZ_TYPE 
		   case 'timestamp':                                                       return this.DataTypes.TIMESTAMP_TZ_TYPE 
           case 'objectid':                                                        return this.DataTypes.MONGO_OBJECT_ID;
		   case 'array':                                                           
           case 'object':                                                          return this.DataTypes.JSON_TYPE 
           case 'null':                                                            return this.DataTypes.JSON_TYPE 
           case 'regex':                                                           return this.DataTypes.JSON_TYPE 
           case 'javascript':                                                      return this.DataTypes.JSON_TYPE 
           case 'javascriptWithScope':                                             return this.DataTypes.JSON_TYPE 
           case 'minkey':                                                          return this.DataTypes.JSON_TYPE 
           case 'maxKey':                                                          return this.DataTypes.JSON_TYPE 
           case 'undefined':                                                       
		   case 'dbPointer':                                                       
		   case 'function':                                                        
		   case 'symbol':                                                          return this.DataTypes.JSON_TYPE
           // No data in the Mongo Collection                                      
           case 'json':                                                            return this.DataTypes.JSON_TYPE
	       deafault:
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
	         return this.DataType.toUpperCase();	
         }                                                                         
         break;		                                                               
       case 'Vertica':                                                             
         switch (dataType) {                                                       
           case 'char':                                                            
			 switch (true) {
               case (dataTypeLength > this.DataTypes.MAX_CHARACTER_SIZE) :         return this.DataTypes.CLOB_TYPE
               default:                                                            return this.DataTypes.CHAR_TYPE
             }

		   case 'varchar':  
           case 'long varchar':                                                    			 
             switch (true) {                                                       
               case (dataTypeLength > this.DataTypes.MAX_CHARACTER_SIZE):          return this.DataTypes.CLOB_TYPE
               default:                                                            return this.DataTypes.VARCHAR_TYPE
             }                    
			 
           case 'binary':                                                          
             switch (true) {                                                       
               case (dataTypeLength > this.DataTypes.MAX_BINARY_SIZE):             return this.DataTypes.BLOB_TYPE
               default:                                                            return this.DataTypes.BINARY_TYPE
             }                                                                     

           case 'varbinary':                                                       
           case 'long varbinary':                                                  
             switch (true) {                                                       
               case (dataTypeLength > this.dbi.MAX_BINARY_SIZE):                   return this.DataTypes.BLOB_TYPE
               default:                                                            return this.DataTypes.VARBINARY_TYPE
             }                                                                     
			 
           case 'numeric':                                                         
             switch (true) {                                                       
               default:                                                            return this.DataTypes.DECIMAL_TYPE                                                    
             }                                                                     
           case 'year':                                                            return this.DataTypes.VERTICA_YEAR_TYPE || this.DataTypes.YEAR_TYPE
           case 'float':                                                           return this.DataTypes.DOUBLE_TYPE
           case 'time':                                                            return this.DataTypes.TIME_TYPE   
           case 'timetz':                                                          return this.DataTypes.TIME_TZ_TYPE
           case 'timestamptz':                                                     return this.DataTypes.TIMESTAMP_TZ_TYPE
           case 'timestamp':                                                       return this.DataTypes.TIMESTAMP_TYPE
           case 'xml':                                                             return this.DataTypes.XML_TYPE 
           case 'json':                                                            return this.DataTypes.JSON_TYPE
           case 'uuid':                                                            return this.DataTypes.UUID_TYPE
           case 'geometry':                                                                       
           case 'geography':                                                       return this.DataTypes.SPATIAL_TYPE
           default:                                                                                 
             if (dataType.indexOf('interval') === 0) {                             
               return this.DataTypes.INTERVAL_TYPE                            
             }
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
	         return this.DataType.toUpperCase();	
         }
         break;
       case 'Teradata':
         switch (dataType) {
           case '++':                                                              return this.DataTypes.JSON_TYPE // ANY TY{E
           case 'A1':                                                              return this.DataTypes.JSON_TYPE // SINGLE DIMENSIONAL ARRAY
           case 'AN':                                                              return this.DataTypes.JSON_TYPE // MULTI DIMENSIONAL ARRAY
           case 'AT':                                                              return this.DataTypes.TIME_TYPE
           case 'BO':                                                              return this.DataTypes.BLOB_TYPE
           case 'BF':                                                              return this.DataTypes.BINARY_TYPE
           case 'BV':                                                              return this.DataTypes.VARBINARY_TYPE
           case 'CF':                                                              return this.DataTypes.CHAR_TYPE
           case 'CV':                                                              return this.DataTypes.VARCHAR_TYPE
           case 'CO':                                                              return this.DataTypes.CLOB_TYPE
           case 'D':                                                               return this.DataTypes.DECIMAL_TYPE
           case 'DA':                                                              return this.DataTypes.DATE_TYPE
           case 'DY':                                                              return this.DataTypes.INTERVAL_DAY_TYPE              || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.DataTypes.INTERVAL_TYPE      // INTERVAL DAY
           case 'DH':                                                              return this.DataTypes.INTERVAL_DAY_TO_HOUR_TYPE      || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.DataTypes.INTERVAL_TYPE      // INTERVAL DAY TO HOUR
           case 'DM':                                                              return this.DataTypes.INTERVAL_DAY_TO_MINUTE_TYPE    || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.DataTypes.INTERVAL_TYPE      // INTERVAL DAY TO MINUTE
           case 'DS':                                                              return this.DataTypes.INTERVAL_DAY_TO_SECOND_TYPE    || this.DataTypes.INTERVAL_TYPE                                              // INTERVAL DAY TO SECOND
           case 'HR':                                                              return this.DataTypes.INTERVAL_HOUR_TYPE             || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.DataTypes.INTERVAL_TYPE      // INTERVAL HOUR
           case 'HM':                                                              return this.DataTypes.INTERVAL_HOUR_TO_MINUTE_TYPE   || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.DataTypes.INTERVAL_TYPE      // INTERVAL HOUR TO MINUTE
           case 'HS':                                                              return this.DataTypes.INTERVAL_HOUR_TO_SECOND_TYPE   || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.DataTypes.INTERVAL_TYPE      // INTERVAL HOUR TO SECOND
           case 'F':                                                               return this.DataTypes.DOUBLE_TYPE
           case 'I':                                                               return this.DataTypes.INTEGER_TYPE
           case 'I1':                                                              return this.DataTypes.TINYINY_TYPE
           case 'I2':                                                              return this.DataTypes.SMALLINT_TYPE
           case 'I4':                                                              return this.DataTypes.INTEGER_TYPE 
           case 'I8':                                                              return this.DataTypes.BIGINT_TYPE
           case 'JN':                                                              return this.DataTypes.JSON_TYPE
           case 'MI':                                                              return this.DataTypes.INTERVAL_MINUTE_TYPE           || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.DataTypes.INTERVAL_TYPE      // INTERVAL MINUTE
           case 'MS':                                                              return this.DataTypes.INTERVAL_MINUTE_TO_SECOND_TYPE || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.DataTypes.INTERVAL_TYPE      // INTERVAL MINUTE TO SECOND
           case 'N':                                                               return this.DataTypes.NUMERIC_TYPE
		   case 'PD':                                                              return this.DataTypes.PERIOD_DAY_TYPE
		   case 'PT':                                                              return this.DataTypes.PERIOD_TIME_TYPE
		   case 'PZ':                                                              return this.DataTypes.PERIOD_TIME_TZ_TYPE
		   case 'PS':                                                              return this.DataTypes.PERIOD_TIMESTAMP_TYPE
           case 'PM':                                                              return this.DataTypes.PERIOD_TIMESTAMP_TZ_TYPE
           case 'N':                                                               return this.DataTypes.NUMERIC_TYPE
           case 'SC':                                                              return this.DataTypes.INTERVAL_SECOND_TYPE           || DataTyes.INTERVAL_DAY_TO_SECOND_TYPE || this.DataTypes.INTERVAL_TYPE      // INTERVAL SECOND
           case 'SZ':                                                              return this.DataTypes.TIMESTAMP_TZ_TYPE
           case 'TS':                                                              return this.DataTypes.TIMESTAMP_TYPE
           case 'TZ':                                                              return this.DataTypes.TIME_TZ_TYPE || this.DataTypes.TIME_TYPE
           case 'UT':                                                              return this.DataTypes.JSON_TYPE // USER DEFINED TYPE
		   case 'XM':                                                              return this.DataTypes.XML_TYPE
           case 'YR':                                                              return this.DataTypes.YEAR_TYPE                      || DataTyes.INTERVAL_YEAR_TO_MONTH      || this.DataTypes.INTERVAL_TYPE      // INTERVAL YEAR
           case 'YM':                                                              return this.DataTypes.INTERVAL_YEAR_TO_MONTH         || this.DataTypes.INTERVAL_TYPE                                              // INTERVAL YEAR TO MONTH
           default:                                                                return this.DataType.toUpperCase()
             this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
		     return this.DataType.toUpperCase();
         }
         break
       default:
         this.yadamuLogger.qaWarning([this.dbi.DATABASE_VENDOR,vendor,dataType],'Explicit mapping not found')
	     return this.DataType.toUpperCase();
    }
  }
  
  getDataTypeMapping(vendor,type,length,scale) {
	  
    const targetDataType = this.mapForeignDataType(vendor,type,length,scale);

	if (targetDataType === undefined) {
	  this.yadamuLogger.logInternalError(['mapForeignDataType()'],`Missing Mapping for "${type}" in mappings for "${vendor}".`)
    }
	
	return targetDataType
  }
	   
}

export { YadamuStatementGenerator as default }
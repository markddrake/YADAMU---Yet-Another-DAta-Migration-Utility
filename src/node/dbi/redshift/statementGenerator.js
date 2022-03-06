"use strict";

import crypto from 'crypto';
import path from 'path';

import YadamuLibrary from '../../lib/yadamuLibrary.js';

class StatementGenerator {

  static get LARGEST_CHAR_SIZE()       { return 65000 }
  static get LARGEST_BINARY_SIZE()     { return 65000 }
  static get LARGEST_VARCHAR_SIZE()    { return 65535 }
  static get LARGEST_VARBINARY_SIZE()  { return 65000 }
  static get LARGEST_LOB_SIZE()        { return 32000000 }
  static get LARGEST_ROW_SIZE()        { return 32768000  }
  static get DEFAULT_SPATIAL_SIZE()    { return 1050000 }

  static get LARGEST_VARCHAR_TYPE()    { return `varchar(${StatementGenerator.LARGEST_VARCHAR_SIZE})` }

  static get LARGEST_CHAR_TYPE()       { return StatementGenerator.LARGEST_VARCHAR_TYPE }
  static get LARGEST_BINARY_TYPE()     { return StatementGenerator.LARGEST_VARCHAR_TYPE }
  static get LARGEST_VARBINARY_TYPE()  { return StatementGenerator.LARGEST_VARCHAR_TYPE }

  static get LARGEST_NUMERIC_TYPE()    { return 'decimal(38)' }
  
  /*
  static get CLOB_TYPE()               { return 'super' }
  static get BLOB_TYPE()               { return 'super' }
  static get MAX_CLOB_TYPE()           { return 'super' }
  static get MAX_BLOB_TYPE()           { return 'super' }
  */
  
  static get CLOB_TYPE()               { return StatementGenerator.LARGEST_VARCHAR_TYPE }
  static get BLOB_TYPE()               { return StatementGenerator.LARGEST_VARCHAR_TYPE }
  static get MAX_CLOB_TYPE()           { return StatementGenerator.LARGEST_VARCHAR_TYPE }
  static get MAX_BLOB_TYPE()           { return StatementGenerator.LARGEST_VARCHAR_TYPE }

  static get UNBOUNDED_NUMBER_TYPE()   { return 'decimal(38,19)' }
  static get ROWID_TYPE()              { return 'varchar(32)';      }
  static get XML_TYPE()                { return StatementGenerator.CLOB_TYPE }
  // static get XML_TYPE()                { return 'super' }
  static get JSON_TYPE()               { return 'super' }
  static get ENUM_TYPE()               { return 'varchar(255)';     }
  static get UUID_TYPE()               { return 'varchar(36)'          }
  static get INTERVAL_TYPE()           { return 'varchar(16)';      }
  static get BFILE_TYPE()              { return 'varchar(2048)';    }
  static get HIERARCHY_TYPE()          { return 'varchar(4000)';    }
  static get MSSQL_MONEY_TYPE()        { return 'decimal(19,4)';    }
  static get MSSQL_SMALL_MONEY_TYPE()  { return 'decimal(10,4)';    }
  static get MSSQL_ROWVERSION_TYPE()   { return `varchar(${8*2})`;        }
  static get PGSQL_MONEY_TYPE()        { return 'decimal(21,2)';    }
  static get PGSQL_NAME_TYPE()         { return 'varchar(64)';      }
  static get PGSQL_SINGLE_CHAR_TYPE()  { return 'char(1)';          }
  static get PGSQL_NUMERIC_TYPE()      { return StatementGenerator.UNBOUNDED_NUMBER_TYPE }; 
  static get ORACLE_NUMERIC_TYPE()     { return StatementGenerator.UNBOUNDED_NUMBER_TYPE }; 
  static get MONGO_DECIMAL_TYPE()      { return StatementGenerator.UNBOUNDED_NUMBER_TYPE }; 
  static get INET_ADDR_TYPE()          { return 'varchar(39)';      }
  static get MAC_ADDR_TYPE()           { return 'varchar(23)';      }
  static get PGSQL_IDENTIFIER()        { return 'bigint';        }
  static get MYSQL_YEAR_TYPE()         { return 'smallint';         }
  static get MONGO_OBJECT_ID()         { return `varchar(${12*2})`}
  static get MONGO_UNKNOWN_TYPE()      { return 'varchar(2048)';    }
  static get MONGO_REGEX_TYPE()        { return 'varchar(2048)';    }
  static get C_UNTYPED_INTERVAL_TYPE() { return 'varchar(16)';    }

  static get UNBOUNDED_TYPES() { 
  StatementGenerator._UNBOUNDED_TYPES = StatementGenerator._UNBOUNDED_TYPES || Object.freeze(['smallint','int','bigint','float','timestamp','timestamp with time zone','timestamp without time zone','super'])
    return StatementGenerator._UNBOUNDED_TYPES;
  }

  constructor(dbi, targetSchema, metadata, yadamuLogger) {
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;
  }
    
  mapForeignDataType(vendor, dataType, dataTypeLength, dataTypeScale) {
    switch (vendor) {
	   case "Vertica": 
         switch (dataType.toUpperCase()) {
           case 'JSON':                                                                  return StatementGenerator.JSON_TYPE;
           case 'XML':                                                                   return StatementGenerator.XML_TYPE;
           default:                                                                  	 return dataType.toLowerCase();
		 }
		 break;
       case 'Oracle':
         switch (dataType.toUpperCase()) {
           case 'VARCHAR2':                                                              return 'varchar';
           case 'NVARCHAR2':                                                             return 'varchar';
           case 'NUMBER':                                                                return dataTypeLength === undefined ? StatementGenerator.ORACLE_NUMERIC_TYPE : 'decimal';
           case 'BINARY_FLOAT':                                                          return 'float';
           case 'BINARY_DOUBLE':                                                         return 'float';
           case 'CLOB':                                                                  return StatementGenerator.CLOB_TYPE;
           case 'BLOB':                                                                  return StatementGenerator.BLOB_TYPE;
           case 'NCLOB':                                                                 return StatementGenerator.CLOB_TYPE;
           case 'XMLTYPE':                                                               return StatementGenerator.XML_TYPE;
           case 'TIMESTAMP':                                                             return 'timestamp without time zone';
           case 'BFILE':                                                                 return StatementGenerator.BFILE_TYPE;
           case 'ROWID':                                                                 return StatementGenerator.ROWID_TYPE;
           case 'RAW':                                                                   return `varchar(${2 * dataTypeLength})`;
           case 'ANYDATA':                                                               return StatementGenerator.CLOB_TYPE;
           case 'JSON':                                                                  return StatementGenerator.JSON_TYPE;
           case '"MDSYS"."SDO_GEOMETRY"':                                                return 'geometry';
           case 'BOOLEAN':                                                               return 'boolean'
           default :
		     switch (true) {
               case (dataType.indexOf('INTERVAL') > -1):                                 return StatementGenerator.INTERVAL_TYPE;
			   case (dataType.indexOf('TIME ZONE') > -1):                                return 'timestamp without time zone'; 
               case (dataType.indexOf('XMLTYPE') > -1):                                  return StatementGenerator.XML_TYPE;
               case (dataType.indexOf('.') > -1):                                        return StatementGenerator.CLOB_TYPE;
               default:                                                                  return dataType.toLowerCase();
			 }
         }
         break;
       case 'MSSQLSERVER':
         switch (dataType.toLowerCase()) {
           case 'varchar':
             switch (true) {
               case (dataTypeLength === -1):                                             return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):          return StatementGenerator.CLOB_TYPE;
               default:                                                                  return 'varchar';
             }                                                                          
           case 'char':                                                                 
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_CHAR_SIZE):             return  tatementGenerator.CLOB_TYPE;
               default:                                                                  return 'char';
             }                                                                          
           case 'nvarchar':                                                             
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):          return StatementGenerator.CLOB_TYPE;
               default:                                                                  return 'varchar';
             }                                                                          
           case 'nchar':                                                                
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_CHAR_SIZE):             return StatementGenerator.CLOB_TYPE;
               default:                                                                  return 'char';
             }
			 
           case 'text':                                                                  return StatementGenerator.CLOB_TYPE;                   
           case 'ntext':                                                                 return StatementGenerator.CLOB_TYPE;
           case 'binary':
             switch (true) {
               case (dataTypeLength === -1):                                             return StatementGenerator.BLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_BINARY_SIZE_SIZE):      return StatementGenerator.BLOB_TYPE;
               default:                                                                  return `varchar(${2 * dataTypeLength})`;
             }
           case 'varbinary':
             switch (true) {
               case (dataTypeLength === -1):                                             return StatementGenerator.BLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARBINARY_SIZE_SIZE):   return StatementGenerator.BLOB_TYPE;
               default:                                                                  return `varchar(${2 * dataTypeLength})`;
             }
           case 'image':                                                                 return StatementGenerator.BLOB_TYPE;
           case 'boolean':                                                               return 'boolean'
           case 'tinyint':                                                               return 'smallint';
           case 'mediumint':                                                             return 'int';
           case 'money':                                                                 return StatementGenerator.MSSQL_MONEY_TYPE
           case 'smallmoney':                                                            return StatementGenerator.MSSQL_SMALL_MONEY_TYPE;
           case 'real':                                                                  return 'float';
           case 'bit':                                                                   return 'boolean'
           case 'datetime':                                                              return 'timestamp without time zone';
           case 'time':                                                                  return 'timestamp without time zone';
           case 'datetime2':                                                             return 'timestamp without time zone';
           case 'datetimeoffset':                                                        return 'timestamp without time zone';
           case 'smalldate':                                                             return 'timestamp without time zone';
           case 'geography':                                                             return 'geometry';
           case 'geometry':                                                              return 'geometry';
           case 'hierarchyid':                                                           return StatementGenerator.HIERARCHY_TYPE
           case 'rowversion':                                                            return `varchar(${2*8})`;
           case 'uniqueidentifier':                                                      return StatementGenerator.UUID_TYPE
           case 'json':                                                                  return StatementGenerator.JSON_TYPE;
           case 'xml':                                                                   return StatementGenerator.XML_TYPE;
           default:                                                                      return dataType.toLowerCase();
         }
         break;
       case 'Postgres':    
         switch (dataType.toLowerCase()) {
           case 'character varying':     
             switch (true) {
               case (dataTypeLength === undefined):                                       return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):           return StatementGenerator.CLOB_TYPE;
               default:                                                                   return 'varchar';
             }
           case 'character':
             switch (true) {
               case (dataTypeLength === undefined):                                       return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_CHAR_SIZE):              return StatementGenerator.CLOB_TYPE;
               default:                                                                   return 'char';
             }
		   case 'text':                                                                   return StatementGenerator.CLOB_TYPE;
		   case 'char':                                                                   return StatementGenerator.PGSQL_SINGLE_CHAR_TYPE;
		   case 'name':                                                                   return StatementGenerator.PGSQL_NAME_TYPE
		   case 'bpchar':                     
             switch (true) {
               case (dataTypeLength === undefined):                                       return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_CHAR_SIZE):              return StatementGenerator.CLOB_TYPE;
               default:                                                                   return 'char';
             }
           case 'bytea':
             switch (true) {
               case (dataTypeLength === undefined):                                       return StatementGenerator.BLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARBINARY_SIZE_SIZE):    return StatementGenerator.BLOB_TYPE;
               default:                                                                   return `varchar(${dataTypeLength > 32767 ? 65535 : 2 * dataTypeLength})`;
             }
		   case 'decimal':
           case 'numeric':                                                               return dataTypeLength === undefined ? StatementGenerator.PGSQL_NUMERIC_TYPE : 'decimal';
		   case 'money':                                                                 return StatementGenerator.PGSQL_MONEY_TYPE
           case 'integer':                                                               return 'int';
           case 'real':                                                                  return 'float';
           case 'double precision':                                                      return 'float';
           case 'boolean':                                                               return 'boolean'
           case 'timestamp':                                                             return 'timestamp'
           case 'timestamp with time zone':                                              return 'timestamp with time zone'                                 
           case 'timestamp without time zone':                                           return 'timestamp'
           case 'time with time zone':                                                   return 'time with time zone'
           case 'time without time zone':                                                return 'time';
		   case 'json':
           case 'jsonb':                                                                 return StatementGenerator.JSON_TYPE;
           case 'xml':                                                                   return StatementGenerator.XML_TYPE;
           case 'geography':                                                             return 'geometry'; 
           case 'geometry':                                                              return 'geometry';
           case 'point':                                                                 return 'point';
           case 'lseg':                                                               
           case 'path':                                                                  return 'linestring';     
           case 'box':                                                                  
           case 'polygon':                                                               return 'polygon';     
           case 'circle':                                                                return this.dbi.INBOUND_CIRCLE_FORMAT === 'CIRCLE' ? StatementGenerator.JSON_TYPE : 'geometry';
           case 'line':                                                                  return StatementGenerator.JSON_TYPE;     
           case 'uuid':                                                                  return StatementGenerator.UUID_TYPE
		   case 'bit':
		   case 'bit varying':    
 		     switch (true) {
               case (dataTypeLength === undefined):                                      return StatementGenerator.LARGEST_VARCHAR_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):          return StatementGenerator.CLOB_TYPE;
               default:                                                                  return 'varchar'
			 }
		   case 'cidr':
		   case 'inet':                                                                  return StatementGenerator.INET_ADDR_TYPE
		   case 'macaddr':                                                              
		   case 'macaddr8':                                                              return StatementGenerator.MAC_ADDR_TYPE
		   case 'int4range':                                                            
		   case 'int8range':                                                            
		   case 'numrange':                                                             
		   case 'tsrange':                                                              
		   case 'tstzrange':                                                            
		   case 'daterange':                                                             return StatementGenerator.JSON_TYPE;
		   case 'tsvector':                                                             
		   case 'gtsvector':                                                             return StatementGenerator.JSON_TYPE;
		   case 'tsquery':                                                               return StatementGenerator.LARGEST_VARCHAR_TYPE;
           case 'oid':                                                                  
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
		   case 'regtype':                                                              return StatementGenerator.PGSQL_IDENTIFIER
		   case 'tid':                                                                  
		   case 'xid':                                                                  
		   case 'cid':                                                                  
		   case 'txid_snapshot':                                                        return StatementGenerator.PGSQL_IDENTIFIER;
		   case 'aclitem':                                                              
		   case 'refcursor':                                                            return StatementGenerator.JSON_TYPE;
           default :
		     switch (true) {
               case (dataType.indexOf('interval') > -1):
   		         switch (true) {
                   case (dataType.indexOf('year') > -1):                                return 'interval day to second'
                   case (dataType.indexOf('day') > -1):                                 return 'interval year to month'
				   default:                                                             return StatementGenerator.C_UNTYPED_INTERVAL_TYPE
				 }                                                           
               default:                                                                 return dataType.toLowerCase();
			 }
         }
         break
       case 'MySQL':
       case 'MariaDB':
         switch (dataType.toLowerCase()) {
           case 'boolean':                                                             return 'boolean'
           case 'double':                                                              return 'float';
           case 'decimal':                                           
             switch (true) {
               case (dataTypeLength > 38 && dataTypeScale === 0):                      return StatementGenerator.LARGEST_NUMERIC_TYPE
               case (dataTypeLength > 38 && dataTypeScale !==0 ):                      return `${StatementGenerator.LARGEST_NUMERIC_TYPE.substr(0,StatementGenerator.LARGEST_NUMERIC_TYPE.length-1)},${Math.round(dataTypeScale*(38/dataTypeLength))})`;
               default:                                                                return 'decimal'                                                      
             }
           case 'tinyint':                                                             return 'smallint';
           case 'mediumint':                                                           return 'int'
           case 'longtext':                 
           case 'mediumtext':               
           case 'text':                                                                return StatementGenerator.CLOB_TYPE;
           case 'binary':
		   case 'varbinary':                                                           return `varchar(${dataTypeLength > 32767 ? 65535 : 2 * dataTypeLength})`;
           case 'datetime':                                                            return 'timestamp without time zone';
           case 'year':                                                                return StatementGenerator.MYSQL_YEAR_TYPE;
           case 'longblob':                 
           case 'mediumblob':                 
           case 'blob':                                                                return StatementGenerator.BLOB_TYPE;
           case 'json':                                                                return StatementGenerator.JSON_TYPE;
           case 'set':                                                                 return StatementGenerator.JSON_TYPE;
           case 'enum':                                                                return StatementGenerator.ENUM_TYPE
		   case 'point':                                                               return 'point';
		   case 'linestring':                                                          return 'linestring';
		   case 'polygon':                                                             return 'polygon';
		   case 'multipoint':                                                          return 'multipoint'; 
		   case 'multilinestring':                                                     return 'multilinestring';
		   case 'multipolygon':                                                        return 'multipolygon';
		   case 'geometrycollection':                                                  return 'geometrycollection';
		   case 'geomcollection':                                                      return 'geometrycollection';
		   case 'geometry':                                                            return 'geometry';
           case 'geography':                                                           return 'geometry';
           default:                                                                    return dataType.toLowerCase();
         }
         break;
       case 'MongoDB':
         switch (dataType) {
           case "string":
		     switch (true) {
               case (dataTypeLength === undefined):                                    return StatementGenerator.CLOB_TYPE;
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):        return StatementGenerator.CLOB_TYPE;
               default:                                                                return 'varchar';
             }
		   case "int":                                                                 return 'int';
		   case "long":                                                                return 'bigint';
		   case "double":                 	                                           return 'float';
		   case "decimal":              		                                       return StatementGenerator.MONGO_DECIMAL_TYPE;
		   case "binData":             		                                           return StatementGenerator.BLOB_TYPE;
		   case "bool":                                                                return 'boolean';
		   case "date":                                                                return 'timestamp without time zone';
		   case "timestamp":		                                                   return 'timestamp without time zone';
		   case "objectId":            		                                           return StatementGenerator.MONGO_OBJECT_ID
		   case "json":                                                            
		   case "object":                                                            
		   case "array":                                                               return StatementGenerator.JSON_TYPE;
           case "null":                                                                return StatementGenerator.MONGO_UNKNOWN_TYPE
           case "regex":                		                                       return StatementGenerator.MONGO_REGEX_TYPE
           case "javascript":		                                                   return StatementGenerator.CLOB_TYPE;
		   case "javascriptWithScope":    	                                           return StatementGenerator.CLOB_TYPE;
		   case "minkey":                                                            
		   case "maxkey":                                                              return StatementGenerator.JSON_TYPE;
		   case "undefined":                                                         
		   case 'dbPointer':                                                         
		   case 'function':                                                          
		   case 'symbol':                                                              return StatementGenerator.JSON_TYPE;
           default:                                                                    return dataType.toLowerCase();
		 }
		 break;
       case 'SNOWFLAKE':
         switch (dataType.toLowerCase()) {
		   case "number":		                                                       return 'decimal';
		   case "float":		                                                       return 'float';
		   case "geography":   	                                                       return 'geometry';
		   case "text":                                                                return dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE ? StatementGenerator.CLOB_TYPE: 'varchar'; 
		   case "binary":                                                              return dataTypeLength > StatementGenerator.LARGEST_VARBINARY_SIZE ? StatementGenerator.BLOB_TYPE : `varchar(dataTypeLength > 32767 ? 65535 : 2 * dataTypeLength)`;
           case 'json':                                                                return StatementGenerator.JSON_TYPE;
		   case "xml":       	                                                       return StatementGenerator.XML_TYPE
		   case "variant":                                                             return StatementGenerator.BLOB_TYPE;
		   case "timestamp_ltz":                                                    
		   case "timestamp_ntz":                                                       return 'timestamp without time zone'; 
		   default:
             return dataType.toLowerCase();
	     }
	   default :
         return dataType.toLowerCase();
    }  
  }

  getColumnDataType(targetDataType, length, scale) {
	
    if (RegExp(/\(.*\)/).test(targetDataType)) {
      return targetDataType
    }
     
    if (StatementGenerator.UNBOUNDED_TYPES.includes(targetDataType)) {
      return targetDataType
    }
     	  
    if (scale) {
      return targetDataType + '(' + length + ',' + scale + ')';
    }                                                   
  
    if (length && (length > 0)) {	 
      return targetDataType + '(' + length + ')'
    }
	
    return targetDataType;     
  }
      
  generateTableInfo(tableMetadata) {
      
    let insertMode = 'Batch';
	
    const columnNames = tableMetadata.columnNames
    const dataTypes = tableMetadata.dataTypes
    const sizeConstraints = tableMetadata.sizeConstraints

    const targetDataTypes = [];
    const copyColumnList = [];
	const insertOperators =[]

    const columnClauses = columnNames.map((columnName,idx) => {
	  const dataType = YadamuLibrary.composeDataType(dataTypes[idx],sizeConstraints[idx])       
	  let targetDataType = this.mapForeignDataType(tableMetadata.vendor,dataType.type,dataType.length,dataType.scale)
	  targetDataTypes.push(targetDataType)
	  
      let targetLength = dataType.length
	  switch (targetDataType) {
		case 'varchar':
          targetLength = tableMetadata.vendor === 'Redshift' ? targetLength : Math.ceil(targetLength * this.dbi.BYTE_TO_CHAR_RATIO);
		  if (targetLength > StatementGenerator.LARGEST_VARCHAR_SIZE) {
			targetLength = StatementGenerator.LARGEST_VARCHAR_SIZE
		  }
		  sizeConstraints[idx] = targetLength
      }		
      
	  return `"${columnName}" ${this.getColumnDataType(targetDataType,targetLength,dataType.scale)}`
    })
	
    const createStatement = `create table if not exists "${this.targetSchema}"."${tableMetadata.tableName}"(\n  ${columnClauses.join(',')})`;
    const insertStatement = `insert into "${this.targetSchema}"."${tableMetadata.tableName}" ("${columnNames.join('","')}") values `;

    const maxBatchSize        = Math.trunc(32768 / tableMetadata.columnNames.length);

    const tableInfo = { 
       ddl             : createStatement, 
       dml             : insertStatement, 
	   mergeout        : mergeoutStatement,
	   columnNames     : columnNames,
       targetDataTypes : targetDataTypes, 
	   sizeConstraints : sizeConstraints,
	   insertOperators : insertOperators,
       insertMode      : insertMode,
       _BATCH_SIZE     : maxBatchSize,
       _SPATIAL_FORMAT : this.dbi.INBOUND_SPATIAL_FORMAT,
	   _SCHEMA_NAME    : this.targetSchema,
	   _TABLE_NAME     : tableMetadata.tableName
    }
	
	
	if (tableMetadata.dataFile) {
	  this.dbi.IAM_ROLE = 'arn:aws:iam::437125103918:role/RedshiftFastLoad'
	  tableInfo.copy  = `copy "${this.targetSchema}"."${tableMetadata.tableName}" from 's3://${this.dbi.BUCKET}/${tableMetadata.dataFile}' iam_role '${this.dbi.IAM_ROLE}' EMPTYASNULL DATEFORMAT 'auto' TIMEFORMAT 'auto' MAXERROR ${this.dbi.TABLE_MAX_ERRORS} FORMAT AS CSV`		
	}
	
	return tableInfo
	
  }
  
  async generateStatementCache() {
      
  const statementCache = {}
    const tables = Object.keys(this.metadata); 
	
    const ddlStatements = tables.map((table,idx) => {
      const tableMetadata = this.metadata[table];
      const tableInfo = this.generateTableInfo(tableMetadata);
      statementCache[this.metadata[table].tableName] = tableInfo;
      return tableInfo.ddl;
    })
    return statementCache;
  }

}

export { StatementGenerator as default }
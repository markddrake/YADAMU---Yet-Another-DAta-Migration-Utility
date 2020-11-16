"use strict";

const YadamuLibrary = require('../../../common/yadamuLibrary.js');

// Code Shared by MySQL 5.7 and MariaDB. 

class StatementGenerator {

  static get UNBOUNDED_TYPES() { 
    StatementGenerator._UNBOUNDED_TYPES = StatementGenerator._UNBOUNDED_TYPES || Object.freeze(['date','time','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json','set','enum'])
    return StatementGenerator._UNBOUNDED_TYPES;
  }

  static get SPATIAL_TYPES() { 
    StatementGenerator._SPATIAL_TYPES = StatementGenerator._SPATIAL_TYPES || Object.freeze(['geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection'])
    return StatementGenerator._SPATIAL_TYPES;
  }

  static get NATIONAL_TYPES() { 
    StatementGenerator._NATIONAL_TYPES = StatementGenerator._NATIONAL_TYPES || Object.freeze(['nchar','nvarchar'])
    return StatementGenerator._NATIONAL_TYPES;
  }
  
  static get INTEGER_TYPES() { 
    StatementGenerator._INTEGER_TYPES = StatementGenerator._INTEGER_TYPES || Object.freeze(['tinyint','mediumint','smallint','int','bigint'])
    return StatementGenerator._INTEGER_TYPES;
  }
  
  constructor(dbi, targetSchema, metadata, spatialFormat) {
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.spatialFormat = spatialFormat
  }
    
  mapForeignDataType(vendor, dataType, dataTypeLength, dataTypeSize) {
    switch (vendor) {
       case 'Oracle':
         switch (dataType) {
           case 'VARCHAR2':                return 'varchar';
           case 'NVARCHAR2':               return 'varchar';
           case 'NUMBER':                  return 'decimal';
           case 'BINARY_FLOAT':            return 'float';
           case 'BINARY_DOUBLE':           return 'double';
           case 'CLOB':                    return 'longtext';
           case 'BLOB':                    return 'longblob';
           case 'NCLOB':                   return 'longtext';
           case 'XMLTYPE':                 return 'longtext';
           case 'TIMESTAMP':
             switch (true) {
               case (dataTypeLength > 6):  return 'datetime(6)';
               default:                    return 'datetime';
             }
           case 'BFILE':                   return 'varchar(2048)';
           case 'ROWID':                   return 'varchar(32)';
           case 'RAW':                     return 'varbinary';
           case 'ROWID':                   return 'varchar(32)';
           case 'ANYDATA':                 return 'longtext';
           case '"MDSYS"."SDO_GEOMETRY"':  return 'geometry';
           case 'BOOLEAN':                 return this.dbi.TREAT_TINYINT1_AS_BOOLEAN ? 'boolean' : 'tinyint(1)'
           default :
             if (dataType.indexOf('TIME ZONE') > -1) {
               return 'datetime'; 
             }
             if (dataType.indexOf('INTERVAL') === 0) {
               return 'varchar(16)'; 
             }
             if (dataType.indexOf('XMLTYPE') > -1) { 
               return 'longtext';
             }
             if (dataType.indexOf('.') > -1) { 
               return 'longtext';
             }
             return dataType.toLowerCase();
         }
         break;
       case 'MSSQLSERVER':
         switch (dataType) {
           case 'binary':
             switch (true) {
               case (dataTypeLength > 16777215):   return 'longblob';
               case (dataTypeLength > 65535):      return 'mediumblob';
               default:                            return 'binary';
             }
           case 'boolean':                         return 'tinyint(1)';
           case 'bit':                             return 'tinyint(1)';
           case 'char':
             switch (true) {
               case (dataTypeLength === -1):       return 'longtext';
               case (dataTypeLength > 16777215):   return 'longtext';
               case (dataTypeLength > 65535):      return 'mediumtext';
                case (dataTypeLength > 255):       return 'text';
                default:                           return 'char';
             }
           case 'datetime':                        return 'datetime(3)';
           case 'datetime2':
             switch (true) {
                case (dataTypeLength > 6):         return 'datetime(6)';
                default:                           return 'datetime';
             }
           case 'datetimeoffset':                  return 'datetime';
           case 'geography':                       return 'geometry';
           case 'geometry':                        return 'geometry';
           case 'hierarchyid':                     return 'varchar(4000)';
           case 'image':                           return 'longblob';
           case 'mediumint':                       return 'int';
           case 'money':                           return 'decimal(19,4)';
           case 'nchar':
             switch (true) {
                case (dataTypeLength === -1):      return 'longtext';
                case (dataTypeLength > 16777215):  return 'longtext';
                case (dataTypeLength > 65535):     return 'mediumtext';
                case (dataTypeLength > 255):       return 'text';
                default:                           return 'char';
             }
           case 'ntext':                           return 'longtext';
           case 'nvarchar':
             switch (true) {
               case (dataTypeLength === -1):       return 'longtext';
               case (dataTypeLength > 16777215):   return 'longtext';
               case (dataTypeLength > 65535):      return 'mediumtext';
               default:                            return 'varchar';
             }             
           case 'real':                            return 'float';
           case 'rowversion':                      return 'binary(8)';
           case 'smalldate':                       return 'datetime';
           case 'smallmoney':                      return 'decimal(10,4)';
           case 'text':                            return 'longtext';
           case 'tinyint':                         return 'smallint';
           case 'uniqueidentifier':                return 'varchar(64)';
           case 'varbinary':
             switch (true) {
               case (dataTypeLength === -1):       return 'longblob';
               case (dataTypeLength > 16777215):   return 'longblob';
               case (dataTypeLength > 65535):      return 'mediumblob';
               default:                            return 'varbinary';
             }
           case 'varchar':
             switch (true) {
               case (dataTypeLength === -1):       return 'longtext';
               case (dataTypeLength > 16777215):   return 'longtext';
               case (dataTypeLength > 65535):      return 'mediumtext';
               default:                            return 'varchar';
             }
           case 'xml':                             return 'longtext';
           default:                                return dataType.toLowerCase();
         }
         break;
       case 'Postgres':                            
         switch (dataType) {
           case 'character varying':       
             switch (true) {
               case (dataTypeLength === undefined): return 'longtext';
               case (dataTypeLength > 16777215):    return 'longtext';
               case (dataTypeLength > 65535):       return 'mediumtext';
               default:                             return 'varchar';
             }
           case 'character':                        return 'nchar';
           case 'bytea':
             switch (true) {
               case (dataTypeLength === undefined): return 'varbinary(4096)';
               case (dataTypeLength > 16777215):    return 'longblob';
               case (dataTypeLength > 65535):       return 'mediumblob';
               default:                             return 'varbinary';
             }
           case 'timestamp': 
           case 'timestamp with time zone': 
           case 'timestamp without time zone': 
           case 'time without time zone': 
             switch (true) {
               case (dataTypeLength === undefined): return 'datetime(6)';
               default:                             return 'datetime';
             }
           case 'numeric':                          return 'decimal';
           case 'boolean':                          return 'tinyint(1)';
           case 'double precision':                 return 'double';
           case 'real':                             return 'float';
           case 'integer':                          return 'int';
           case 'xml':                              return 'longtext';     
           case 'jsonb':                            return 'json';     
           case 'text':                             return 'longtext';     
           case 'geography':       
           case 'geography':                        return 'geometry';     
           default:
             if (dataType.indexOf('interval') === 0) {
               return 'varchar(16)'; 
             }
             return dataType.toLowerCase();
         }
         break
       case 'MySQL':
       case 'MariaDB':
         switch (dataType) {
           case 'boolean':                        return this.dbi.TREAT_TINYINT1_AS_BOOLEAN ? 'boolean' : 'tinyint(1)';
           case 'set':                            return 'json';
           case 'enum':                           return 'varchar(512)';
           default:                               return dataType.toLowerCase();
         }
         break;
       case 'MongoDB':
         switch (dataType) {
			case "double":                 	        return 'double';
           case "string":
		     switch (true) {
               case (dataTypeLength === undefined): return 'longtext';
               case (dataTypeLength > 16777215):    return 'longtext';
               case (dataTypeLength > 65535):       return 'mediumtext';
               default:                             return 'varchar';
             }
		   case "object":
		   case "array":
		     return 'json';
		   case "binData":
		     return 'longblob';
		   case "objectId":
		     return "binary(12)";
		   case "bool":
		     return 'tinyint(1)';
           case "null":
		     return 'varchar(128)';
           case "regex":
		     return 'varchar(256)';
           case "javascript":
		     return 'longtext';
		   case "javascriptWithScope":
		     return 'longtext';
		   case "int":
		     return 'int';
		   case "long":
		     return 'bigint';
		   case "decimal":
		     return 'decimal';
		   case "timestamp":
		     return 'datetime(6)';
		   case "date":
		     return 'datatime(6)';
		   case "minkey":
		   case "maxkey":
             return "json";
           default:
             return dataType.toLowerCase();
		 }
		 break;
       case 'SNOWFLAKE':
         switch (dataType.toLowerCase()) {
		   case "number":
		     return 'decimal';
		   case "geography":
		     return 'geometry';
		   case "text":
		     switch (true) {
               case (dataTypeLength > 65535):       return 'mediumtext';
			   default:                             return 'varchar'; 
			 }
		   case "binary":
		     switch (true) {
               case (dataTypeLength > 65535):       return 'mediumblob';
			   default:                             return 'varbinary'; 
			 }
		   case "xml":
		     return 'longtext';
		   case "variant":
		     return 'longblob';
		   case "timestamp_ltz":
		   case "timestamp_ntz":
		     switch (true) {
               case (dataTypeLength > 6):           return 'datetime(6)';
			   default:                             return 'datetime'; 
			 }
		   default:
             return dataType.toLowerCase();
	     }
	   default :
         return dataType.toLowerCase();0	
    }  
  }
  
  getColumnDataType(targetDataType, length, scale) {
  
     if (RegExp(/\(.*\)/).test(targetDataType)) {
       return targetDataType
     }
     
     if (targetDataType.endsWith(" unsigned")) {
       return targetDataType
     }
   
     if (targetDataType === "boolean") {
       return 'tinyint(1)'
     }
  
     if (StatementGenerator.UNBOUNDED_TYPES.includes(targetDataType)) {
       return targetDataType
     }
  
     if (StatementGenerator.SPATIAL_TYPES.includes(targetDataType)) {
       return targetDataType
     }
  
     if (StatementGenerator.NATIONAL_TYPES.includes(targetDataType)) {
       return targetDataType + '(' + length + ')'
     }
  
     if (scale) {
       if (StatementGenerator.INTEGER_TYPES.includes(targetDataType)) {
         return targetDataType + '(' + length + ')';
       }
       return targetDataType + '(' + length + ',' + scale + ')';
     }                                                   
  
     if (length) {
       if (targetDataType === 'double')  {
         return targetDataType
       }
       if (length)
       return targetDataType + '(' + length + ')';
     }
  
     return targetDataType;     
  }
      
  generateTableInfo(tableMetadata) {
      
    let insertMode = 'Batch';

    const columnNames = tableMetadata.columnNames
    const dataTypes = tableMetadata.dataTypes
    const sizeConstraints = tableMetadata.sizeConstraints
    const targetDataTypes = [];
    const setOperators = []
    
    const columnClauses = columnNames.map((columnName,idx) => {    
	  const dataType = YadamuLibrary.composeDataType(dataTypes[idx],sizeConstraints[idx])       
      let targetDataType = this.mapForeignDataType(tableMetadata.vendor,dataType.type,dataType.length,dataType.scale);
      targetDataTypes.push(targetDataType);

      let ensureNullable = false;
      switch (targetDataType) {
        case 'geometry':
           switch (this.spatialFormat) {
             case "WKB":
             case "EWKB":
               setOperators.push('ST_GeomFromWKB(?)');
               break
             case "WKT":
             case "EWRT":
               setOperators.push('ST_GeomFromText(?)');
               break;
             case "GeoJSON":
               setOperators.push('ST_GeomFromGeoJSON(?)');
               break;
             default:
               setOperators.push('ST_GeomFromWKB(?)');
           }              
           break;                                                 
        case 'timestamp':
           ensureNullable = true;
        default:
           setOperators.push('?')
      }
      return `"${columnName}" ${this.getColumnDataType(targetDataType,dataType.length,dataType.scale)} ${ensureNullable === true ? 'null':''}`
    })
                                       
    const createStatement = `create table if not exists "${this.targetSchema}"."${tableMetadata.tableName}"(\n  ${columnClauses.join(',')})`;
    const insertStatement = `insert into "${this.targetSchema}"."${tableMetadata.tableName}" ("${columnNames.join('","')}") values `;
    const rowConstructor = `(${setOperators.join(',')})`
    
    return { 
       ddl             : createStatement, 
       dml             : insertStatement, 
	   columnNames     : columnNames,
       rowConstructor  : rowConstructor,
       targetDataTypes : targetDataTypes, 
       insertMode      : insertMode,
       _BATCH_SIZE     : this.dbi.BATCH_SIZE,
       _COMMIT_COUNT   : this.dbi.COMMIT_COUNT,
       _SPATIAL_FORMAT : this.spatialFormat
    }
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

module.exports = StatementGenerator
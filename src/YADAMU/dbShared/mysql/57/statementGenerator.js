"use strict";

import path from 'path';

import YadamuLibrary from '../../../common/yadamuLibrary.js';

// Code Shared by MySQL 5.7 and MariaDB. 

class StatementGenerator {

  static get LARGEST_CHAR_SIZE()       { return 255 }
  static get LARGEST_BINARY_SIZE()     { return 255 }
  static get LARGEST_VARCHAR_SIZE()    { return 4096 }
  static get LARGEST_VARBINARY_SIZE()  { return 8192 }
  static get LARGEST_CHAR_TYPE()       { return `varchar(${StatementGenerator.LARGEST_CHAR_SIZE})`}
  static get LARGEST_BINARY_TYPE()     { return `varchar(${StatementGenerator.LARGEST_BINARY_SIZE})`}
  static get LARGEST_VARCHAR_TYPE()    { return `varchar(${StatementGenerator.LARGEST_VARCHAR_SIZE})`}
  static get LARGEST_VARBINARY_TYPE()  { return `varbinary(${StatementGenerator.LARGEST_VARBINARY_SIZE})`}

  static get TINYTEXT_SIZE()           { return 255};
  static get TEXT_SIZE()               { return 65535};
  static get MEDIUMTEXT_SIZE()         { return 16777215};
  static get LONGTEXT_SIZE()           { return 4294967295};
  
  static get TINYBLOB_SIZE()           { return 255};
  static get BLOB_SIZE()               { return 65535};
  static get MEDIUMBLOB_SIZE()         { return 16777215};
  static get LONGBLOB_SIZE()           { return 4294967295};

  /*
  **
  ** MySQL BIT Column truncates leading '0's 
  **
  
  static get LARGEST_BIT_TYPE()        { return 'varchar(64)';      }
  static get BIT_TYPE()                { return 'varchar';          }

  **
  */
   
  static get LARGEST_BIT_TYPE()        { return 'bit(64)';          }
  static get BIT_TYPE()                { return 'bit';              }

  static get BFILE_TYPE()              { return 'varchar(2048)';    }
  static get ROWID_TYPE()              { return 'varchar(32)';      }
  static get XML_TYPE()                { return 'longtext';         }
  static get UUID_TYPE()               { return 'varchar(36)';      }
  static get ENUM_TYPE()               { return 'varchar(255)';     }
  static get INTERVAL_TYPE()           { return 'varchar(16)';      }
  static get BOOLEAN_TYPE()            { return 'tinyint(1)';       }
  static get HIERARCHY_TYPE()          { return 'varchar(4000)';    }
  static get MSSQL_MONEY_TYPE()        { return 'decimal(19,4)';    }
  static get MSSQL_SMALL_MONEY_TYPE()  { return 'decimal(10,4)';    }
  static get MSSQL_ROWVERSION_TYPE()   { return 'binary(8)';        }
  static get PGSQL_MONEY_TYPE()        { return 'decimal(21,2)';    }
  static get PGSQL_NAME_TYPE()         { return 'varchar(64)';      }
  static get PGSQL_SINGLE_CHAR_TYPE()  { return 'char(1)';          }
  static get PGSQL_NUMERIC_TYPE()      { return 'decimal(65,30)'    }; 
  static get ORACLE_NUMERIC_TYPE()     { return 'decimal(65,30)'    }; 
  static get INET_ADDR_TYPE()          { return 'varchar(39)';      }
  static get MAC_ADDR_TYPE()           { return 'varchar(23)';      }
  static get PGSQL_IDENTIFIER()        { return 'binary(4)';        }
  static get MONGO_OBJECT_ID()         { return 'binary(12)';       }
  static get MONGO_UNKNOWN_TYPE()      { return 'varchar(2048)';    }
  static get MONGO_REGEX_TYPE()        { return 'varchar(2048)';    }
                                       r
  static get UNBOUNDED_TYPES() { 
    StatementGenerator._UNBOUNDED_TYPES = StatementGenerator._UNBOUNDED_TYPES || Object.freeze(['date','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json','set','enum'])
    return StatementGenerator._UNBOUNDED_TYPES;
  }

  static get SPATIAL_TYPES() { 
    StatementGenerator._SPATIAL_TYPES = StatementGenerator._SPATIAL_TYPES || Object.freeze(['geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geomcollection'])
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
  
  constructor(dbi, targetSchema, metadata, yadamuLogger) {
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;
  }
    
  mapForeignDataType(vendor, dataType, dataTypeLength, dataTypeScale) {
    switch (vendor) {
       case 'Oracle':
         switch (dataType) {
           case 'VARCHAR2':                                                              return 'varchar';
           case 'NVARCHAR2':                                                             return 'varchar';
           case 'NUMBER':                                                                return dataTypeLength === undefined ? StatementGenerator.ORACLE_NUMERIC_TYPE : 'decimal';
           case 'BINARY_FLOAT':                                                          return 'float';
           case 'BINARY_DOUBLE':                                                         return 'double';
           case 'CLOB':                                                                  return 'longtext';
           case 'BLOB':                                                                  return 'longblob';
           case 'NCLOB':                                                                 return 'longtext';
           case 'XMLTYPE':                                                               return 'longtext';
           case 'TIMESTAMP':                                                             return dataTypeLength > 6 ? 'datetime(6)' : 'datetime';
           case 'BFILE':                                                                 return StatementGenerator.BFILE_TYPE;
           case 'ROWID':                                                                 return StatementGenerator.ROWID_TYPE;
           case 'RAW':                                                                   return 'varbinary';
           case 'ANYDATA':                                                               return 'longtext';
           case '"MDSYS"."SDO_GEOMETRY"':                                                return 'geometry';
           case 'BOOLEAN':                                                               return this.dbi.TREAT_TINYINT1_AS_BOOLEAN ? 'boolean' : 'tinyint(1)'
           default :
             switch (true) {
               case (dataType.indexOf('TIME ZONE') > -1):                                return 'datetime'; 
               case (dataType.indexOf('INTERVAL') === 0):                                return StatementGenerator.INTERVAL_TYPE;
               case (dataType.indexOf('XMLTYPE') > -1):                                  return XML_TYPE;
               case (dataType.indexOf('.') > -1):                                        return 'longtext';
               default:                                                                  return dataType.toLowerCase();
             }
         }
         break;
       case 'MSSQLSERVER':
         switch (dataType) {
           case 'varchar':
             switch (true) {
               case (dataTypeLength === -1):                                             return 'longtext';
               case (dataTypeLength > StatementGenerator.MEDIUMTEXT_SIZE):               return 'longtext';
               case (dataTypeLength > StatementGenerator.TEXT_SIZE):                     return 'mediumtext';
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):          return 'text';
               default:                                                                  return 'varchar';
             }                                                                          
           case 'char':                                                                 
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return 'longtext';
               case (dataTypeLength > StatementGenerator.MEDIUMTEXT_SIZE):               return 'longtext';
               case (dataTypeLength > StatementGenerator.TEXT_SIZE):                     return 'mediumtext';
               case (dataTypeLength > StatementGenerator.LARGEST_CHAR_SIZE):             return 'text';
               default:                                                                  return 'char';
             }                                                                          
           case 'nvarchar':                                                             
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return 'longtext';
               case (dataTypeLength > StatementGenerator.MEDIUMTEXT_SIZE):               return 'longtext';
               case (dataTypeLength > StatementGenerator.TEXT_SIZE):                     return 'mediumtext';
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):          return 'text';
               default:                                                                  return 'varchar';
             }                                                                          
           case 'nchar':                                                                
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return 'longtext';
               case (dataTypeLength > StatementGenerator.MEDIUMTEXT_SIZE):               return 'longtext';
               case (dataTypeLength > StatementGenerator.TEXT_SIZE):                     return 'mediumtext';
               case (dataTypeLength > StatementGenerator.LARGEST_CHAR_SIZE):             return 'text';
               default:                                                                  return 'char';
             }
             
           case 'text':                            return 'longtext';                   
           case 'ntext':                                                                 return 'longtext';
           case 'binary':
             switch (true) {
               case (dataTypeLength === -1):                                             return 'longblob';
               case (dataTypeLength > StatementGenerator.MEDIUMBLOB_SIZE):               return 'longblob';
               case (dataTypeLength > StatementGenerator.BLOB_SIZE):                     return 'mediumblob';
               case (dataTypeLength > StatementGenerator.LARGEST_BINARY_SIZE_SIZE):      return 'blob';
               default:                                                                  return 'binary';
             }
           case 'varbinary':
             switch (true) {
               case (dataTypeLength === -1):                                             return 'longblob';
               case (dataTypeLength > StatementGenerator.MEDIUMBLOB_SIZE):               return 'longblob';
               case (dataTypeLength > StatementGenerator.BLOB_SIZE):                     return 'mediumblob';
               case (dataTypeLength > StatementGenerator.LARGEST_VARBINARY_SIZE_SIZE):   return 'blob';
               default:                                                                  return 'varbinary';
             }
           case 'image':                                                                 return 'longblob';
           case 'boolean':                                                               return StatementGenerator.BOOLEAN_TYPE
           case 'tinyint':                                                               return 'smallint';
           case 'mediumint':                                                             return 'int';
           case 'money':                                                                 return StatementGenerator.MSSQL_MONEY_TYPE
           case 'smallmoney':                                                            return StatementGenerator.MSSQL_SMALL_MONEY_TYPE;
           case 'real':                                                                  return 'float';
           case 'bit':                                                                   return StatementGenerator.BOOLEAN_TYPE;
           case 'datetime':                                                              return 'datetime(3)';
           case 'time':                                                                  return dataTypeLength > 6 ? 'time(6)' : 'time';
           case 'datetime2':                                                             return dataTypeLength > 6 ? 'datetime(6)' : 'datetime';
           case 'datetimeoffset':                                                        return dataTypeLength > 6 ? 'datetime(6)' : 'datetime';
           case 'smalldate':                                                             return 'datetime';
           case 'geography':                                                             return 'geometry';
           case 'geometry':                                                              return 'geometry';
           case 'hierarchyid':                                                           return StatementGenerator.HIERARCHY_TYPE
           case 'rowversion':                                                            return 'binary(8)';
           case 'uniqueidentifier':                                                      return StatementGenerator.UUID_TYPE
           case 'xml':                                                                   return 'longtext';
           default:                                                                      return dataType.toLowerCase();
         }
         break;
       case 'Postgres':    
         switch (dataType) {
           case 'character varying':     
             switch (true) {
               case (dataTypeLength === undefined):                                       return 'longtext';
               case (dataTypeLength > StatementGenerator.MEDIUMTEXT_SIZE):                return 'longtext';
               case (dataTypeLength > StatementGenerator.TEXT_SIZE):                      return 'mediumtext';
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):           return 'text';
               default:                                                                   return 'varchar';
             }
           case 'character':
             switch (true) {
               case (dataTypeLength === undefined):                                       return 'longtext';
               case (dataTypeLength > StatementGenerator.MEDIUMTEXT_SIZE):                return 'longtext';
               case (dataTypeLength > StatementGenerator.TEXT_SIZE):                      return 'mediumtext';
               case (dataTypeLength > StatementGenerator.LARGEST_CHAR_SIZE):              return 'text';
               default:                                                                   return 'char';
             }
           case 'text':                                                                   return 'longtext';
           case 'char':                                                                   return StatementGenerator.PGSQL_SINGLE_CHAR_TYPE;
           case 'name':                                                                   return StatementGenerator.PGSQL_NAME_TYPE
           case 'bpchar':                     
             switch (true) {
               case (dataTypeLength === undefined):                                       return 'longtext';
               case (dataTypeLength > StatementGenerator.MEDIUMTEXT_SIZE):                return 'longtext';
               case (dataTypeLength > StatementGenerator.TEXT_SIZE):                      return 'mediumtext';
               case (dataTypeLength > StatementGenerator.LARGEST_CHAR_SIZE):              return 'text';
               default:                                                                   return 'char';
             }
           case 'bytea':
             switch (true) {
               case (dataTypeLength === undefined):                                       return 'longblob';
               case (dataTypeLength > StatementGenerator.MEDIUMBLOB_SIZE):                return 'longblob';
               case (dataTypeLength > StatementGenerator.BLOB_SIZE):                      return 'mediumblob';
               case (dataTypeLength > StatementGenerator.LARGEST_VARBINARY_SIZE_SIZE):    return 'blob';
               default:                                                                   return 'varbinary';
             }
           case 'decimal':
           case 'numeric':                                                               return dataTypeLength === undefined ? StatementGenerator.PGSQL_NUMERIC_TYPE : 'decimal';
           case 'money':                                                                 return StatementGenerator.PGSQL_MONEY_TYPE
           case 'integer unsigned':                                                      return 'oid';
           case 'integer':                                                               return 'int';
           case 'real':                                                                  return 'float';
           case 'double precision':                                                      return 'double';
           case 'boolean':                                                               return StatementGenerator.BOOLEAN_TYPE
           case 'timestamp':                                                          
           case 'timestamp with time zone':                                           
           case 'timestamp without time zone':                                        
           case 'time without time zone':                                                return 'datetime';
           case 'time with time zone':                                                   return dataTypeLength === undefined ? 'time(6)' : 'time';
           case 'xml':                                                                   return 'longtext';     
           case 'jsonb':                                                                 return 'json';     
           case 'geography':                                                          
           case 'geography':                                                             return 'geometry';     
           case 'point':                                                                 return 'point';     
           case 'lseg':                                                               
           case 'path':                                                                  return 'linestring';     
           case 'box':                                                                   
           case 'polygon':                                                               return 'polygon';
           case 'circle':                                                                return this.dbi.INBOUND_CIRCLE_FORMAT === 'CIRCLE' ? 'json' : 'polygon';
           case 'line':                                                                  return 'json';     
           case 'uuid':                                                                  return StatementGenerator.UUID_TYPE
           case 'bit':
           case 'bit varying':    
             switch (true) {
           //  case (dataTypeLength === undefined):                                      return StatementGenerator.LARGEST_BIT_TYPE;
               case (dataTypeLength === undefined):                                      return StatementGenerator.LARGEST_VARCHAR_TYPE;
               case (dataTypeLength > StatementGenerator.MEDIUMTEXT_SIZE):               return 'longtext';
               case (dataTypeLength > StatementGenerator.TEXT_SIZE):                     return 'mediumtext';
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):          return 'text';
               case (dataTypeLength > 64):                                               return 'varchar';
           //  default:                                                                  return StatementGenerator.BIT_TYPE;
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
           case 'daterange':                                                             return 'json';
           case 'tsvector':                                                             
           case 'gtsvector':                                                             return 'json';
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
           case 'regtype':                                                              return 'int unsigned';
           case 'tid':                                                                  
           case 'xid':                                                                  
           case 'cid':                                                                  
           case 'txid_snapshot':                                                        return StatementGenerator.PGSQL_IDENTIFIER;
           case 'aclitem':                                                              
           case 'refcursor':                                                            return 'json';
           default:                                                                     
             if (dataType.indexOf('interval') === 0) {
               return StatementGenerator.INTERVAL_TYPE;
             }
             return dataType.toLowerCase();
         }
         break
       case 'MySQL':
       case 'MariaDB':
         switch (dataType) {
           case 'boolean':                                                             return this.dbi.TREAT_TINYINT1_AS_BOOLEAN ? 'boolean' : 'tinyint(1)';
           case 'set':                                                                 return 'json';
           case 'enum':                                                                return StatementGenerator.ENUM_TYPE
           default:                                                                    return dataType.toLowerCase();
         }
         break;
       case 'Vertica':
         switch (dataType) {
           case 'varchar':     
           case 'long varchar':                                                        return 'longtext';
             switch (true) {
               case (dataTypeLength > StatementGenerator.MEDIUMTEXT_SIZE):             return 'longtext';
               case (dataTypeLength > StatementGenerator.TEXT_SIZE):                   return 'mediumtext';
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):        return 'text';
               default:                                                                return 'varchar';
             }
           case 'char':
             switch (true) {
               case (dataTypeLength > StatementGenerator.MEDIUMTEXT_SIZE):             return 'longtext';
               case (dataTypeLength > StatementGenerator.TEXT_SIZE):                   return 'mediumtext';
               case (dataTypeLength > StatementGenerator.LARGEST_CHAR_SIZE):           return 'text';
               default:                                                                return 'char';
             }
           case 'binary':
           case 'varbinary':
           case 'long varbinary':
             switch (true) {
               case (dataTypeLength === undefined):                                    return 'longblob';
               case (dataTypeLength > StatementGenerator.MEDIUMBLOB_SIZE):             return 'longblob';
               case (dataTypeLength > StatementGenerator.BLOB_SIZE):                   return 'mediumblob';
               case (dataTypeLength > StatementGenerator.LARGEST_VARBINARY_SIZE_SIZE): return 'blob';
               default:                                                                return 'varbinary';
             }
           case 'float':                                                               return 'double';
           case 'time':                                                                return dataTypeLength === undefined ? 'time(6)' : 'time';             
           case 'timetz':                                                              return 'datetime(6)';
           case 'timestamptz':                                                         return 'datetime(6)';
           case 'timestamp':                                                           return 'datetime(6)';
           case 'xml':                                                                 return 'longtext';
           case 'uuid':                                                                return StatementGenerator.UUID_TYPE;
           case 'geography':                                                          
           case 'geography':                                                           return 'geometry';     
           default:                                                                    
             if (dataType.indexOf('interval') === 0) {
               return StatementGenerator.INTERVAL_TYPE;
             }
             return dataType.toLowerCase();
         }
         break;
       case 'MongoDB':
         switch (dataType) {
           case "string":
             switch (true) {
               case (dataTypeLength === undefined):                                    return 'longtext';
               case (dataTypeLength > StatementGenerator.MEDIUMTEXT_SIZE):             return 'longtext';
               case (dataTypeLength > StatementGenerator.TEXT_SIZE):                   return 'mediumtext';
               case (dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE):        return 'text';
               default:                                                                return 'varchar';
             }
           case "int":                                                                 return 'int';
           case "long":                                                                return 'bigint';
           case "double":                                                              return 'double';
           case "decimal":                                                             return 'decimal(65,30)';
           case "binData":                                                             return 'longblob';
           case "bool":                                                                return 'tinyint(1)';
           case "date":                                                                return 'datatime(6)';
           case "timestamp":                                                           return 'datetime(6)';
           case "objectId":                                                            return StatementGenerator.MONGO_OBJECT_ID
           case "object":                                                            
           case "array":                                                               return 'json';
           case "null":                                                                return StatementGenerator.MONGO_UNKNOWN_TYPE
           case "regex":                                                               return StatementGenerator.MONGO_REGEX_TYPE
           case "javascript":                                                          return 'longtext';
           case "javascriptWithScope":                                                 return 'longtext';
           case "minkey":                                                            
           case "maxkey":                                                              return "json";
           case "undefined":                                                         
           case 'dbPointer':                                                         
           case 'function':                                                          
           case 'symbol':                                                              return "json";
           default:                                                                    return dataType.toLowerCase();
         }
         break;
       case 'SNOWFLAKE':
         switch (dataType.toLowerCase()) {
           case "number":                                                              return 'decimal';
           case "float":                                                               return 'double';
           case "geography":                                                           return 'geometry';
           case "text":                                                                return dataTypeLength > StatementGenerator.LARGEST_VARCHAR_SIZE ? 'mediumtext' : 'varchar'; 
           case "binary":                                                              return dataTypeLength > StatementGenerator.LARGEST_VARBINARY_SIZE ? 'mediumblob' : 'varbinary'; 
           case "xml":                                                                 return StatementGenerator.XML_TYPE
           case "variant":                                                             return 'longblob';
           case "time":                                                                return dataTypeLength > 6 ? 'time(6)' : 'time'; 
           case "timestamp_ltz":                                                    
           case "timestamp_ntz":                                                       return dataTypeLength > 6 ? 'datetime(6)' : 'datetime'; 
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
       targetDataType = targetDataType === 'geomcollection'  ?  'geometrycollection' : targetDataType
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
    const setOperators = []
    const columnClauses = columnNames.map((columnName,idx) => {
      const dataType = YadamuLibrary.composeDataType(dataTypes[idx],sizeConstraints[idx])       
      let targetDataType = this.mapForeignDataType(tableMetadata.vendor,dataType.type,dataType.length,dataType.scale);
      targetDataTypes.push(targetDataType);

      let ensureNullable = false;
      switch (targetDataType) {
        case 'geometry':
        case 'point':
        case 'lseg':
        case 'linestring':
        case 'box':
        case 'path':
        case 'polygon':
        case 'multipoint':
        case 'multilinestring':
        case 'multipolygon':
        case 'geomcollection':
        case 'geometrycollection':
          switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
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
         case 'bit':
           setOperators.push('conv(?,2,10)+0');
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

    const tableInfo =  { 
       ddl             : createStatement, 
       dml             : insertStatement, 
       columnNames     : columnNames,
       rowConstructor  : rowConstructor,
       targetDataTypes : targetDataTypes, 
       insertMode      : insertMode,
       _BATCH_SIZE     : this.dbi.BATCH_SIZE,
       _SPATIAL_FORMAT : this.dbi.INBOUND_SPATIAL_FORMAT
    }
    
    if (tableMetadata.dataFile) {
      const loadColumnNames = []
      const setOperations = []
      const copyOperators = targetDataTypes.map((targetDataType,idx) => {
		const dataType = YadamuLibrary.decomposeDataType(targetDataType)
        const psuedoColumnName = `@YADAMU_${String(idx+1).padStart(3,"0")}`
        loadColumnNames.push(psuedoColumnName);
        setOperations.push(`"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, ${psuedoColumnName})`)
	    switch (dataType.type.toLowerCase()) {
          case 'point':
          case 'linestring':
          case 'polygon':
          case 'geometry':
          case 'multipoint':
          case 'multilinestring':
          case 'multipolygon':
          case 'geometry':                             
          case 'geometrycollection':
          case 'geomcollection':
            let spatialFunction
            switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
              case "WKB":
              case "EWKB":
                spatialFunction = `ST_GeomFromWKB(UNHEX(${psuedoColumnName}))`;
                break;
              case "WKT":
                case "EWRT":
                spatialFunction = `ST_GeomFromText(${psuedoColumnName})`;
                break;
              case "GeoJSON":
                spatialFunction = `ST_GeomFromGeoJSON(${psuedoColumnName})`;
                break;
              default:
                return `ST_GeomFromWKB(UNHEX(${psuedoColumnName}))`;
            }
            setOperations[idx] = `"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, ${spatialFunction})`
            break
          case 'binary':                              
          case 'varbinary':                              
          case 'blob':                                 
          case 'tinyblob':                             
          case 'mediumblob':                           
          case 'longblob':                             
            setOperations[idx] = `"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, UNHEX(${psuedoColumnName}))`
            break;
          case 'time':
            setOperations[idx] = `"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(INSTR(${psuedoColumnName},'.') > 0,str_to_date(${psuedoColumnName},'%Y-%m-%dT%T.%f'),str_to_date(${psuedoColumnName},'%Y-%m-%dT%T')))`
            break;
          case 'datetime':
          case 'timestamp':
            setOperations[idx] = `"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(INSTR(${psuedoColumnName},'.') > 0,str_to_date(${psuedoColumnName},'%Y-%m-%dT%T.%f'),str_to_date(${psuedoColumnName},'%Y-%m-%dT%T')))`
            break;
		  case 'boolean':
            setOperations[idx] = `"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(${psuedoColumnName} = 'true',1,0))`
            break;
          case 'tinyint':    
            switch (true) {
              case ((dataType.length === 1) && this.dbi.TREAT_TINYINT1_AS_BOOLEAN):
                setOperations[idx] = `"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(${psuedoColumnName} = 'true',1,0))`
                break;
            }
            break;                 
          /*
          case 'smallint':
          case 'mediumint':
          case 'integer':
          case 'bigint':
          case 'decimal':                                           
          case 'float':                                           
          case 'double':                                           
          case 'bit':
          case 'date':
          case 'year':                            
          case 'char':                              
          case 'varchar':                              
          case 'text':                                 
          cae 'tinytext':
          case 'mediumtext':                           
          case 'longtext':                             
          case 'set':                                  
          case 'enum':                                 
          case 'json':                                 
          case 'xml':                                  
          */
          default:
        }
      })
   
	  // Partitioned Tables need one entry per partition 

      if (tableMetadata.hasOwnProperty('partitionCount')) {
  	    tableInfo.copy = tableMetadata.dataFile.map((filename,idx) => {
		  return  {
            dml             : `load data infile '${filename.split(path.sep).join(path.posix.sep)}' into table "${this.targetSchema}"."${tableMetadata.tableName}" character set UTF8 fields terminated by ',' optionally enclosed by '"' ESCAPED BY '"' lines terminated by '\n' (${loadColumnNames.join(",")}) SET ${setOperations.join(",")}`
		  , partitionCount  : tableMetadata.partitionCount
		  , partitionID     : idx+1
          }
		})
	  }
	  else {
    	tableInfo.copy = {
          dml         : `load data infile '${tableMetadata.dataFile.split(path.sep).join(path.posix.sep)}' into table "${this.targetSchema}"."${tableMetadata.tableName}" character set UTF8 fields terminated by ',' optionally enclosed by '"' ESCAPED BY '"' lines terminated by '\n' (${loadColumnNames.join(",")}) SET ${setOperations.join(",")}`
        }
	  }
	  
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
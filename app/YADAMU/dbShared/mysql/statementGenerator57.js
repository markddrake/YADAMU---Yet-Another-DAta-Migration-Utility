"use strict";

// Code Shared by MySQL 5.7 and MariaDB. 

const unboundedTypes = ['date','time','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json','set','enum'];
const spatialTypes   = ['geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection'];
const nationalTypes  = ['nchar','nvarchar'];
const integerTypes   = ['tinyint','mediumint','smallint','int','bigint']

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, spatialFormat, batchSize, commitSize) {
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.spatialFormat = spatialFormat
    this.batchSize = batchSize
    this.commitSize = commitSize;    
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
           case 'bit':                             return 'boolean';
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
           case 'double precision':                 return 'double';
           case 'real':                             return 'float';
           case 'integer':                          return 'int';
           case 'xml':                              return 'longtext';     
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
           case 'set':                            return 'varchar(512)';
           case 'enum':                           return 'varchar(512)';
           default:                               return dataType.toLowerCase();
         }
         break;
       default: 
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
  
     if (unboundedTypes.includes(targetDataType)) {
       return targetDataType
     }
  
     if (spatialTypes.includes(targetDataType)) {
       return targetDataType
     }
  
     if (nationalTypes.includes(targetDataType)) {
       return targetDataType + '(' + length + ')'
     }
  
     if (scale) {
       if (integerTypes.includes(targetDataType)) {
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
      
  generateTableInfo(metadata) {
      
    let insertMode = 'Batch';
  
    const columnNames = metadata.columns.split(',');
    const dataTypes = metadata.dataTypes
    const sizeConstraints = metadata.sizeConstraints
    const targetDataTypes = [];
    const setOperators = []
  
    const columnClauses = columnNames.map(function(columnName,idx) {    
        
       const dataType = {
                type : dataTypes[idx]
             }    
        
       const sizeConstraint = sizeConstraints[idx]
       if ((sizeConstraint !== null) && (sizeConstraint.length > 0)) {
          const components = sizeConstraint.split(',');
          dataType.length = parseInt(components[0])
          if (components.length > 1) {
            dataType.scale = parseInt(components[1])
          }
       }
    
       let targetDataType = this.mapForeignDataType(metadata.vendor,dataType.type,dataType.length,dataType.scale);
    
       targetDataTypes.push(targetDataType);
       let ensureNullable = false;
       switch (targetDataType) {
         case 'geometry':
            insertMode = 'Iterative';
            switch (this.spatialFormat) {
              case "WKB":
              case "EWKB":
                setOperators.push(' ' + columnName + ' = ST_GeomFromWKB(UNHEX(?))');
                break
              case "WKT":
              case "EWRT":
                setOperators.push(' ' + columnName + ' =  ST_GeomFromText(?)');
                break;
              default:
                setOperators.push(' ' + columnName + ' = ST_GeomFromWKB(UNHEX(?))');
            }              
            break;                                                 
         case 'timestamp':
            ensureNullable = true;
         default:
           setOperators.push(' ' + columnName + ' = ?')
       }
       return `${columnName} ${this.getColumnDataType(targetDataType,dataType.length,dataType.scale)} ${ensureNullable === true ? 'null':''}`
    },this)
                                       
    const createStatement = `create table if not exists "${this.targetSchema}"."${metadata.tableName}"(\n  ${columnClauses.join(',')})`;
    let insertStatement = `insert into "${this.targetSchema}"."${metadata.tableName}"`;
    if (insertMode === 'Iterative') {
      insertStatement += ` set` + setOperators.join(',');
    }
    else {
      insertStatement += `(${metadata.columns}) values ?`;
    }
    return { 
       ddl             : createStatement, 
       dml             : insertStatement, 
       targetDataTypes : targetDataTypes, 
       insertMode      : insertMode,
       batchSize       : this.batchSize, 
       commitSize      : this.commitSize
    }
  }
  
  async generateStatementCache(executeDDL,vendor) {
      
    const statementCache = {}
    const tables = Object.keys(this.metadata); 

    const ddlStatements = tables.map(function(table,idx) {
      const tableMetadata = this.metadata[table];
      const tableInfo = this.generateTableInfo(tableMetadata);
	  tableInfo.dataTypes = this.dbi.decomposeDataTypes(tableInfo.targetDataTypes);
      statementCache[this.metadata[table].tableName] = tableInfo;
      return tableInfo.ddl;
    },this)
    if (executeDDL === true) {
      await this.dbi.executeDDL(ddlStatements)
    }
    return statementCache;
  }

}

module.exports = StatementGenerator
"use strict";

// Code Shared by MySQL 5.7 and MariaDB. 

const unboundedTypes = ['date','time','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json','set','enum'];
const spatialTypes   = ['geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection'];
const nationalTypes  = ['nchar','nvarchar'];
const integerTypes   = ['tinyint','mediumint','smallint','int','bigint']

class StatementGenerator {
  
  constructor(dbWriter, status, logWriter, ) {
      
    this.dbWriter = dbWriter;
    this.status = status;
    this.logWriter = logWriter;
    
  }
    
  mapForeignDataType(vendor, dataType, dataTypeLength, dataTypeSize) {
    switch (vendor) {
       case 'Oracle':
         switch (dataType) {
           case 'VARCHAR2':        return 'varchar';
           case 'NVARCHAR2':       return 'varchar';
           case 'NUMBER':          return 'decimal';
           case 'CLOB':            return 'longtext';
           case 'BLOB':            return 'longblob';
           case 'NCLOB':           return 'longtext';
           case 'XMLTYPE':         return 'longtext';
           case 'BFILE':           return 'varchar(2048)';
           case 'ROWID':           return 'varchar(32)';
           case 'RAW':             return 'binary';
           case 'ROWID':           return 'varchar(32)';
           case 'ANYDATA':         return 'longtext';
           default :
             if (dataType.indexOf('TIME ZONE') > -1) {
               return 'timestamp'; 
             }
             if (dataType.indexOf('INTERVAL') === 0) {
               return 'timestamp'; 
             }
             if (dataType.indexOf('XMLTYPE') > -1) { 
               return 'varchar(16)';
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
           case 'datetime2':                       return 'datetime';
           case 'datetimeoffset':                  return 'datetime';
           case 'geography':                       return 'json';
           case 'geogmetry':                       return 'json';
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
       case 'Postgres':                            return dataType.toLowerCase();
         break
       case 'MySQL':
         switch (dataType) {
           case 'set':                             return 'varchar(512)';
           case 'enum':                            return 'varchar(512)';
           default:                                return dataType.toLowerCase();
         }
         break;
       case 'MariaDB':
         switch (dataType) {
           case 'set':                             return 'varchar(512)';
           case 'enum':                            return 'varchar(512)';
           default:                                return dataType.toLowerCase();
         }
         break;
       default:                                    return dataType.toLowerCase();
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
       return targetDataType
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
      
  generateStatements(vendor, schema, metadata) {
      
     let useSetClause = false;
     
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
                                             if (sizeConstraint.length > 0) {
                                                const components = sizeConstraint.split(',');
                                                dataType.length = parseInt(components[0])
                                                if (components.length > 1) {
                                                  dataType.scale = parseInt(components[1])
                                                }
                                             }
                                          
                                             let targetDataType = this.mapForeignDataType(vendor,dataType.type,dataType.length,dataType.scale);
                                         
                                             targetDataTypes.push(targetDataType);
                                             
                                             switch (targetDataType) {
                                               case 'geometry':
                                                  useSetClause = true;
                                                  setOperators.push(' "' + columnName + '" = ST_GEOMFROMGEOJSON(?)');
                                                  break;
                                                  
                                               default:
                                                 setOperators.push(' "' + columnName + '" = ?')
                                             }
                                             return `${columnName} ${this.getColumnDataType(targetDataType,dataType.length,dataType.scale)}`
                                          },this)
                                        
      const createStatement = `create table if not exists "${schema}"."${metadata.tableName}"(\n  ${columnClauses.join(',')})`;
      let insertStatement = `insert into "${schema}"."${metadata.tableName}"`;
      if (useSetClause) {
        insertStatement += ` set` + setOperators.join(',');
      }
      else {
        insertStatement += `(${metadata.columns}) values ?`;
      }
      return { ddl : createStatement, dml : insertStatement, targetDataTypes : targetDataTypes, useSetClause : useSetClause}
  }
  
  async generateStatementCache( schema, systemInformation, metadata) {
      
    const ddlStatements = [];  
    const statementCache = {}
    const tables = Object.keys(metadata); 

    tables.forEach(async function(table,idx) {
                          const tableMetadata = metadata[table];
                          const sql = this.generateStatements(systemInformation.vendor, schema,tableMetadata);
                          statementCache[table] = sql;
                          ddlStatements[idx] = sql.ddl;
    },this)
    const results = await this.dbWriter.executeDDL(ddlStatements)
    return statementCache;
  }

}

module.exports = StatementGenerator
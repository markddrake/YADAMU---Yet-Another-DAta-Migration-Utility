
import path                     from 'path';

import YadamuLibrary            from '../../lib/yadamuLibrary.js';
import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

// Code Shared by MySQL 5.7 and MariaDB. 

class MariadbStatementGenerator extends YadamuStatementGenerator {

  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }

  getMappedDataType(dataType,sizeConstraint) {
	  
      const mappedDataType = super.getMappedDataType(dataType,sizeConstraint)
      const length = parseInt(sizeConstraint)

      switch (mappedDataType) {
		  
        case this.dbi.DATA_TYPES.BLOB_TYPE:
          switch (true) {
            case (length === undefined) :                                return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length === -1) :                                       return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length > this.dbi.DATA_TYPES.MEDIUMBLOB_LENGTH) :      return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length > this.dbi.DATA_TYPES.BLOB_LENGTH) :            return this.dbi.DATA_TYPES.MYSQL_MEDIUMBLOB_TYPE
            case (length > this.dbi.DATA_TYPES.TINYBLOB_LENGTH) :        return this.dbi.DATA_TYPES.MYSQL_BLOB_TYPE
            default:                                                     return this.dbi.DATA_TYPES.MYSQL_TINYBLOB_TYPE
          }
		  
        case this.dbi.DATA_TYPES.CLOB_TYPE:
          switch (true) {
            case (length === undefined) :                                return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length === -1) :                                       return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.MEDIUMTEXT_LENGTH) :      return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.TEXT_LENGTH) :            return this.dbi.DATA_TYPES.MYSQL_MEDIUMTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.TINYTEXT_LENGTH) :        return this.dbi.DATA_TYPES.MYSQL_TEXT_TYPE
            default:                                                     return this.dbi.DATA_TYPES.MYSQL_TINYTEXT_TYPE
          }
        case this.dbi.DATA_TYPES.CHAR_TYPE:
        case this.dbi.DATA_TYPES.VARCHAR_TYPE:
          switch (true) {
  		    case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH) :         return this.dbi.DATA_TYPES.MYSQL_MEDIUMTEXT_TYPE
		    case (length > this.dbi.DATA_TYPES.CHAR_LENGTH) :            return this.dbi.DATA_TYPES.VARCHAR_TYPE
            default:                                                     return mappedDataType
          }
        
        case this.dbi.DATA_TYPES.BINARY_TYPE:
        case this.dbi.DATA_TYPES.VARBINARY_TYPE:
          switch (true) {
  		    case (length > this.dbi.DATA_TYPES.VARBINARY_LENGTH) :       return this.dbi.DATA_TYPES.MYSQL_MEDIUMTEXT_TYPE
		    case (length > this.dbi.DATA_TYPES.BINARY_LENGTH) :          return this.dbi.DATA_TYPES.VARCHAR_TYPE
            default:                                                     return mappedDataType
          }

        case this.dbi.DATA_TYPES.TIME_TYPE:
        case this.dbi.DATA_TYPES.DATETIME_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMPTZ_TYPE:
          switch (true) {
            case (length > this.dbi.DATA_TYPES.TIMESTAMP_PRECISON) :     return `${mappedDataType}(${this.dbi.DATA_TYPES.TIMESTAMP_PRECISON})`
            default:                                                     return mappedDataType
          }
        default:
         return mappedDataType
      }   
     
  }

  
  getColumnDataType(mappedDataType,sizeConstraint) {
     
     if (mappedDataType.endsWith(" unsigned")) {
       return mappedDataType
     }
   
     if (mappedDataType === "boolean") {
       return 'tinyint(1)'
     }
      
     return super.getColumnDataType(mappedDataType,sizeConstraint)
  }

  generateDDLStatement(schema,tableName,columnDefinitions,mappedDataTypes) {
      
    return `create table  if not exists "${schema}"."${tableName}"(\n  ${columnDefinitions.join(',')})`;
    
  }

  generateDMLStatement(schema,tableName,columnNames,insertOperators) {
    return `insert into "${schema}"."${tableName}" ("${columnNames.join('","')}") values `;  
  }

  generateCopyStatement(schema,tableName,filename,columnNames,setOperations) {
	 filename = filename.split(path.sep).join(path.posix.sep)
    return `load data infile '${filename}' into table "${schema}"."${tableName}" character set UTF8 fields terminated by ',' optionally enclosed by '"' ESCAPED BY '"' lines terminated by '\n' (${columnNames.join(",")}) SET ${setOperations.join(",")}`  
}
  
  generateCopyOperation(tableMetadata,mappedDataTypes) {
    const columnNames = []
    const setOperations = mappedDataTypes.map((mappedDataType,idx) => {
      const dataType = YadamuLibrary.decomposeDataType(mappedDataType)
      const psuedoColumnName = `@YADAMU_${String(idx+1).padStart(3,"0")}`
      columnNames.push(psuedoColumnName);
      switch (dataType.type.toLowerCase()) {
        case this.dbi.DATA_TYPES.SPATIAL_TYPE:                
        case this.dbi.DATA_TYPES.POINT_TYPE:                
        case this.dbi.DATA_TYPES.LINE_TYPE:                 
        case this.dbi.DATA_TYPES.POLYGON_TYPE:              
        case this.dbi.DATA_TYPES.MULTI_POINT_TYPE:          
        case this.dbi.DATA_TYPES.MULTI_LINE_TYPE:           
        case this.dbi.DATA_TYPES.MULTI_POLYGON_TYPE:        
        case this.dbi.DATA_TYPES.GEOMETRY_COLLECTION_TYPE:  
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
          return `"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, ${spatialFunction})`
        case this.dbi.DATA_TYPES.BINARY_TYPE:
        case this.dbi.DATA_TYPES.VARBINARY_TYPE:
        case this.dbi.DATA_TYPES.BLOB_TYPE:
        case this.dbi.DATA_TYPES.TINYBLOB_TYPE:
        case this.dbi.DATA_TYPES.MEDIUMBLOB_TYPE:
        case this.dbi.DATA_TYPES.LONGBLOB_TYPE:
          return `"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, UNHEX(${psuedoColumnName}))`
        case this.dbi.DATA_TYPES.TIME_TYPE:
          return `"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(INSTR(${psuedoColumnName},'.') > 0,str_to_date(${psuedoColumnName},'%Y-%m-%dT%T.%f'),str_to_date(${psuedoColumnName},'%Y-%m-%dT%T')))`
        case this.dbi.DATA_TYPES.DATETIME_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
          return `"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(INSTR(${psuedoColumnName},'.') > 0,str_to_date(${psuedoColumnName},'%Y-%m-%dT%T.%f'),str_to_date(${psuedoColumnName},'%Y-%m-%dT%T')))`
        case 'boolean':
          return `"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(${psuedoColumnName} = 'true',1,0))`
        case TINYINT_TYPE:
          switch (true) {
            case ((dataType.length === 1) && this.dbi.TREAT_TINYINT1_AS_BOOLEAN):
              return `"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(${psuedoColumnName} = 'true',1,0))`
          }
        default:
          return `"${columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, ${psuedoColumnName})`
      }
    })
   
    // Partitioned Tables need one entry per partition 

    if (tableMetadata.hasOwnProperty('partitionCount')) {
      return tableMetadata.dataFile.map((filename,idx) => {
        return {
          dml             : this.generateCopyStatement(this.targetSchema,tableMetadata.tableName,columnNames,setOperations)
        , partitionCount  : tableMetadata.partitionCount
        , partitionID     : idx+1
        }
      })
    }
    else {
      return {
        dml : this.generateCopyStatement(this.targetSchema,tableMetadata.tableName,columnNames,setOperations)
      }
    }  
  }

  generateTableInfo(tableMetadata) {
      
    let insertMode = 'Batch';
	
    const columnNames = tableMetadata.columnNames

    const mappedDataTypes = [];
    const insertOperators = []
    const columnDefinitions = columnNames.map((columnName,idx) => {
      let addNullClause = false;
      const mappedDataType = tableMetadata.source ? tableMetadata.dataTypes[idx] : this.getMappedDataType(tableMetadata.dataTypes[idx],tableMetadata.sizeConstraints[idx])
      mappedDataTypes.push(mappedDataType)
      switch (mappedDataType) {
        case this.dbi.DATA_TYPES.SPATIAL_TYPE:                
        case this.dbi.DATA_TYPES.POINT_TYPE:                
        case this.dbi.DATA_TYPES.LINE_TYPE:                 
        case this.dbi.DATA_TYPES.POLYGON_TYPE:              
        case this.dbi.DATA_TYPES.MULTI_POINT_TYPE:          
        case this.dbi.DATA_TYPES.MULTI_LINE_TYPE:           
        case this.dbi.DATA_TYPES.MULTI_POLYGON_TYPE:        
        case this.dbi.DATA_TYPES.GEOMETRY_COLLECTION_TYPE:  
          switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
            case "WKB":
            case "EWKB":
              insertOperators.push('ST_GeomFromWKB(?)');
              break
            case "WKT":
            case "EWRT":
              insertOperators.push('ST_GeomFromText(?)');
              break;
            case "GeoJSON":
              insertOperators.push('ST_GeomFromGeoJSON(?)');
              break;
            default:
              insertOperators.push('ST_GeomFromWKB(?)');
          }              
          break;                                                 
        case this.dbi.DATA_TYPES.BIT_STRING_TYPE:
          insertOperators.push('conv(?,2,10)+0');
          break;
        case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMPTZ_TYPE:
           addNullClause = true;
        default:
          insertOperators.push('?')
      }
      return `"${columnName}" ${this.generateStorageClause(mappedDataType,tableMetadata.sizeConstraints[idx])} ${addNullClause === true ? 'null':''}`      
	  
    })
                                       
    const rowConstructor = `(${insertOperators.join(',')})`

    const tableInfo = {
      ddl             : this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,mappedDataTypes)
    , dml             : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,columnNames,insertOperators)
    , columnNames     : tableMetadata.columnNames
    , sourceDataTypes : tableMetadata.source ? tableMetadata.source.dataTypes : tableMetadata.dataTypes
    , targetDataTypes : mappedDataTypes
    , insertMode      : insertMode
	, rowConstructor  : rowConstructor
    , _BATCH_SIZE     : this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT : this.dbi.INBOUND_SPATIAL_FORMAT
    }
    
    // Add Support for Copy based Operations
    
    if (tableMetadata.dataFile) {
      tableInfo.copy = this.generateCopyStatements(tableMetadata,mappedDataTypes) 
    }
        
    return tableInfo
  }      

}

export { MariadbStatementGenerator as default }

/*
**
   
  mapForeignDataType(vendor, dataType, dataTypeLength, dataTypeScale) {
    switch (vendor) {
       case 'Oracle':
         switch (dataType) {
           case 'VARCHAR2':                                                              return 'varchar';
           case 'NVARCHAR2':                                                             return 'varchar';
           case 'NUMBER':                                                                return dataTypeLength === undefined ? MariadbStatementGenerator.ORACLE_NUMERIC_TYPE : 'decimal';
           case 'BINARY_FLOAT':                                                          return 'float';
           case 'BINARY_DOUBLE':                                                         return 'double';
           case 'CLOB':                                                                  return 'longtext';
           case 'BLOB':                                                                  return 'longblob';
           case 'NCLOB':                                                                 return 'longtext';
           case 'XMLTYPE':                                                               return 'longtext';
           case 'TIMESTAMP':                                                             return dataTypeLength > 6 ? 'datetime(6)' : 'datetime';
           case 'BFILE':                                                                 return MariadbStatementGenerator.BFILE_TYPE;
           case 'ROWID':                                                                 return MariadbStatementGenerator.ROWID_TYPE;
           case 'RAW':                                                                   return 'varbinary';
           case 'ANYDATA':                                                               return 'longtext';
           case '"MDSYS"."SDO_GEOMETRY"':                                                return 'geometry';
           case 'BOOLEAN':                                                               return this.dbi.TREAT_TINYINT1_AS_BOOLEAN ? 'boolean' : 'tinyint(1)'
           default :
             switch (true) {
               case (dataType.indexOf('TIME ZONE') > -1):                                return 'datetime'; 
               case (dataType.indexOf('INTERVAL') === 0):                                return MariadbStatementGenerator.INTERVAL_TYPE;
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
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMTEXT_SIZE):               return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.TEXT_SIZE):                     return 'mediumtext';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_VARCHAR_SIZE):          return 'text';
               default:                                                                  return 'varchar';
             }                                                                          
           case 'char':                                                                 
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMTEXT_SIZE):               return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.TEXT_SIZE):                     return 'mediumtext';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_CHAR_SIZE):             return 'text';
               default:                                                                  return 'char';
             }                                                                          
           case 'nvarchar':                                                             
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMTEXT_SIZE):               return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.TEXT_SIZE):                     return 'mediumtext';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_VARCHAR_SIZE):          return 'text';
               default:                                                                  return 'varchar';
             }                                                                          
           case 'nchar':                                                                
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMTEXT_SIZE):               return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.TEXT_SIZE):                     return 'mediumtext';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_CHAR_SIZE):             return 'text';
               default:                                                                  return 'char';
             }
             
           case 'text':                            return 'longtext';                   
           case 'ntext':                                                                 return 'longtext';
           case 'binary':
             switch (true) {
               case (dataTypeLength === -1):                                             return 'longblob';
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMBLOB_SIZE):               return 'longblob';
               case (dataTypeLength > MariadbStatementGenerator.BLOB_SIZE):                     return 'mediumblob';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_BINARY_SIZE_SIZE):      return 'blob';
               default:                                                                  return 'binary';
             }
           case 'varbinary':
             switch (true) {
               case (dataTypeLength === -1):                                             return 'longblob';
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMBLOB_SIZE):               return 'longblob';
               case (dataTypeLength > MariadbStatementGenerator.BLOB_SIZE):                     return 'mediumblob';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_VARBINARY_SIZE_SIZE):   return 'blob';
               default:                                                                  return 'varbinary';
             }
           case 'image':                                                                 return 'longblob';
           case 'boolean':                                                               return MariadbStatementGenerator.BOOLEAN_TYPE
           case 'tinyint':                                                               return 'smallint';
           case 'mediumint':                                                             return 'int';
           case 'money':                                                                 return MariadbStatementGenerator.MSSQL_MONEY_TYPE
           case 'smallmoney':                                                            return MariadbStatementGenerator.MSSQL_SMALL_MONEY_TYPE;
           case 'real':                                                                  return 'float';
           case 'bit':                                                                   return MariadbStatementGenerator.BOOLEAN_TYPE;
           case 'datetime':                                                              return 'datetime(3)';
           case 'time':                                                                  return dataTypeLength > 6 ? 'time(6)' : 'time';
           case 'datetime2':                                                             return dataTypeLength > 6 ? 'datetime(6)' : 'datetime';
           case 'datetimeoffset':                                                        return dataTypeLength > 6 ? 'datetime(6)' : 'datetime';
           case 'smalldate':                                                             return 'datetime';
           case 'geography':                                                             return 'geometry';
           case 'geometry':                                                              return 'geometry';
           case 'hierarchyid':                                                           return MariadbStatementGenerator.HIERARCHY_TYPE
           case 'rowversion':                                                            return 'binary(8)';
           case 'uniqueidentifier':                                                      return MariadbStatementGenerator.UUID_TYPE
           case 'xml':                                                                   return 'longtext';
           default:                                                                      return dataType.toLowerCase();
         }
         break;
       case 'Postgres':    
         switch (dataType) {
           case 'character varying':     
             switch (true) {
               case (dataTypeLength === undefined):                                       return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMTEXT_SIZE):                return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.TEXT_SIZE):                      return 'mediumtext';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_VARCHAR_SIZE):           return 'text';
               default:                                                                   return 'varchar';
             }
           case 'character':
             switch (true) {
               case (dataTypeLength === undefined):                                       return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMTEXT_SIZE):                return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.TEXT_SIZE):                      return 'mediumtext';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_CHAR_SIZE):              return 'text';
               default:                                                                   return 'char';
             }
           case 'text':                                                                   return 'longtext';
           case 'char':                                                                   return MariadbStatementGenerator.PGSQL_SINGLE_CHAR_TYPE;
           case 'name':                                                                   return MariadbStatementGenerator.PGSQL_NAME_TYPE
           case 'bpchar':                     
             switch (true) {
               case (dataTypeLength === undefined):                                       return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMTEXT_SIZE):                return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.TEXT_SIZE):                      return 'mediumtext';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_CHAR_SIZE):              return 'text';
               default:                                                                   return 'char';
             }
           case 'bytea':
             switch (true) {
               case (dataTypeLength === undefined):                                       return 'longblob';
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMBLOB_SIZE):                return 'longblob';
               case (dataTypeLength > MariadbStatementGenerator.BLOB_SIZE):                      return 'mediumblob';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_VARBINARY_SIZE_SIZE):    return 'blob';
               default:                                                                   return 'varbinary';
             }
           case 'decimal':
           case 'numeric':                                                               return dataTypeLength === undefined ? MariadbStatementGenerator.PGSQL_NUMERIC_TYPE : 'decimal';
           case 'money':                                                                 return MariadbStatementGenerator.PGSQL_MONEY_TYPE
           case 'integer unsigned':                                                      return 'oid';
           case 'integer':                                                               return 'int';
           case 'real':                                                                  return 'float';
           case 'double precision':                                                      return 'double';
           case 'boolean':                                                               return MariadbStatementGenerator.BOOLEAN_TYPE
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
           case 'uuid':                                                                  return MariadbStatementGenerator.UUID_TYPE
           case 'bit':                                                                        
           case 'bit varying':    
             switch (true) {
           //  case (dataTypeLength === undefined):                                      return MariadbStatementGenerator.LARGEST_BIT_TYPE;
               case (dataTypeLength === undefined):                                      return MariadbStatementGenerator.LARGEST_VARCHAR_TYPE;
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMTEXT_SIZE):               return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.TEXT_SIZE):                     return 'mediumtext';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_VARCHAR_SIZE):          return 'text';
               case (dataTypeLength > 64):                                               return 'varchar';
           //  default:                                                                  return MariadbStatementGenerator.BIT_TYPE;
               default:                                                                  return 'varchar'
             }
           case 'cidr':
           case 'inet':                                                                  return MariadbStatementGenerator.INET_ADDR_TYPE
           case 'macaddr':                                                              
           case 'macaddr8':                                                              return MariadbStatementGenerator.MAC_ADDR_TYPE
           case 'int4range':                                                            
           case 'int8range':                                                            
           case 'numrange':                                                             
           case 'tsrange':                                                              
           case 'tstzrange':                                                            
           case 'daterange':                                                             return 'json';
           case 'tsvector':                                                             
           case 'gtsvector':                                                             return 'json';
           case 'tsquery':                                                               return MariadbStatementGenerator.LARGEST_VARCHAR_TYPE;
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
           case 'txid_snapshot':                                                        return MariadbStatementGenerator.PGSQL_IDENTIFIER;
           case 'aclitem':                                                              
           case 'refcursor':                                                            return 'json';
           default:                                                                     
             if (dataType.indexOf('interval') === 0) {
               return MariadbStatementGenerator.INTERVAL_TYPE;
             }
             return dataType.toLowerCase();
         }
         break
       case 'MySQL':
       case 'MariaDB':
         switch (dataType) {
           case 'boolean':                                                             return this.dbi.TREAT_TINYINT1_AS_BOOLEAN ? 'boolean' : 'tinyint(1)';
           case 'set':                                                                 return 'json';
           case 'enum':                                                                return MariadbStatementGenerator.ENUM_TYPE
           default:                                                                    return dataType.toLowerCase();
         }
         break;
       case 'Vertica':
         switch (dataType) {
           case 'varchar':     
           case 'long varchar':                                                        return 'longtext';
             switch (true) {
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMTEXT_SIZE):             return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.TEXT_SIZE):                   return 'mediumtext';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_VARCHAR_SIZE):        return 'text';
               default:                                                                return 'varchar';
             }
           case 'char':
             switch (true) {
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMTEXT_SIZE):             return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.TEXT_SIZE):                   return 'mediumtext';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_CHAR_SIZE):           return 'text';
               default:                                                                return 'char';
             }
           case 'binary':
           case 'varbinary':
           case 'long varbinary':
             switch (true) {
               case (dataTypeLength === undefined):                                    return 'longblob';
               case (dataTypeLength > this.dbi.DATA_TYPES.MEDIUM_BLOB_LENGTH):             return 'longblob';
               case (dataTypeLength > MariadbStatementGenerator.BLOB_SIZE):                   return 'mediumblob';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_VARBINARY_SIZE_SIZE): return 'blob';
               default:                                                                return 'varbinary';
             }
           case 'float':                                                               return 'double';
           case 'time':                                                                return dataTypeLength === undefined ? 'time(6)' : 'time';             
           case 'timetz':                                                              return 'datetime(6)';
           case 'timestamptz':                                                         return 'datetime(6)';
           case 'timestamp':                                                           return 'datetime(6)';
           case 'xml':                                                                 return 'longtext';
           case 'uuid':                                                                return MariadbStatementGenerator.UUID_TYPE;
           case 'geography':                                                          
           case 'geography':                                                           return 'geometry';     
           default:                                                                    
             if (dataType.indexOf('interval') === 0) {
               return MariadbStatementGenerator.INTERVAL_TYPE;
             }
             return dataType.toLowerCase();
         }
         break;
       case 'MongoDB':
         switch (dataType) {
           case "string":
             switch (true) {
               case (dataTypeLength === undefined):                                    return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.MEDIUMTEXT_SIZE):             return 'longtext';
               case (dataTypeLength > MariadbStatementGenerator.TEXT_SIZE):                   return 'mediumtext';
               case (dataTypeLength > MariadbStatementGenerator.LARGEST_VARCHAR_SIZE):        return 'text';
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
           case "objectId":                                                            return MariadbStatementGenerator.MONGO_OBJECT_ID
           case "object":                                                            
           case "array":                                                               return 'json';
           case "null":                                                                return MariadbStatementGenerator.MONGO_UNKNOWN_TYPE
           case "regex":                                                               return MariadbStatementGenerator.MONGO_REGEX_TYPE
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
           case "text":                                                                return dataTypeLength > MariadbStatementGenerator.LARGEST_VARCHAR_SIZE ? 'mediumtext' : 'varchar'; 
           case "binary":                                                              return dataTypeLength > MariadbStatementGenerator.LARGEST_VARBINARY_SIZE ? 'mediumblob' : 'varbinary'; 
           case "xml":                                                                 return MariadbStatementGenerator.XML_TYPE
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
  
**
*/
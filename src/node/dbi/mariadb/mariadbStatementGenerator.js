
import path                     from 'path';

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
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

        case this.dbi.DATA_TYPES.CHAR_TYPE:
          switch (true) {
            case (isNaN(length)):                                        return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length === undefined):                                 return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length === -1):                                        return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.MEDIUMTEXT_LENGTH):       return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.TEXT_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH):          return this.dbi.DATA_TYPES.MYSQL_TEXT_TYPE
            case (length > this.dbi.DATA_TYPES.CHAR_LENGTH):             return this.dbi.DATA_TYPES.VARCHAR_TYPE
            default:                                                     return mappedDataType
          }

        case this.dbi.DATA_TYPES.VARCHAR_TYPE:
          switch (true) {
            case (isNaN(length)):                                        return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length === undefined):                                 return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length === -1):                                        return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.MEDIUMTEXT_LENGTH):       return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.TEXT_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH):          return this.dbi.DATA_TYPES.MYSQL_TEXT_TYPE
            default:                                                     return mappedDataType
          }

        case this.dbi.DATA_TYPES.CLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_MEDIUMTEXT_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_TEXT_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_TINYTEXT_TYPE:
          switch (true) {
            case (isNaN(length)):                                        return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length === undefined):                                 return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length === -1):                                        return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.MEDIUMTEXT_LENGTH):       return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.TEXT_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.TINYTEXT_LENGTH):         return this.dbi.DATA_TYPES.MYSQL_TEXT_TYPE
            default:                                                     return this.dbi.DATA_TYPES.MYSQL_TINYTEXT_TYPE
          }

        case this.dbi.DATA_TYPES.BINARY_TYPE:
          switch (true) {
            case (isNaN(length)):                                        return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length === undefined):                                 return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length === -1):                                        return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length > this.dbi.DATA_TYPES.MEDIUMBLOB_LENGTH):       return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length > this.dbi.DATA_TYPES.BLOB_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMBLOB_TYPE
            case (length > this.dbi.DATA_TYPES.VARBINARY_LENGTH):        return this.dbi.DATA_TYPES.MYSQL_BLOB_TYPE
            case (length > this.dbi.DATA_TYPES.BINARY_LENGTH):           return this.dbi.DATA_TYPES.MYSQL_VARBINARY_TYPE
            default:                                                     return mappedDataType
          }

        case this.dbi.DATA_TYPES.VARBINARY_TYPE:
          switch (true) {
            case (isNaN(length)):                                        return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length === undefined):                                 return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length === -1):                                        return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length > this.dbi.DATA_TYPES.MEDIUMBLOB_LENGTH):       return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length > this.dbi.DATA_TYPES.BLOB_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMBLOB_TYPE
            case (length > this.dbi.DATA_TYPES.VARBINARY_LENGTH):        return this.dbi.DATA_TYPES.MYSQL_BLOB_TYPE
            default:                                                     return mappedDataType
          }

		  
        case this.dbi.DATA_TYPES.BLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_MEDIUMBLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_BLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_BLOB_TYPE:
          switch (true) {
            case (isNaN(length)):                                        return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length === undefined):                                 return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length === -1):                                        return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length > this.dbi.DATA_TYPES.MEDIUMBLOB_LENGTH):       return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (length > this.dbi.DATA_TYPES.BLOB_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMBLOB_TYPE
            case (length > this.dbi.DATA_TYPES.TINYBLOB_LENGTH):         return this.dbi.DATA_TYPES.MYSQL_BLOB_TYPE
            default:                                                     return this.dbi.DATA_TYPES.MYSQL_TINYBLOB_TYPE
          }
		  
        case this.dbi.DATA_TYPES.NUMERIC_TYPE:        
          switch (true) {
            case (isNaN(length)):                                        
            case (length === undefined):                                 return this.dbi.DATA_TYPES.UNBOUNDED_NUMERIC_TYPE
			default:                                                     return mappedDataType
		  }
		  
        case this.dbi.DATA_TYPES.BIT_STRING_TYPE:
        case this.dbi.DATA_TYPES.VARBIT_STRIN_TYPE:
          switch (true) {
            case (isNaN(length)):                                        return this.dbi.DATA_TYPES.C_LARGEST_VARCHAR_TYPE
            case (length === undefined):                                 return this.dbi.DATA_TYPES.C_LARGEST_VARCHAR_TYPE
            case (length === -1):                                        return this.dbi.DATA_TYPES.C_LARGEST_VARCHAR_TYPE
            case (length > this.dbi.DATA_TYPES.MEDIUMTEXT_LENGTH):       return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.TEXT_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMTEXT_TYPE
            case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH):          return this.dbi.DATA_TYPES.MYSQL_TEXT_TYPE
            case (length > 64):                                          return this.dbi.DATA_TYPES.MYSQL_VARCHAR_TYPE
            default:                                                     return mappedDataType
          }

        case this.dbi.DATA_TYPES.TIME_TYPE:
        case this.dbi.DATA_TYPES.DATETIME_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMPTZ_TYPE:
          switch (true) {
            case (length > this.dbi.DATA_TYPES.TIMESTAMP_PRECISON):      return `${mappedDataType}(${this.dbi.DATA_TYPES.TIMESTAMP_PRECISON})`
            default:                                                     return mappedDataType
          }

        case this.dbi.DATA_TYPES.SET_TYPE:                               return 'json'
        case this.dbi.DATA_TYPES.ENUM_TYPE:                              return 'varchar(512)'
        default:                                                         return mappedDataType
      }   
     
  }

  getColumnDataType(mappedDataType,sizeConstraint) {

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
      const dataType = YadamuDataTypes.decomposeDataType(mappedDataType)
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
          dml            :  this.generateCopyStatement(this.targetSchema,tableMetadata.tableName,columnNames,setOperations)
        , partitionCount :  tableMetadata.partitionCount
        , partitionID    :  idx+1
        }
      })
    }
    else {
      return {
        dml:  this.generateCopyStatement(this.targetSchema,tableMetadata.tableName,columnNames,setOperations)
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
      const mappedDataType = tableMetadata.source ? tableMetadata.dataTypes[idx]:  this.getMappedDataType(tableMetadata.dataTypes[idx],tableMetadata.sizeConstraints[idx])
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
      ddl            :  this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,mappedDataTypes)
    , dml            :  this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,columnNames,insertOperators)
    , columnNames    :  tableMetadata.columnNames
    , sourceDataTypes:  tableMetadata.source ? tableMetadata.source.dataTypes:  tableMetadata.dataTypes
    , targetDataTypes:  mappedDataTypes
    , insertMode     :  insertMode
	, rowConstructor :  rowConstructor
    , _BATCH_SIZE    :  this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT:  this.dbi.INBOUND_SPATIAL_FORMAT
    }
    
    // Add Support for Copy based Operations
    
    if (tableMetadata.dataFile) {
      tableInfo.copy = this.generateCopyStatements(tableMetadata,mappedDataTypes) 
    }
        
    return tableInfo
  }      

}

export { MariadbStatementGenerator as default }

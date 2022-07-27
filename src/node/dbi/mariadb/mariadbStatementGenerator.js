
import path                     from 'path';

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

// Code Shared by MySQL 5.7 and MariaDB. 

class MariadbStatementGenerator extends YadamuStatementGenerator {

  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }

  refactorBySizeConstraint(sourceDataType,targetDataType,sizeConstraint) {

	  switch (targetDataType) {

        case this.dbi.DATA_TYPES.CHAR_TYPE:
          switch (true) {
            case (sizeConstraint.length === 0):                                     return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.MEDIUMTEXT_LENGTH):       return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.TEXT_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMTEXT_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.VARCHAR_LENGTH):          return this.dbi.DATA_TYPES.MYSQL_TEXT_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.CHAR_LENGTH):             return this.dbi.DATA_TYPES.VARCHAR_TYPE
            default:                                                                return targetDataType
          }
		  
		case this.dbi.DATA_TYPES.VARCHAR_TYPE:
          switch (true) {
            case (sizeConstraint.length === 0):                                     return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.MEDIUMTEXT_LENGTH):      return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.TEXT_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMTEXT_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.VARCHAR_LENGTH):          return this.dbi.DATA_TYPES.MYSQL_TEXT_TYPE
            default:                                                                return targetDataType
          }

        case this.dbi.DATA_TYPES.CLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_MEDIUMTEXT_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_TEXT_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_TINYTEXT_TYPE:
          switch (true) {
            case (sizeConstraint.length === 0):                                     return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.MEDIUMTEXT_LENGTH):       return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.TEXT_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMTEXT_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.TINYTEXT_LENGTH):         return this.dbi.DATA_TYPES.MYSQL_TEXT_TYPE
            case (sizeConstraint[0] < this.dbi.DATA_TYPES.TINYTEXT_LENGTH):         return this.dbi.DATA_TYPES.VARCHAR_TYPE
            default:                                                                return this.dbi.DATA_TYPES.MYSQL_TINYTEXT_TYPE
          }

        case this.dbi.DATA_TYPES.BINARY_TYPE:
          switch (true) {
            case (sizeConstraint.length === 0):                                     return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.MEDIUMBLOB_LENGTH):       return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.BLOB_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMBLOB_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.VARBINARY_LENGTH):        return this.dbi.DATA_TYPES.MYSQL_BLOB_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.BINARY_LENGTH):           return this.dbi.DATA_TYPES.VARBINARY_TYPE
            default:                                                                return targetDataType
          }

        case this.dbi.DATA_TYPES.VARBINARY_TYPE:
          switch (true) {
            case (sizeConstraint.length === 0):                                     return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.MEDIUMBLOB_LENGTH):       return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.BLOB_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMBLOB_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.VARBINARY_LENGTH):        return this.dbi.DATA_TYPES.MYSQL_BLOB_TYPE
            default:                                                                return targetDataType
          }

		  
        case this.dbi.DATA_TYPES.BLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_MEDIUMBLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_BLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_TINYBLOB_TYPE:
          switch (true) {
            case (sizeConstraint.length === 0):                                     return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.MEDIUMBLOB_LENGTH):       return this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.BLOB_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMBLOB_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.TINYBLOB_LENGTH):         return this.dbi.DATA_TYPES.MYSQL_BLOB_TYPE
            case (sizeConstraint[0] < this.dbi.DATA_TYPES.TINYBLOB_LENGTH):         return this.dbi.DATA_TYPES.VARBINARY_TYPE
            default:                                                                return this.dbi.DATA_TYPES.MYSQL_TINYBLOB_TYPE
          }
		  
        case this.dbi.DATA_TYPES.NUMERIC_TYPE:        
          switch (true) {
			// Need to address P,S and linits on P,S
            case (sizeConstraint.length === 0):                                     
			default:                                                                return targetDataType
		  }
		  
        case this.dbi.DATA_TYPES.BIT_STRING_TYPE:
        case this.dbi.DATA_TYPES.VARBIT_STRING_TYPE:
          switch (true) {
            case (sizeConstraint.length === 0):                                     return this.dbi.DATA_TYPES.C_LARGEST_VARCHAR_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.MEDIUMTEXT_LENGTH):       return this.dbi.DATA_TYPES.MYSQL_LONGTEXT_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.TEXT_LENGTH):             return this.dbi.DATA_TYPES.MYSQL_MEDIUMTEXT_TYPE
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.VARCHAR_LENGTH):          return this.dbi.DATA_TYPES.MYSQL_TEXT_TYPE
            case (sizeConstraint[0] > 64):                                          return this.dbi.DATA_TYPES.MYSQL_VARCHAR_TYPE
            default:                                                                return targetDataType
          }

	    // MySQL Timestamp limited to Unix EPOCH date range. Map to datetime when data comes from other sources.

        case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMPTZ_TYPE:
          switch (true) {
            case this.SOURCE_VENDOR ===  'MariaDB':
            case this.SOURCE_VENDOR === 'MySQL':                                    return targetDataType
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.TIMESTAMP_PRECISION):     return `${this.dbi.DATA_TYPES.DATETIME_TYPE}(${this.dbi.DATA_TYPES.TIMESTAMP_PRECISION})`
            default:                                                                return `${this.dbi.DATA_TYPES.DATETIME_TYPE}`
          }
     
        case this.dbi.DATA_TYPES.TIME_TYPE:
        case this.dbi.DATA_TYPES.DATETIME_TYPE:
          switch (true) {
            case (sizeConstraint[0] > this.dbi.DATA_TYPES.TIMESTAMP_PRECISION):     return `${targetDataType}(${this.dbi.DATA_TYPES.TIMESTAMP_PRECISION})`
            default:                                                                return targetDataType
          }
        default:                                                                    return targetDataType
      }   
     
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
      switch (dataType.type) {
        case this.dbi.DATA_TYPES.SPATIAL_TYPE:                
        case this.dbi.DATA_TYPES.POINT_TYPE:                
        case this.dbi.DATA_TYPES.LINE_TYPE:                 
        case this.dbi.DATA_TYPES.POLYGON_TYPE:              
        case this.dbi.DATA_TYPES.MULTI_POINT_TYPE:          
        case this.dbi.DATA_TYPES.MULTI_LINE_TYPE:           
        case this.dbi.DATA_TYPES.MULTI_POLYGON_TYPE:        
        case this.dbi.DATA_TYPES.GEOMETRY_COLLECTION_TYPE:  
          let spatialFunction
          switch (this.SPATIAL_FORMAT) {
            case "WKB":
            case "EWKB":
              spatialFunction = `ST_GeomFromWKB(UNHEX(${psuedoColumnName}))`;
              break;
            case "WKT":
            case "EWKT":
              spatialFunction = `ST_GeomFromText(${psuedoColumnName})`;
              break;
            case "GeoJSON":
              spatialFunction = `ST_GeomFromGeoJSON(${psuedoColumnName})`;
              break;
            default:
              return `ST_GeomFromWKB(UNHEX(${psuedoColumnName}))`;
          }  
          return `"${tableMetadata.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, ${spatialFunction})`
        case this.dbi.DATA_TYPES.BINARY_TYPE:
        case this.dbi.DATA_TYPES.VARBINARY_TYPE:
        case this.dbi.DATA_TYPES.BLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_TINYBLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_MEDIUMBLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_BLOB_TYPE:
        case this.dbi.DATA_TYPES.MYSQL_LONGBLOB_TYPE:
          return `"${tableMetadata.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, UNHEX(${psuedoColumnName}))`
        case this.dbi.DATA_TYPES.TIME_TYPE:
          return `"${tableMetadata.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(INSTR(${psuedoColumnName},'.') > 0,str_to_date(${psuedoColumnName},'%Y-%m-%dT%T.%f'),str_to_date(${psuedoColumnName},'%Y-%m-%dT%T')))`
        case this.dbi.DATA_TYPES.DATETIME_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
          return `"${tableMetadata.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(INSTR(${psuedoColumnName},'.') > 0,str_to_date(${psuedoColumnName},'%Y-%m-%dT%T.%f'),str_to_date(${psuedoColumnName},'%Y-%m-%dT%T')))`
        case this.dbi.DATA_TYPES.BOOLEAN_TYPE:
          return `"${tableMetadata.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(${psuedoColumnName} = 'true',1,0))`
        case this.dbi.DATA_TYPES.TINYINT_TYPE:
          switch (true) {
            case ((dataType.length === 1) && this.dbi.DATA_TYPES.storageOptions.TINYINT1_IS_BOOLEAN):
              return `"${tableMetadata.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(${psuedoColumnName} = 'true',1,0))`
          }
        default:
          return `"${tableMetadata.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, ${psuedoColumnName})`
      }
    })
   
    // Partitioned Tables need one entry per partition 

    if (tableMetadata.hasOwnProperty('partitionCount')) {
      return tableMetadata.dataFile.map((filename,idx) => {
        return {
          dml            :  this.generateCopyStatement(this.targetSchema,tableMetadata.tableName,filename,columnNames,setOperations)
        , partitionCount :  tableMetadata.partitionCount
        , partitionID    :  idx+1
        }
      })
    }
    else {
      return {
        dml:  this.generateCopyStatement(this.targetSchema,tableMetadata.tableName,tableMetadata.dataFile,columnNames,setOperations)
      }
    }  
  }

  validateRowSize(columnNames,mappedDataTypes,sizeConstraints,columnDefinitions) {

    const rowSizes = [...sizeConstraints]
	// TODO: Add correct sizes for INT, FLOAT, NUMBER etc.

    let rowSize = rowSizes.reduce((sum, cv) => { return isNaN(cv[0]) ? sum : sum + cv[0] },0)
	while (rowSize > 65535) {
      const idx = rowSizes.reduce((idx, cv, cidx ) => {return cv[0] >= rowSizes[idx] ? cidx : idx}, 0);
      rowSize = rowSize - rowSizes[idx] + 8
      switch(mappedDataTypes[idx]) { 
        case this.dbi.DATA_TYPES.VARCHAR_TYPE:
          mappedDataTypes[idx] = this.dbi.DATA_TYPES.MYSQL_TEXT_TYPE
          break
        case this.dbi.DATA_TYPES.VARBINARY_TYPE:
          mappedDataTypes[idx] = this.dbi.DATA_TYPES.MYSQL_BLOB_TYPE
          break;
        default: 
          return
      }
      columnDefinitions[idx] = `"${columnNames[idx]}" ${this.generateStorageClause(mappedDataTypes[idx],[])}`
    }
  }

  generateTableInfo(tableMetadata) {
    
    let insertMode = 'Batch';
    this.SPATIAL_FORMAT = this.getSpatialFormat(tableMetadata)
	
    const insertOperators = []

	const targetDataTypes = this.getTargetDataTypes(tableMetadata)
	const columnDataTypes = [...targetDataTypes]
	
	// this.debugStatementGenerator(null,null)

    const columnDefinitions = targetDataTypes.map((targetDataType,idx) => {	
	
      let addNullClause = false;
   	  const columnName = tableMetadata.columnNames[idx]

      const dataTypeDefinition = YadamuDataTypes.decomposeDataType(targetDataType)
	  
	  switch (dataTypeDefinition.type) {
        case this.dbi.DATA_TYPES.SPATIAL_TYPE:                
        case this.dbi.DATA_TYPES.POINT_TYPE:                
        case this.dbi.DATA_TYPES.LINE_TYPE:                 
        case this.dbi.DATA_TYPES.POLYGON_TYPE:              
        case this.dbi.DATA_TYPES.MULTI_POINT_TYPE:          
        case this.dbi.DATA_TYPES.MULTI_LINE_TYPE:           
        case this.dbi.DATA_TYPES.MULTI_POLYGON_TYPE:        
        case this.dbi.DATA_TYPES.GEOMETRY_COLLECTION_TYPE:  
          switch (this.SPATIAL_FORMAT) {
            case "WKB":
            case "EWKB":
              insertOperators.push('ST_GeomFromWKB(?)');
              break
            case "WKT":
            case "EWKT":
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
          insertOperators.push('?')
          break;
        case this.dbi.DATA_TYPES.BOOLEAN_TYPE:
          columnDataTypes[idx] = this.dbi.DATA_TYPES.storageOptions.BOOLEAN_TYPE
          insertOperators.push('?')
          break;
        case this.dbi.DATA_TYPES.XML_TYPE:
          columnDataTypes[idx] = this.dbi.DATA_TYPES.storageOptions.XML_TYPE
          insertOperators.push('?')
          break;
        case this.dbi.DATA_TYPES.MYSQL_SET_TYPE:                         
		  columnDataTypes[idx] = this.dbi.DATA_TYPES.storageOptions.SET_TYPE
          insertOperators.push('?')
          break;
        case this.dbi.DATA_TYPES.MYSQL_ENUM_TYPE:                       
		  columnDataTypes[idx] = this.dbi.DATA_TYPES.storageOptions.ENUM_TYPE
          insertOperators.push('?')
          break;		  
        default:
          insertOperators.push('?')
      }
	  return `"${columnName}" ${this.generateStorageClause(columnDataTypes[idx],tableMetadata.sizeConstraints[idx])} ${addNullClause === true ? 'null':''}`      
    })
                                   
	this.dbi.applyDataTypeMappings(tableMetadata.tableName,tableMetadata.columnNames,targetDataTypes,this.dbi.IDENTIFIER_MAPPINGS,true)
	
  	this.validateRowSize(tableMetadata.columnNames,targetDataTypes,tableMetadata.sizeConstraints,columnDefinitions)					          
    								       
    const rowConstructor = `(${insertOperators.join(',')})`

    const tableInfo = {
      ddl             : this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,targetDataTypes)
    , dml             : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,tableMetadata.columnNames,insertOperators)
    , columnNames     : tableMetadata.columnNames
    , targetDataTypes : targetDataTypes
    , insertMode      : insertMode
	, rowConstructor  : rowConstructor
    , _BATCH_SIZE     : this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT : this.SPATIAL_FORMAT
    }
    
    // Add Support for Copy based Operations
    
    if (tableMetadata.dataFile) {
      tableInfo.copy = this.generateCopyOperation(tableMetadata,targetDataTypes) 
    }
    return tableInfo
  }      

}

export { MariadbStatementGenerator as default }

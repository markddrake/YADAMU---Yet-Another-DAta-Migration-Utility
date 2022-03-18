"use strict";

import Yadamu                   from '../../core/yadamu.js';
import YadamuLibrary            from '../../lib/yadamuLibrary.js';
import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'
import DataTypes                from './mongoDataTypes.js'

class MongoStatementGenerator extends YadamuStatementGenerator {

  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }
  
  generateStorageClause(mappedDataType) {
	return mappedDataType
  }
  
  emptyTable(tableMetadata) {
	return ((tableMetadata.columnNames.length === 1) && (tableMetadata.columnNames[0] === 'JSON_DATA') && (tableMetadata.dataTypes[0] === 'json'))
  }
  
  generateTableInfo(tableMetadata) {

    let insertMode = 'DOCUMENT';
    
    let columnNames = tableMetadata.columnNames
    let dataTypes = tableMetadata.dataTypes
    let sizeConstraints = tableMetadata.sizeConstraints

    /*
    **
    ** ARRAY_TO_DOCUMENT uses the column name, data types and size constraint information from the source database to set up 
    ** the transformations required to convert the incoming array into a document. 
    **   
    */

    if (((this.dbi.WRITE_TRANSFORMATION === 'ARRAY_TO_DOCUMENT') || this.emptyTable(tableMetadata)) && (tableMetadata.source)) {
      columnNames = tableMetadata.source.columnNames 
      dataTypes = tableMetadata.source.dataTypes;
      sizeConstraints = tableMetadata.source.sizeConstraints 
    }
     
	 
    const mappedDataTypes = columnNames.map((columnName,idx) => { 
	  const mappedDataType = tableMetadata.source ? tableMetadata.source.dataTypes[idx] : this.getMappedDataType(dataTypes[idx],sizeConstraints[idx])
	  return mappedDataType
	})
   	
    if ((columnNames.length === 1) && (dataTypes[0] === 'JSON')) {
        // If the source table consists of a single JSON Column then insert each row into MongoDB 'As Is'   
        insertMode = 'DOCUMENT'
    }
    else {
      switch (this.dbi.writeTransformation) {
        case 'ARRAY_TO_DOCUMENT':
          insertMode = 'OBJECT'
          break;
        case 'PRESERVE':
          insertMode = 'ARRAY'
          break;
        default:
          insertMode = 'OBJECT'
      }
    }    
    
    const tableInfo =  { 
      ddl             : tableMetadata.tableName
    , dml             : `insertMany(${tableMetadata.tableName})`
    , columnNames     : columnNames
    , targetDataTypes : mappedDataTypes
    , sizeConstraints : sizeConstraints
    , insertMode      : insertMode
    , _BATCH_SIZE     : this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT : this.dbi.INBOUND_SPATIAL_FORMAT
    }
	
	return tableInfo
  }

}

export { MongoStatementGenerator as default }

/*
**

  assignDefaultDataType(dataType, sizeConstraint, columnName) {

      sizeConstraint = sizeConstraint || ''
      
      const precision = Number(sizeConstraint.split(',')[0])
      switch (true) {
        case (YadamuLibrary.isBooleanType(dataType,sizeConstraint)):                                         return 'boolean'
        case (YadamuLibrary.isFloatingPointType(dataType)):                                                  return "number";
        case (dataType === 'bigint'):                                                                        return 'long';
        case (YadamuLibrary.isIntegerType(dataType) && (precision > 32)):                                    return "decimal";
        case (YadamuLibrary.isIntegerType(dataType)):                                                        return "number";
        case (YadamuLibrary.isNumericType(dataType) && ((sizeConstraint === '') || (precision > 15))):       return "decimal";
        case (YadamuLibrary.isNumericType(dataType)):                                                        return 'number'
        case YadamuLibrary.isDateTimeType(dataType):                                                         return 'date'
        case YadamuLibrary.isBinaryType(dataType):                                                           return ((columnName === '_id' && precision === 12)) ? 'objectId' : 'binData'
        case YadamuLibrary.isSpatialType(dataType):                                                          return 'geometry'
        case YadamuLibrary.isJSON(dataType):                                                                 return 'object'
        default:                                                                                             return 'string'
      }           
  }  

  mapForeignDataType(vendor, columnName, dataType, sizeConstraint) {

    switch (vendor) {
      case "MongoDB":                                                                                        return dataType;
      case "Postgres":                                                                                      
        switch (dataType) {                                                                                 
          case "money":                                                                                      return "decimal";
          case 'circle':                                                                                    
		    switch(true) {                                                                                  
  		      case (this.dbi.INBOUND_CIRCLE_FORMAT === 'CIRCLE'):                                            return 'object'            
		    }                                                                                               
        }                                                                                                   
		break;                                                                                              
      case 'MSSQLSERVER':                                                                                   
        switch (dataType) {                                                                                 
          case "bigint":                                                                                     return "long";
          case "numeric":                                                                                   
          case "decimal":                                                                                   
		    switch (true) {                                                                                 
               case  ((sizeConstraint === null) || (sizeConstraint > 15)):                                   return "decimal";
			}
        }    
        break;		
      default:        
       break;	  
    }   
	return this.assignDefaultDataType(dataType, sizeConstraint, columnName)
  } 
  
**  
*/
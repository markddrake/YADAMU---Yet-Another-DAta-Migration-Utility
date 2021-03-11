"use strict";

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, yadamuLogger) {
    
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;
  }
  
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
  
  generateTableInfo(tableMetadata) {

    let insertMode = 'DOCUMENT';


    let vendor = tableMetadata.vendor
  
    let columnNames = tableMetadata.columnNames
    let dataTypes = tableMetadata.dataTypes
    let sizeConstraints = tableMetadata.sizeConstraints
    
    /*
    **
    ** ARRAY_TO_DOCUMENT uses the column name, data types and size constraint information from the source database to set up 
    ** the transformations required to convert the incoming array into a document. 
    **   
    */

    if ((this.dbi.WRITE_TRANSFORMATION === 'ARRAY_TO_DOCUMENT')  && (tableMetadata.source)) {
      vendor = tableMetadata.source.vendor;
      columnNames = tableMetadata.source.columnNames 
      dataTypes = tableMetadata.source.dataTypes;
      sizeConstraints = tableMetadata.source.sizeConstraints 
    }
    
    const targetDataTypes = columnNames.map((columnName,idx) => { return this.mapForeignDataType(vendor,columnName,dataTypes[idx],sizeConstraints[idx])})
 
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
    
    return {
       ddl             : tableMetadata.tableName,
       dml             : `insertMany(${tableMetadata.tableName})`,
       columnNames     : columnNames,
       targetDataTypes : targetDataTypes, 
       sizeConstraints : sizeConstraints,
       insertMode      : insertMode,
       _BATCH_SIZE     : this.dbi.BATCH_SIZE,
       _COMMIT_COUNT   : this.dbi.COMMIT_COUNT,
       _SPATIAL_FORMAT : this.dbi.INBOUND_SPATIAL_FORMAT
    }
  }
    
  async generateStatementCache() {
    
    const statementCache = {}
    const tables = Object.keys(this.metadata); 
    
    tables.forEach((table,idx) => {
      const tableMetadata = this.metadata[table];
      const tableInfo = this.generateTableInfo(tableMetadata);
      statementCache[tableMetadata.tableName] = tableInfo;
      return tableMetadata.tableName;
    })
	
    return statementCache;
  }  

}

module.exports = StatementGenerator;
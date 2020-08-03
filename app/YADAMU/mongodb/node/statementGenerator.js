"use strict";

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, spatialFormat) {
    
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.spatialFormat = spatialFormat
  }
  
  mapForeignDataType(vendor, columnName, dataType, sizeConstraint) {
    
      if (vendor === 'MongoDB') {
        return dataType;
      }
      
      switch (true) {
        case (YadamuLibrary.isBooleanType(dataType)):
          return 'boolean'
        case (YadamuLibrary.isNumericDataType(dataType)):
          return 'number'
        case YadamuLibrary.isDateDataType(dataType):
          return 'date'
         case YadamuLibrary.isBinaryDataType(dataType):
           return ((columnName === '_id' && sizeConstraint === '12')) ? 'objectId' : 'binData'
         case YadamuLibrary.isSpatialDataType(dataType):
           return 'geometry'
         case YadamuLibrary.isJSON(dataType):
           return 'object'
         default:
           return 'string'
      }           
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
 
    if ((tableMetadata.columnNames.length === 1) && (dataTypes[0] === 'JSON')) {
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
       _SPATIAL_FORMAT : this.spatialFormat
    }
  }
    
    
    
  async generateStatementCache(executeDDL) {
    
    const statementCache = {}
    const tables = Object.keys(this.metadata); 
    
    const collectionList = tables.map((table,idx) => {
      const tableMetadata = this.metadata[table];
      const tableInfo = this.generateTableInfo(tableMetadata);
      statementCache[tableMetadata.tableName] = tableInfo;
      return tableMetadata.tableName;
    })
    if (executeDDL === true) {
      await this.dbi.executeDDL(collectionList)
    }
    return statementCache;
  }  

}

module.exports = StatementGenerator;
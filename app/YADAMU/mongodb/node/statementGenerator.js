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
  
  mapForeignDataType(vendor, dataType, dataTypeLength, dataTypeSize) {
    
      if (vendor === 'MONGO') {
        return dataType;
      }
      
      switch (true) {
        case (YadamuLibrary.isNumericDataType(dataType)):
          return 'number'
        case YadamuLibrary.isDateDataType(dataType):
          return 'date'
         case YadamuLibrary.isBinaryDataType(dataType):
           return 'binData'
         case YadamuLibrary.isSpatialDataType(dataType):
           return 'geometry'
         case YadamuLibrary.isJSON(dataType):
           return 'object'
         default:
           return 'string'
      }           
  } 
  
  generateTableInfo(tableMetadata) {
          
    let insertMode = 'Batch';
    const columnNames = tableMetadata.columnNames
    const targetDataTypes = tableMetadata.dataTypes.map((dataType) => { return this.mapForeignDataType(tableMetadata.vendor,dataType)})
 
    if ((tableMetadata.columnNames.length === 1) && (tableMetadata.dataTypes[0] === 'JSON')) {
	    // If the source table consists of a single JSON Column then insert each row into MongoDB 'As Is'	
        insertMode = 'DOCUMENT_MODE'
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
       ddl             : `createCollection(${tableMetadata.tableName})`,
       dml             : `insertMany(${tableMetadata.tableName})`,
	   columnNames     : columnNames,
       targetDataTypes : targetDataTypes, 
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
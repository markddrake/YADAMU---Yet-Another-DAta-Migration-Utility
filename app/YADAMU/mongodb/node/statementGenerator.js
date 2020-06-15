"use strict";

const Yadamu = require('../../common/yadamu.js');

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, spatialFormat, batchSize, commitSize) {
    
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.spatialFormat = spatialFormat
    this.batchSize = batchSize
    this.commitSize = commitSize;
  }
  
  async generateStatementCache (executeDDL, vendor) {    
     
    const statementCache = {}
	const collectionList = []
    const tableList = Object.keys(this.metadata); 
	tableList.forEach((table,idx) => {
      const tableMetadata = this.metadata[table]
      const tableInfo = {} 
      tableInfo.tableName = tableMetadata.tableName
      tableInfo.batchSize = this.batchSize;
      tableInfo.commitSize = this.commitSize;
      tableInfo.ddl = tableMetadata.tableName
      if (tableMetadata.source) {
		tableInfo.keys = JSON.parse('[' + tableMetadata.source.columns + "]");
        tableInfo.sourceDataTypes = tableMetadata.source.dataTypes
        tableInfo.sizeConstraints = tableMetadata.source.sizeConstraints
      }
      else {
        tableInfo.keys = JSON.parse('[' + tableMetadata.columns + "]");
        tableInfo.sourceDataTypes = tableMetadata.dataTypes
        tableInfo.sizeConstraints = tableMetadata.sizeConstraints
      }
 	  tableInfo.dataTypes = this.dbi.decomposeDataTypes(tableInfo.sourceDataTypes);

      if ((tableInfo.sourceDataTypes.length === 1) && (tableInfo.sourceDataTypes[0] === 'JSON')) {
	    // If the source table consists of a single JSON Column then insert each row into MongoDB 'As Is'	
        tableInfo.insertMode = 'DOCUMENT_MODE'
      }
      else {
	    switch (this.dbi.writeTransformation) {
  		  case 'ARRAY_TO_DOCUMENT':
		    tableInfo.insertMode = 'OBJECT'
		    break;
		  case 'PRESERVE':
		    tableInfo.insertMode = 'ARRAY'
		    break;
		  default:
		    tableInfo.insertMode = 'OBJECT'
	    }
      }    
      collectionList.push(tableInfo.tableName)
      statementCache[tableInfo.tableName] = tableInfo;

    });
   
    if (executeDDL) {
      await this.dbi.executeDDL(collectionList);
    }
    return statementCache;
  }
}

module.exports = StatementGenerator;
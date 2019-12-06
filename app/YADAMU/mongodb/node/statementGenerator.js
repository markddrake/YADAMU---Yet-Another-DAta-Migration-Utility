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
     
    let statementCache = {}
    const tables = Object.keys(this.metadata); 
    const collectionList = tables.map(function(table,idx) {
      const tableMetadata = this.metadata[table]
      const tableInfo = {} 
      tableInfo.tableName = tableMetadata.tableName
      tableInfo.batchSize = this.batchSize;
      tableInfo.commitSize = this.commitSize;
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
      tableInfo.insertMode = 'InsertMany';
      statementCache[tableInfo.tableName] = tableInfo;
      return tableInfo.tableName
    },this);
    
    if (executeDDL) {
      await this.dbi.executeDDL(collectionList);
    }
    return statementCache;
  }
}

module.exports = StatementGenerator;
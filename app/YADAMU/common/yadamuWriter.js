"use strict"

const { performance } = require('perf_hooks');

class YadamuWriter {

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    this.dbi = dbi;
    this.schema = this.dbi.parameters.TO_USER;
    this.tableName = tableName
    this.tableInfo = tableInfo;
    this.status = status;
    this.yadamuLogger = yadamuLogger;    

    this.skipTable = false;
    this.batch = [];
    this.batchCount = 0;

    this.startTime = performance.now();
    this.endTime = undefined;
    this.insertMode = 'Batch';    
    this.suppressBatchWrites = (this.tableInfo.batchSize === this.tableInfo.commitSize)
	this.sqlInitialTime = this.dbi.sqlCumlativeTime
  }

  async initialize() {
	  
     await this.dbi.beginTransaction()
  }

  batchComplete() {
    return this.batch.length === this.tableInfo.batchSize;
  }
  
  batchRowCount() {
    return this.batch.length
  }
  
  reportBatchWrites() {
    return !this.suppressBatchWrites
  }
  
  commitWork(rowCount) {
    return (rowCount % this.tableInfo.commitSize) === 0;
  }

  hasPendingRows() {
    return this.batch.length > 0;
  }
                 
  async appendRow(row) {
  }    
  
  async writeBatch() {
    return this.skipTable     
  }

  async finalize() {
	if (this.hasPendingRows()) {
      this.skipTable = await this.writeBatch();   
      if (this.skipTable) {
        await this.dbi.rollbackTransaction()
      }
    }
    if (!this.skipTable) {
      await this.dbi.commitTransaction()
    }   
    return {
      startTime    : this.startTime
    , endTime      : this.endTime
	, sqlTime      : this.dbi.sqlCumlativeTime - this.sqlInitialTime
    , insertMode   : this.insertMode
    , skipTable    : this.skipTable
    , batchCount   : this.batchCount
    }    
  }

}

module.exports = YadamuWriter;
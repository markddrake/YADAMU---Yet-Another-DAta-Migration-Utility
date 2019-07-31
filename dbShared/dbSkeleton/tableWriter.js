"use strict"

const Yadamu = require('../../common/yadamu.js');

class TableWriter {

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    this.dbi = dbi;
    this.tableName = tableName
    this.tableInfo = tableInfo;
    this.status = status;
    this.yadamuLogger = yadamuLogger;    

    this.batch = [];
    this.batchCount = 0;

    this.startTime = new Date().getTime();
    this.endTime = undefined;

    this.skipTable = false;
  }

  async initialize() {
    await this.dbi.beginTransaction();
  }

  batchComplete() {
    return (this.batch.length === this.tableInfo.batchSize)
  }
  
  commitWork(rowCount) {
    return (rowCount % this.tableInfo.commitSize) === 0;
  }

  async appendRow(row) {

  }

  hasPendingRows() {
    return this.batch.length > 0;
  }
      
  async writeBatch() {
    this.batchCount++;
    return this.skipTable
  }

  async finalize() {
    if (this.hasPendingRows()) {
      this.skipTable = await this.writeBatch();   
    }
    await this.dbi.commitTransaction();
    return {
      startTime    : this.startTime
    , endTime      : this.endTime
    , insertMode   : this.insertMode
    , skipTable    : this.skipTable
    ,batchCount    : this.batchCount;
    }    
  }

}

module.exports = TableWriter;
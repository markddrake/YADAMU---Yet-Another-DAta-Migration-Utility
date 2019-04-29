"use strict"

const Yadamu = require('../../common/yadamu.js');

class TableWriter {

  constructor(dbi,tableName,tableInfo,status,logWriter) {
    this.dbi = dbi;
    this.tableName = tableName
    this.tableInfo = tableInfo;
    this.status = status;
    this.logWriter = logWriter;    

    this.batch = [];

    this.startTime = new Date().getTime();
    this.endTime = undefined;

    this.skipTable = false;

    this.logDDLIssues   = (this.status.loglevel && (this.status.loglevel > 2));
    // this.logDDLIssues   = true;
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
    }    
  }

}

module.exports = TableWriter;
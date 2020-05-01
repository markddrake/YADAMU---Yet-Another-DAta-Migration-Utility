"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');

class TableWriter {

  constructor(tableName,outputStream) {
    this.tableName = tableName
    this.outputStream = outputStream
    this.firstRow = true;
    this.startTime = performance.now();
	this.currentTable = {}
	this.rowsCommitted = 0; 
  }

  async initialize() {
    this.outputStream.write(`"${this.tableName}":[`);
  }

  batchComplete() {
    return false
  }
  
  commitWork(rowCount) {
    return false;
  }

  async appendRow(row) {
      
    if (this.firstRow === true) {
      this.firstRow = false
    }
    else {
      this.outputStream.write(',');
    }
    
    this.outputStream.write(row);
	this.rowsCommitted++;
  }

  async writeBatch() {
  }

  async commitTransaction() {
  }

  async rollbackTransaction() {
  }

  getStatistics() {
    return {
      startTime     : this.startTime
    , endTime       : performance.now()
    , insertMode    : 'JSON'
    , skipTable     : false
	, rowsLost      : 0
	, rowsSkipped   : 0
	, rowsCommitted : this.rowsCommitted    }    
  }
  
  async finalize() {
    this.outputStream.write(`]`);
  }

}

module.exports = TableWriter;
"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');

class TableWriter {

  constructor(tableName,outputStream) {
    this.tableName = tableName
    this.outputStream = outputStream
    this.firstRow = true;
    this.startTime = performance.now();
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
  }

  async writeBatch() {
  }

  async finalize() {
    this.outputStream.write(`]`);
    return {
      startTime    : this.startTime
    , endTime      : performance.now()
    , insertMode   : 'JSON'
    , skipTable    : false
    }    
  }

}

module.exports = TableWriter;
"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {

  // Simulate subclass of YadamuWriter. Actaully extending YadamuWriter is problametic. ???

  constructor(dbi,tableName,tableInfo,status,yadamuLogger,outputStream) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
    this.insertMode = 'JSON';    
    this.outputStream = outputStream
    this.firstRow = true;
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
	this.rowCounters.committed++;
  }

  async writeBatch() {
  }

  async commitTransaction() {
  }

  async rollbackTransaction() {
  }

  async finalize() {
    this.outputStream.write(`]`);
  }

}

module.exports = TableWriter;
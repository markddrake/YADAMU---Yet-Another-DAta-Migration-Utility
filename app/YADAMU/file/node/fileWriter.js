"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class FileWriter extends YadamuWriter {

  // Simulate subclass of YadamuWriter. Actaully extending YadamuWriter is problametic. ???

  constructor(dbi,primary,status,yadamuLogger,outputStream) {
    super({objectMode: true},dbi,primary,status,yadamuLogger)
    this.outputStream = outputStream
	this.tableSeperator = ''
  }

  setTableInfo(tableInfo) {
	super.setTableInfo(tableInfo)
	let tableName = this.tableInfo.tableName
	this.insertMode = 'JSON';    
    if (this.dbi.tableMappings && this.dbi.tableMappings.hasOwnProperty(tableName)) {
	  tableName = this.dbi.tableMappings[tableName].tableName
	}
    this.outputStream.write(`${this.tableSeperator}"${tableName}":[`);
	this.tableSeperator = ',';
	this.rowSeperator = '';
  }

  async initialize() {
  }
  
  batchComplete() {
    return false
  }
  
  commitWork(rowCount) {
    return false;
  }

  cacheRow(row) {
      
    this.outputStream.write(`${this.rowSeperator}${row}`);
	this.rowSeperator = ','
    this.rowCounters.committed++;
  }

  async writeBatch() {
  }
  
  async flushCache(readerStatistics) {
	this.outputStream.write(']');
	super.flushCache(readerStatistics);
  }

  async commitTransaction() {
  }

  async rollbackTransaction() {
  }

  async finalize() {
    this.outputStream.write(`]`);
  }

}

module.exports = FileWriter;
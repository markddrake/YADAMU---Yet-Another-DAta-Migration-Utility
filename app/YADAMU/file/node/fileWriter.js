"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class FileWriter extends YadamuWriter {

  // Simulate subclass of YadamuWriter. Actaully extending YadamuWriter is problametic. ???

  constructor(dbi,tableName,status,yadamuLogger,firstTable,outputStream) {
    super({objectMode: true},dbi,tableName,status,yadamuLogger)
    this.tableName = tableName  
    this.outputStream = outputStream	
    this.tableInfo = this.dbi.getTableInfo(tableName)
	this.setTableInfo(tableName)
	
	this.insertMode = 'JSON';    
    if (this.dbi.tableMappings && this.dbi.tableMappings.hasOwnProperty(tableName)) {
	  tableName = this.dbi.tableMappings[tableName].tableName
	}
    const tableSeperator = firstTable ? '' :  ','
	this.outputStream.write(`${tableSeperator}"${tableName}":[`);
	this.tableSeperator = ',';
	this.rowSeperator = '';
  }

  setTableInfo(tableInfo) {
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
      
    this.outputStream.write(`${this.rowSeperator}${JSON.stringify(row)}`);
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
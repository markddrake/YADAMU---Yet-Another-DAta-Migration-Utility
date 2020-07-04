"use strict"
const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');

const YadamuWriter = require('../../../YADAMU/common/yadamuWriter.js');
const YadamuLogger = require('../../../YADAMU/common/yadamuLogger.js');

class StatisticsCollector extends YadamuWriter {
  
  constructor(dbi,yadamuLogger) {
	const nulLogger = new YadamuLogger(fs.createWriteStream("\\\\.\\NUL"),{});
	// nulLogger is used to supress row counting. ### Use of the nulLogger means supresses error reporting as well as row counting
    super({objectMode: true},dbi,null,{},nulLogger)  
	this.nulLogger = nulLogger
	this.tableInfo = {}
  }
  
  newTable(tableName) {
    
	this.tableName = tableName;    
    this.tableInfo[tableName] = {
      rowCount  : 0
     ,byteCount : 2
     ,hash      : null
    }    
    this.skipTable = false
  }

  batchComplete() {
    return false
  }
  
  async checkColumnCount(row){
  }
  
  cacheRow(row) { 
    this.tableInfo[this.tableName].rowCount++;
    this.tableInfo[this.tableName].byteCount+= JSON.stringify(row).length;    
	return false;
  }

  async writeBatch() {
    if (this.tableInfo[this.tableName].rowCount > 1) {
      this.tableInfo[this.tableName].byteCount += this.tableInfo[this.tableName].rowCount - 1;
    }
  }  
  
  getStatististics() {
	return this.tableInfo
  }
  
   async flushCache() {
	this.nulLogger.close();
  }
  
}

module.exports = StatisticsCollector;
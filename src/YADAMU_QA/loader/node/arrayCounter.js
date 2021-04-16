"use strict"
const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');

const YadamuWriter = require('../../../YADAMU/common/yadamuWriter.js');
const YadamuLogger = require('../../../YADAMU/common/yadamuLogger.js');

class ArrayCounter extends YadamuWriter {
  
  constructor(dbi) {
	const ddlComplete = true
    super({objectMode: true},dbi,true,null,{},YadamuLogger.NULL_LOGGER)  
	this.rowCount = 0;
  }
  
  setTableInfo(tableName) {
	 this.tableInfo = {}
  }
    

  batchComplete() {
    return false
  }
  
  async checkColumnCount(row){ /* OVERRIDE */ }
  
  getRowCount() {
	return this.rowCount;
  }
  
  async processRow()  { 
    this.rowCount++;
  }


  async writeBatch() { /* OVERRIDE */ }
    
  reportPerformance() { /* OVERRIDE */ }
  
}

module.exports = ArrayCounter
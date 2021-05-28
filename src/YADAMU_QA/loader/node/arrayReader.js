"use strict"
const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');

const YadamuWriter = require('../../../YADAMU/common/yadamuWriter.js');
const YadamuLogger = require('../../../YADAMU/common/yadamuLogger.js');

class ArrayReader extends YadamuWriter {
  
  constructor(dbi) {
	const ddlComplete = true
    super({objectMode: true},dbi,true,null,{},YadamuLogger.NULL_LOGGER)  
	this.array = [];
  }
  
  setTableInfo(tableName) {
	 this.tableInfo = {}
  }
    
  batchComplete() {
    return false
  }
  
  async checkColumnCount(row){ /* OVERRIDE */ }
  
  getArray() {
	return this.array
  }
  
  async processRow(row)  { 
    this.array.push(row)
  }

  async writeBatch() { /* OVERRIDE */ }
    
  reportPerformance() { /* OVERRIDE */ }
  
}

module.exports = ArrayReader
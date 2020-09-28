"use strict"

const { performance } = require('perf_hooks');

const JSONWriter = require('./jsonWriter.js');

class ErrorWriter extends JSONWriter {

  constructor(dbi,yadamuLogger) {
    super(dbi,undefined,undefined,0,{},yadamuLogger)
	this.firstTable = true
  }
 
  async initialize(tableName) {
	this.startTable = this.firstTable ? `"${tableName}":[` :  `],"${tableName}":[`
	this.firstTable = false;
    this.rowSeperator = '';
	await super.initialize(tableName);
  }
  
  //   Disable the columnCountCheck when writing an error report
  
  checkColumnCount() { /* OVERRRIDE */ }
  
  reportPerformance() { /* OVERRRIDE */ }
      
}

module.exports = ErrorWriter;
"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const JSONWriter = require('./jsonWriter.js');

class ErrorWriter extends JSONWriter {

  /* Supress the table name from the file */

  constructor(dbi,tableName,ddlComplete,tableIdx,status,yadamuLogger) {
    super(dbi,tableName,ddlComplete,tableIdx,status,yadamuLogger)
  }

  //   Disable the columnCountCheck when writing an error report
  checkColumnCount() { /* OVERRRIDE */ }
  
  reportPerformance() { /* OVERRRIDE */ }
  
}

module.exports = ErrorWriter;
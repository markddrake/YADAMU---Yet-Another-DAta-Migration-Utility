"use strict"

const { performance } = require('perf_hooks');

const JSONWriter = require('./jsonWriter.js');

class CSVWriter extends JSONWriter {

  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super(dbi,tableName,ddlComplete,status,yadamuLogger)
  }
   
  beginTable() { /* OVERRIDE */ }
  
  formatRow(row) {
    let nextLine = JSON.stringify(row) 
    nextLine = nextLine.substring(1,nextLine.length-1) + "\r\n" 
    return nextLine;
  }

  async endTable() { /* OVERRIDE */ }

}

module.exports = CSVWriter;
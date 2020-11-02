"use strict"

const { performance } = require('perf_hooks');

const JSONWriter = require('./jsonWriter.js');

class ArrayWriter extends JSONWriter {

  
  // Write each row as a JSON array without a surrounding Array and without a comma seperating the rows. 
  // Each array is on a seperate line
  
  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super(dbi,tableName,ddlComplete,status,yadamuLogger)
  }
   
  beginTable() { /* OVERRIDE */ }
 
  formatRow(row) {
    return `${JSON.stringify(row)}\r\n`
  }
 
  async endTable() { /* OVERRIDE */ }

}

module.exports = ArrayWriter;
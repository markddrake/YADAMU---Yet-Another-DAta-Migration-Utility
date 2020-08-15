"use strict"

const { performance } = require('perf_hooks');

const FileWriter = require('../../File/node/fileWriter.js');

class CSVWriter extends FileWriter {

  
  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super(dbi,tableName,ddlComplete,status,yadamuLogger)
  }
   
  async startOuterArray() {
  }
  
  cacheRow(row) {
	  
	// if (this.rowCounters.received === 1) console.log(row)
    this.rowTransformation(row)
    let nextLine = JSON.stringify(row) 
    nextLine = nextLine.substring(1,nextLine.length-1) + "\r\n" 
    this.outputStream.write(nextLine);
    this.rowCounters.committed++;
  }

  async endOuterArray() {
  }

}

module.exports = CSVWriter;
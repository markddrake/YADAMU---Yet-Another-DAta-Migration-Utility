"use strict" 

const FileDBI = require('../../../YADAMU/file/node/fileDBI.js');

class FileStatistics extends FileDBI {
    
  constructor(yadamu) {
	 super(yadamu)
     this.tableInfo = {}
	 this.skipTable = false;
     this.tableName = undefined;
     this.parameters = {}
  }
  
  async initialize() {}

  async initializeImport() {}

  setSystemInformation(systemInformation) {}
  
  setMetadata(metadata) {}

  async initializeData() {}  

  async finalizeData() {}
  
  async finalizeImport() {}

  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
  
  async initializeDataLoad(databaseVendor) {     
    this.tableInfo = {}
  }

  getTableWriter(tableName) {
	  
	// This class doubles up as it's TableWriter... Need to fake methods provided by TableWriter
	  
    this.skipTable = false

    this.tableInfo[tableName] = {
      rowCount  : 0
     ,byteCount : 2
     ,hash      : null
    }    
    this.tableName = tableName;
    return this;
  }
  
  batchComplete() {
    return false
  }
  
  commitWork(rowCount) {
    return false;
  }

  async appendRow(row) { 

    this.tableInfo[this.tableName].rowCount++;
    this.tableInfo[this.tableName].byteCount+= JSON.stringify(row).length;    
  }

  async writeBatch() {
    if (this.tableInfo[this.tableName].rowCount > 1) {
      this.tableInfo[this.tableName].byteCount += this.tableInfo[this.tableName].rowCount - 1;
    }
  }    

}

module.exports = FileStatistics
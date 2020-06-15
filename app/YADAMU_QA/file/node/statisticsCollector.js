"use strict"
const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');

const DBWriter = require('../../../YADAMU/common/dbWriter.js');
const YadamuWriter = require('../../../YADAMU/common/yadamuWriter.js');
const YadamuLogger = require('../../../YADAMU/common/yadamuLogger.js');

class StatisticsCollector extends YadamuWriter {

  /*
  **
  ** Optimization of LOB Usage
  **
  ** Since using LOB causes a 50%+  reduction in throughput only use LOBS where necessary.
  ** The oracle node driver (oracledb) allows strings and buffers to be bound to CLOBS and BLOBS
  ** This requires the LOB to be buffered in the client until it is written to the database
  ** You cannot insert a mixture of rows contains LOBs and rows containing Strings and Buffers using executeMany as the bind specification must explicitly state what is being bound.
  **
  ** Binding LOBS is slower than binding Strings and Buffers
  ** Binding LOBS requires less client side memory than binding Strings and Buffers
  **
  ** The Yadamu Oracle interface allows you to optimize LOB usage via the following parameters
  **    LOB_BATCH_COUNT : A Batch will be regarded as complete when it uses more LOBS than LOB_BATCH_COUNT
  **    LOB_MIN_SIZE    : If a String or Buffer is mapped to a CLOB or a BLOB then it will be inserted using a LOB if it exceeeds this value.
  **    LOB_CACHE_COUNT  : A Batch will be regarded as complete when the number of CACHED (String & Buffer) LOBs exceeds this value.
  **
  ** The amount of client side memory required to manage the LOB Cache is approx LOB_MIN_SIZE * LOB_CACHE_COUNT
  **
  */
  
  constructor(dbi,yadamuLogger) {
    const nulLogger = new YadamuLogger(fs.createWriteStream("\\\\.\\NUL"),{});
	super({objectMode: true},dbi,new DBWriter(dbi,'FILE_COMPARE',{},nulLogger,{}),{},nulLogger)  
	this.tableInfo = {}
  }
  
  setTableInfo(tableName) {
    
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
  
}

module.exports = StatisticsCollector;
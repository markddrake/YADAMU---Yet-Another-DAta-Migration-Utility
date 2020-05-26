"use strict"

const { performance } = require('perf_hooks');

class YadamuWriter {


  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    this.dbi = dbi;
    this.schema = this.dbi.parameters.TO_USER;
    this.tableName = tableName
    this.tableInfo = tableInfo
    this.status = status;
    this.yadamuLogger = yadamuLogger;    
    this.rejectManager = this.dbi.yadamu.rejectManager
	
    this.skipTable = false;
    this.batch = [];
	
    this.batchCount = 0;     // Batches created
	
	this.rowCounters = {
	  cached    : 0 // Rows recieved and cached by appendRow(). Reset every time a batch of cached rows is written to disk
	, written   : 0 // Rows written to disk in the current transaction
	, committed : 0 // Rows successfully committed to disk
	, skipped   : 0 // Rows not written to disk due to unrecoverable write errors
	, lost      : 0 // Rows written to disk and thene lost as a result of a rollback or lost connnection 
	}
	
	this.dbi.trackCounters(this.rowCounters)
	
    this.startTime = performance.now();
    this.insertMode = 'Batch';    
    this.supressBatchWriteLogging = (this.tableInfo.batchSize === this.tableInfo.commitSize) // Prevent duplicate logging if batchSize and Commit SIze are the same
	this.sqlInitialTime = this.dbi.sqlCumlativeTime

	dbi.currentTable = this;
		
  }

  async initialize() {  
     await this.dbi.beginTransaction()
  }
 
  batchComplete() {
    return ((this.rowCounters.cached === this.tableInfo.batchSize) && !this.skipTable)
  }
  
  batchRowCount() {
    return this.rowCounters.cached
  }
  
  reportBatchWrites() {
    return !this.supressBatchWriteLogging
  }
  
  commitWork() {
    return (this.rowCounters.written >= this.tableInfo.commitSize);
  }

  hasPendingRows() {
    return this.rowCounters.cached > 0;
  }
                 
  async appendRow(row) {

	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
	  
	this.transformations.forEach(function (transformation,idx) {
      if ((transformation !== null) && (row[idx] !== null)) {
	    row[idx] = transformation(row[idx])
      }
	},this)
	
    this.batch.push(row);
	
	this.rowCounters.cached++
	return this.skipTable;
  }  
  
  async writeBatch() {
	this.rowCounters.written += this.rowCounters.cached;
	this.rowCounters.cached = 0;
    return this.skipTable     
  }
  
  async commitTransaction() {
    if (!this.skipTable) {
      await this.dbi.commitTransaction()
	  this.rowCounters.committed += this.rowCounters.written;
	  this.rowCounters.written = 0;
	}
  }
  
  async rollbackTransaction(cause) {
      this.rowCounters.lost += this.rowCounters.written;
	  this.rowCounters.written = 0;
  	  await this.dbi.rollbackTransaction(cause)
  }

  rejectRow(tableName,row) {
    // Allows the rejection process to be overridden by a particular driver.
    this.rejectManager.rejectRow(tableName,row);
  }
  
  async handleInsertError(currentOperation,batchSize,row,record,err,info) {
    this.rowCounters.skipped++;
    this.rejectRow(this.tableName,record);
    this.yadamuLogger.logRejected([this.dbi.DATABASE_VENDOR,this.tableName,currentOperation,batchSize,row],err);
	/*
    info.forEach(function (info) {
      this.yadamuLogger.writeDirect(`${info}\n`);
    },this)
    */
    const abort = (this.rowCounters.skipped === ( this.dbi.parameters.MAX_ERRORS ? this.dbi.parameters.MAX_ERRORS : 10)) 
    if (abort) {
      this.yadamuLogger.error([this.dbi.DATABASE_VENDOR,this.tableName,this.insertMode],`Maximum Error Count exceeded. Skipping Table.`);
    }
    return abort;     
  }
  
  getStatistics() {
	return {
      startTime     : this.startTime
    , endTime       : performance.now()
	, sqlTime       : this.dbi.sqlCumlativeTime - this.sqlInitialTime
    , insertMode    : this.insertMode
    , skipTable     : this.skipTable
	, counters      : this.rowCounters
    }    
  }

  async finalize() {
 	if (this.dbi.transactionInProgress === true) {
	  if (this.hasPendingRows()) {
        this.skipTable = await this.writeBatch();   
      }
      if (this.skipTable === true) {
        await this.rollbackTransaction()
      }
      else {
        await this.commitTransaction()
      }
    }
	this.dbi.currentTable = undefined;
    return !this.skipTable
  }

}

module.exports = YadamuWriter;
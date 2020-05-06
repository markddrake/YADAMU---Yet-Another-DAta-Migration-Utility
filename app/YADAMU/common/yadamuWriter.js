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
	this.rowsCached = 0;     // Rows recieved and cached by appendRow(). Reset every time a batch of cached rows is written to disk
	this.rowsWritten = 0;    // Rows written to disk in the current transaction
	this.rowsCommitted = 0;  // Rows successfully committed to disk
	this.rowsSkipped = 0;    // Rows not written to disk due to unrecoverable write errors
	this.rowsLost = 0;       // Rows written to disk and thene lost as a result of a rollback or lost connnection 
	
    this.startTime = performance.now();
    this.endTime = undefined;
    this.insertMode = 'Batch';    
    this.supressBatchWriteLogging = (this.tableInfo.batchSize === this.tableInfo.commitSize) // Prevent duplicate logging if batchSize and Commit SIze are the same
	this.sqlInitialTime = this.dbi.sqlCumlativeTime

	dbi.currentTable = this;
		
  }

  async initialize() {
	  
     await this.dbi.beginTransaction()
  }


  lostConnection() {
   
    /*
    **
    ** Invoked by the DBI when the connection is lost. Assume a rollback took place. All rows written but no committed are lost. 
    ** In theory this cuuld be taken care of by invoking the current table's rollback method, rather than the DBI's rollback but 
    ** if the rollback fails and processing continues the state of the counters could be indeterminate.
    **
    */
	
	this.rowsLost = this.rowsWritten;
	this.rowsWritten = 0;
  }	  
	 

  batchComplete() {
    return this.rowsCached === this.tableInfo.batchSize;
  }
  
  batchRowCount() {
    return this.rowsCached
  }
  
  reportBatchWrites() {
    return !this.supressBatchWriteLogging
  }
  
  commitWork() {
    return (this.rowsWritten >= this.tableInfo.commitSize);
  }

  hasPendingRows() {
    return this.rowsCached > 0;
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
	
	this.rowsCached++
	return this.skipTable;
  }  
  
  async writeBatch() {
	this.rowsWritten += this.rowsCached;
	this.rowsCached = 0;
    return this.skipTable     
  }
  
  async commitTransaction() {
	await this.dbi.commitTransaction()
	this.rowsCommitted += this.rowsWritten;
	this.rowsWritten = 0;
  }
  
  async rollbackTransaction() {
	this.rowsLost += this.rowsWritten;
	this.rowsWritten = 0;
	await this.dbi.rollbackTransaction()
  }

  rejectRow(tableName,row) {
    // Allows the rejection process to be overridden by a particular driver.
    this.rejectManager.rejectRow(tableName,row);
  }
  
  async handleInsertError(operation,tableName,batchSize,row,record,err,info) {
    this.rowsSkipped++;
    this.status.warningRaised = true;
    this.yadamuLogger.logRejected([`${operation}`,`"${tableName}"`,`${batchSize}`,`${row}`],err);
    this.rejectRow(tableName,record);
    info.forEach(function (info) {
      this.yadamuLogger.writeDirect(`${info}\n`);
    },this)

    const abort = (this.rowsSkipped === ( this.dbi.parameters.MAX_ERRORS ? this.dbi.parameters.MAX_ERRORS : 10)) 
    if (abort) {
      this.yadamuLogger.error([`${operation}`,`"${tableName}"`],`Maximum Error Count exceeded. Skipping Table.`);
    }
    return abort;     
  }
  
  getStatistics() {
	return {
      startTime     : this.startTime
    , endTime       : this.endTime
	, sqlTime       : this.dbi.sqlCumlativeTime - this.sqlInitialTime
    , insertMode    : this.insertMode
    , skipTable     : this.skipTable
	, rowsLost      : this.rowsLost
	, rowsSkipped   : this.rowsSkipped
	, rowsCommitted : this.rowsCommitted
    }    
  }

  async finalize() {
	if (this.hasPendingRows()) {
      this.skipTable = await this.writeBatch();   
      if (this.skipTable) {
        await this.rollbackTransaction()
      }
    }
    if (!this.skipTable) {
      await this.commitTransaction()
    }
	this.dbi.currentTable = undefined;
    return !this.skipTable
  }

}

module.exports = YadamuWriter;
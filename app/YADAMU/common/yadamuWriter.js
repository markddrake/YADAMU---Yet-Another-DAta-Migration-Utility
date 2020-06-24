"use strict"

const Writable = require('stream').Writable
const { performance } = require('perf_hooks');

const YadamuLibrary = require('./yadamuLibrary.js');

class YadamuWriter extends Writable {

  constructor(options,dbi,tableName,status,yadamuLogger) {
	super(options)
    this.dbi = dbi;
    this.schema = this.dbi.parameters.TO_USER;
    this.status = status;
    this.yadamuLogger = yadamuLogger;    
	this.maxErrors =  this.dbi.parameters.MAX_ERRORS ? this.dbi.parameters.MAX_ERRORS : 10
    this.rejectManager = this.dbi.yadamu.rejectManager
    this.configureFeedback(this.dbi.parameters.FEEDBACK); 	
	
	this.tableName = tableName  
    this.tableInfo = this.dbi.getTableInfo(tableName)
	this.supressBatchWriteLogging = (this.tableInfo.batchSize === this.tableInfo.commitSize) // Prevent duplicate logging if batchSize and Commit SIze are the same
	this.setTableInfo(tableName);
 	
 	this.rowCounters = {
      received   : 0 // Rows accepted
	, batchCount : 0 // Batches created
	, cached     : 0 // Rows recieved and cached by appendRow(). Reset every time a batch of cached rows is written to disk
	, written    : 0 // Rows written to disk in the current transaction
	, committed  : 0 // Rows successfully committed to disk
	, skipped    : 0 // Rows not written to disk due to unrecoverable write errors
	, lost       : 0 // Rows written to disk and thene lost as a result of a rollback or lost connnection 
	}
	this.dbi.setCounters(this.rowCounters)
	
    this.batch = [];
    this.insertMode = 'Batch';    
    this.skipTable = this.dbi.parameters.MODE === 'DDL_ONLY';
    this.sqlInitialTime = this.dbi.sqlCumlativeTime
	this.startTime = performance.now();
  }
  
  setTableInfo(tableName) {
	// In-Lined in constructor
  }
   
  newTable(tableName) { 
    // Used by statisticsCollector
  }
   
  async initialize() {  
     await this.beginTransaction()
  }
  
  abortWriter() {
	this.skipTable = true;
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
                 
  cacheRow(row) {

    // Apply transformations and cache transformed row.
	
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.

    // this.yadamuLogger.trace([this.constructor.name,'YADAMU WRITER',this.rowCounters.cached],'cacheRow()')
	  
	this.transformations.forEach((transformation,idx) => {
      if ((transformation !== null) && (row[idx] !== null)) {
	    row[idx] = transformation(row[idx])
      }
	})
	
    this.batch.push(row);
	
	this.rowCounters.cached++
	return this.skipTable;
  }  
  
  async writeBatch() {
		
    if (this.tableInfo.insertMode === 'Batch') {
      try {    
        await this.dbi.createSavePoint();
        const results = await this.dbi.executeSQL(this.tableInfo.dml,this.batch);
        this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
		this.batch.length = 0;  
        this.rowCounters.written += this.rowCounters.cached;
		this.rowCounters.cached = 0;
        return this.skipTable
      } catch (e) {
        await this.dbi.restoreSavePoint(e);
		this.handleBatchException(e,'Batch Insert')
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,this.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative' 
        
      }
    }

    for (const row in batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,batch[row]);
   	    this.rowCounters.written++
      } catch (e) {
        const errInfo = [this.tableInfo.dml]
        await this.handleInsertError(`INSERT ONE`,this.rowCounters.cached,row,batch[row],e,errInfo);
        if (this.skipTable) {
          break;
        }
      }
    }     
   
    this.endTime = performance.now();
    this.batch.length = 0;
    this.rowCounters.cached = 0;
    return this.skipTable          
  }
  
  async beginTransaction() {
	await this.dbi.beginTransaction();
  }
  
  async commitTransaction() {
    if (!this.skipTable) {
      await this.dbi.commitTransaction()
	  this.rowCounters.committed += this.rowCounters.written;
	  this.rowCounters.written = 0;
	}
  }
  
  async rollbackTransaction(cause) {
      // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber()],'rollbackTransaction()')
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
    this.rejectRow(this.tableInfo.tableName,record);
    this.yadamuLogger.logRejected([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,currentOperation,batchSize,row],err);

    if (this.rowCounters.skipped === this.maxErrors) {
      this.yadamuLogger.error([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,this.insertMode],`Maximum Error Count exceeded. Skipping Table.`);
      this.skipTable = true;
    }
  }
  
  getStatistics() {
	return {
      startTime     : this.startTime
    , endTime       : this.endTime
	, sqlTime       : this.dbi.sqlCumlativeTime - this.sqlInitialTime
    , insertMode    : this.insertMode
    , skipTable     : this.skipTable
	, counters      : this.rowCounters
    }    
  }

  async finalize() {
	this.dbi.currentTable = undefined;
    return !this.skipTable
  }

  configureFeedback(feedbackModel) {
      
    this.reportCommits      = false;
    this.reportBatchWrites  = false;
    this.feedbackCounter    = 0;
    
    if (feedbackModel !== undefined) {
        
      if (feedbackModel === 'COMMIT') {
        this.reportCommits = true;
        return;
      }
  
      if (feedbackModel === 'BATCH') {
        this.reportCommits = true;
        this.reportBatchWrites = true;
        return;
      }
    
      if (!isNaN(feedbackModel)) {
        this.reportCommits = true;
        this.reportBatchWrites = true;
        this.feedbackInterval = parseInt(feedbackModel)
      }
    }      
  }
  
  async processRow(data) {
    // console.log(new Date().toISOString(),`${this.constructor.name}._write`,action,this.skipTable);
  	this.cacheRow(data)
    this.rowCounters.received++;
    if ((this.rowCounters.received % this.feedbackInterval === 0) & !this.batchComplete()) {
      this.yadamuLogger.info([`${this.tableInfo.tableName}`,this.insertMode],`Rows buffered: ${this.batchRowCount()}.`);
    }
    if (this.batchComplete()) {
      await this.writeBatch(this.status);
	  if (this.skipTable) {
        await this.rollbackTransaction();
      }
      if (this.reportBatchWrites && this.reportBatchWrites() && !this.commitWork(this.rowCounters.received)) {
        this.yadamuLogger.info([`${this.tableInfo.tableName}`,this.insertMode],`Rows written:  ${this.rowCount}.`);
      }                    
	}  
    if (this.commitWork(this.rowCounters.received)) {
      await this.commitTransaction(this.rowCounters.received)
      if (this.reportCommits) {
        this.yadamuLogger.info([`${this.tableInfo.tableName}`,this.insertMode],`Rows commited: ${this.rowCounters.received}.`);
      }          
      await this.beginTransaction();            
    }
  }

  
  async _write(obj, encoding, callback) {
	const messageType = Object.keys(obj)[0]
	try {
	  // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber(),messageType],'_write()')
      switch (messageType) {
		case 'table':
	      this.newTable(obj.table)
		  break;  
        case 'data':
          if (this.skipTable === false) {
			await this.processRow(obj.data)
          }
		  // ### Else Throw an Error to stop the reader ????
		  break;
	  case 'eod':
        // Used when processing serial data sources such as files to indicate that all records have been processed by the writer
	    // this.yadamuLogger.trace([this.constructor.name,`_write()`,this.dbi.DATABASE_VENDOR,messageType],`Emit "allDataReceived"`)  
	    this.emit('allDataReceived')
		break;
  	  default:
      }
      callback();
    } catch (e) {		
      this.yadamuLogger.handleException([`Writer`,this.dbi.DATABASE_VENDOR,this.tableName,this.dbi.parameters.ON_ERROR,this.dbi.getWorkerNumber(),messageType],e);
  	  /*
	  **
	  ** Error Handling. 
	  **   ABORT: Rollback the current transaction. Pass the error to the cllback function, which should propogate the error.
	  **   SKIP:  
	  **   FLUSH: 
	  **
	  */
	  try {
        switch (this.dbi.parameters.ON_ERROR) {
		  case undefined:
		  case 'ABORT': 
  		    await this.rollbackTransaction(e)
            callback(e);
		    break;
		  case 'SKIP':
		  case 'FLUSH':
            callback();
		    break;
		  default:
		}
      } catch (e) {
        // Passing the exception to callback triggers the onError() event
		callback(e)	
      }
    }
  }
  
  async flushCache() {
    // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.hasPendingRows(),this.rowCounters.received,this.rowCounters.committed,this.rowCounters.written,this.rowCounters.cached],'_flushCahce()')
    if (this.hasPendingRows()) {
      this.skipTable = await this.writeBatch();   
    }
    if (this.skipTable === true) {
      await this.rollbackTransaction()
    }
    else {
      await this.commitTransaction()
    }
    this.endTime = performance.now()
  }	  
  	   	  
  async _final(callback) {
    // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber()],'_final()')
	this.endTime = performance.now();
	try {
	  await this.flushCache()
      callback();
    } catch (e) {
      this.yadamuLogger.handleException([`${this.dbi.DATABASE_VENDOR}`,`"${this.currentTable}"`],e);
	  // Passing the exception to callback triggers the onError() event
      callback(e);
    } 
  } 

  async forcedEnd() {
	// Called when a writer fails. Once a writer has emitted an 'error' event calling the end() method does not appear to invoke that the _final() method.
	// this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber()],'forcedEnd()')
	try {
	  await this.flushCache();
    } catch (e) {
      this.yadamuLogger.handleException([`${this.dbi.DATABASE_VENDOR}`,`"${this.tableName}"`],e);
    } 
  }
  
  reportPerformance(readerStatistics) {
	  	  
	const writerStatistics = this.getStatistics();
	const readerElapsedTime = readerStatistics.readerEndTime - readerStatistics.pipeStartTime;
    const writerElapsedTime = writerStatistics.endTime - writerStatistics.startTime;        
	const pipeElapsedTime = writerStatistics.endTime - readerStatistics.pipeStartTime;
	const readerThroughput = isNaN(readerElapsedTime) ? 'N/A' : Math.round((readerStatistics.rowsRead/readerElapsedTime) * 1000)
    const writerThroughput = isNaN(writerElapsedTime) ? 'N/A' : Math.round((writerStatistics.counters.committed/writerElapsedTime) * 1000)
	
	let readerStatus = ''
	let rowCountSummary = ''
	
    const readerTimings = `Reader Elapsed Time: ${YadamuLibrary.stringifyDuration(readerElapsedTime)}s. Throughput ${Math.round(readerThroughput)} rows/s.`
	const writerTimings = `Writer Elapsed Time: ${YadamuLibrary.stringifyDuration(writerElapsedTime)}s. SQL Exection Time: ${YadamuLibrary.stringifyDuration(Math.round(writerStatistics.sqlTime))}s. Throughput: ${Math.round(writerThroughput)} rows/s.`
    
	if ((readerStatistics.rowsRead === 0) || (readerStatistics.rowsRead === writerStatistics.counters.committed)) {
      rowCountSummary = `Rows ${readerStatistics.rowsRead}.`
    }
	else {
      rowCountSummary = `Read ${readerStatistics.rowsRead}. Written ${writerStatistics.counters.committed}.`
    }

	rowCountSummary = writerStatistics.counters.skipped > 0 ? `${rowCountSummary} Skipped ${writerStatistics.counters.skipped}.` : rowCountSummary
   
	if (readerStatistics.copyFailed) {
	  rowCountSummary = readerStatistics.tableNotFound === true ? `Table not found.` : `Read operation failed. ${rowCountSummary} `  
      this.yadamuLogger.error([`${this.tableName}`,`${writerStatistics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
	}
	else {
	  if (readerStatistics.rowsRead !== writerStatistics.counters.committed) {
        this.yadamuLogger.error([`${this.tableName}`,`${writerStatistics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
      }
	  else {
        this.yadamuLogger.info([`${this.tableName}`,`${writerStatistics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
	  }
    }	  
	
    const timings = {[this.tableName] : {rowCount: writerStatistics.counters.committed, insertMode: writerStatistics.insertMode,  rowsSkipped: writerStatistics.counters.skipped, elapsedTime: Math.round(writerElapsedTime).toString() + "ms", throughput: Math.round(writerThroughput).toString() + "/s", sqlExecutionTime: Math.round(writerStatistics.sqlTime)}};
    return timings;
  }
  
}

module.exports = YadamuWriter;
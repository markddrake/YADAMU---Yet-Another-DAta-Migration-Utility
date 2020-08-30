"use strict"

const assert = require('assert').strict;
const Writable = require('stream').Writable
const Transform = require('stream').Transform
const { performance } = require('perf_hooks');

const YadamuLibrary = require('./yadamuLibrary.js');
const {BatchInsertError, IterativeInsertError, DatabaseError} = require('./yadamuError.js')

class YadamuWriter extends Transform {

  get BATCH_SIZE()     {  return this.tableInfo._BATCH_SIZE }
  get COMMIT_COUNT()   {  return this.tableInfo._COMMIT_COUNT }
  get SPATIAL_FORMAT() {  return this.tableInfo._SPATIAL_FORMAT}

  
  constructor(options,dbi,tableName,ddlComplete,status,yadamuLogger) {
	options.highWaterMark = 64
	super(options)
    this.dbi = dbi;
    this.schema = this.dbi.parameters.TO_USER;
    this.ddlComplete = ddlComplete;
    this.status = status;
    this.yadamuLogger = yadamuLogger;    
	this.configureFeedback(this.dbi.parameters.FEEDBACK); 	
	this.tableName = tableName  
 	this.rowCounters = {
      received   : 0 // Rows accepted
	, batchCount : 0 // Batches created
	, cached     : 0 // Rows received and cached by appendRow(). Reset every time a batch of cached rows is written to disk
	, written    : 0 // Rows written to disk in the current transaction
	, committed  : 0 // Rows successfully committed to disk
	, skipped    : 0 // Rows not written to disk due to unrecoverable write errors
	, lost       : 0 // Rows written to disk and thene lost as a result of a rollback or lost connnection 
	}
    
	this.dbi.setCounters(this.rowCounters)
	
    this.batch = [];
    this.insertMode = 'Batch';    
    this.skipTable = this.dbi.MODE === 'DDL_ONLY';
    this.sqlInitialTime = this.dbi.sqlCumlativeTime
	this.startTime = performance.now();
	
	// this.on('pipe',(src)=>{console.log('pipe',src.constructor.name)})
	// this.on('unpipe',(src)=>{console.log('unpipe',src.constructor.name)})
  }
  
  setTableInfo(tableName) {
	this.skipTable = true
	this.tableInfo = this.dbi.getTableInfo(tableName)
    this.skipTable = false;
	this.supressBatchWriteLogging = (this.BATCH_SIZE === this.COMMIT_COUNT) // Prevent duplicate logging if batchSize and Commit SIze are the same
  }
   
  async initialize(tableName) {  
    // Do not start processing table until all DDL operations have completed.
	await this.ddlComplete;
	// Workers need to reload their copy of the Statement Cache from the Manager before processing can begin
	this.dbi.reloadStatementCache()
	this.setTableInfo(tableName);
	await this.beginTransaction()
  }
  
  abortTable() {
	// Disable the processRow() function.
	this.processRow = async () => {}
	this.skipTable = true;
  }
 
  batchComplete() {
    return ((this.rowCounters.cached === this.BATCH_SIZE) && !this.skipTable)
  }
  
  batchRowCount() {
    return this.rowCounters.cached
  }
  
  reportBatchWrites() {
    return !this.supressBatchWriteLogging
  }
  
  commitWork() {
    return ((this.rowCounters.written >= this.COMMIT_COUNT) && !this.skipTable)
  }

  hasPendingRows() {
    return ((this.rowCounters.cached > 0) && !this.skipTable)
  }
              

  async checkColumnCount(row) {
	  
	try {
	  if (!this.skipTable) {
        assert.strictEqual(this.tableInfo.columnNames.length,row.length,`Table ${this.tableName}. Incorrect number of columns supplied.`)
	  }
	} catch (cause) {
	  const info = this.tableInfo === undefined  ? this.tableName : this.tableInfo
	  await this.handleIterativeError('CACHE',cause,this.rowCounters.received+1,row,info);
	}
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
		this.reportBatchError(`INSERT MANY`,e,this.batch[0],this.batch[this.batch.length-1])
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableInfo.tableName,this.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative' 
        
      }
    }

    for (const row in batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,batch[row]);
   	    this.rowCounters.written++
      } catch (cause) {
        const errInfo = {}
        await this.handleIterativeError(`INSERT ONE`,cause,row,batch[row],errInfo);
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

  createBatchException(cause,batchSize,firstRow,lastRow,info) {

    const details = {
	  currentSettings        : {
	    yadamu               : this.dbi.yadamu
	  , systemInformation    : this.dbi.systemInformation
	  , metadata             : { 
	      [this.tableName]   : this.dbi.metadata[this.tableName]
	    }
	  }
	, columnNames          : this.tableInfo.columnNames
	, targetDataTypes      : this.tableInfo.targetDataTypes 
	}
    Object.assign(details, info === undefined ? {} : typeof info === 'object' ? info : {info: info})
    return new BatchInsertError(cause,this.tableName,batchSize,firstRow,lastRow,details)
  }
  
  createIterativeException(cause,batchSize,rowNumber,row,info) {
    // String to Object conversion takes place in the handleIterativeError since the JSON record is written to the Rejected Records file
    const details = {
	  currentSettings        : {
	    yadamu               : this.dbi.yadamu
	  , systemInformation    : this.dbi.systemInformation
	  , metadata             : { 
	      [this.tableName]   : this.dbi.metadata[this.tableName]
	    }
	  }
	, columnNames            : this.tableInfo.columnNames
	, targetDataTypes        : this.tableInfo.targetDataTypes 
	}
    Object.assign(details, info === undefined ? {} : typeof info === 'object' ? info : {info: info})
    return new IterativeInsertError(cause,this.tableName,batchSize,rowNumber,row,details)
  }
  	
  async rejectRow(tableName,row) {
    // Allows the rejection process to be overridden by a particular driver.
    await this.dbi.yadamu.REJECTION_MANAGER.rejectRow(tableName,row);
  }
  
  async handleIterativeError(operation,cause,rowNumber,record,info) {
	  
    this.rowCounters.skipped++;
	
	try {
      await this.rejectRow(this.tableName,record);
      const iterativeError = this.createIterativeException(cause,this.rowCounters.cached,rowNumber,record,info)
      this.yadamuLogger.logRejected([this.dbi.DATABASE_VENDOR,this.tableName,operation,this.rowCounters.cached,rowNumber],iterativeError);
    } catch (e) {
      this.yadamuLogger.handleException([this.dbi.DATABASE_VENDOR,'ITERATIVE_ERROR',this.tableName,this.insertMode],e)
    }

	
    if (this.rowCounters.skipped === this.dbi.TABLE_MAX_ERRORS) {
      this.yadamuLogger.error([this.dbi.DATABASE_VENDOR,this.tableName,this.insertMode],`Maximum Error Count exceeded. Skipping Table.`);
	  this.abortTable()
    }
  }

  async handleSpatialError(operation,cause,rowNumber,record,info) {
	  
    this.rowCounters.skipped++;
	
	try {
      await this.rejectRow(this.tableName,record);
      const iterativeError = this.createIterativeException(cause,this.rowCounters.cached,rowNumber,record,info)
      this.yadamuLogger.logRejectedAsWarning([this.dbi.DATABASE_VENDOR,this.tableName,operation,this.rowCounters.cached,rowNumber],iterativeError);
    } catch (e) {
      this.yadamuLogger.handleException([this.dbi.DATABASE_VENDOR,'ITERATIVE_ERROR',this.tableName,this.insertMode],e)
    }

	
    if (this.rowCounters.skipped === this.dbi.TABLE_MAX_ERRORS) {
      this.yadamuLogger.error([this.dbi.DATABASE_VENDOR,this.tableName,this.insertMode],`Maximum Error Count exceeded. Skipping Table.`);
	  this.abortTable()
    }
  }

  reportBatchError(operation,cause,firstRow,lastRow,info) {
    const batchException = this.createBatchException(cause,this.rowCounters.cached,firstRow,lastRow,info)
    this.yadamuLogger.handleWarning([this.dbi.DATABASE_VENDOR,this.tableName,operation,this.insertMode,this.rowCounters.cached],batchException)
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

  /*
  async finalize() {
    return !this.skipTable
  }
  */
  
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
	await this.checkColumnCount(data)
  	await this.cacheRow(data)
    this.rowCounters.received++;
    if ((this.rowCounters.received % this.feedbackInterval === 0) & !this.batchComplete()) {
      this.yadamuLogger.info([`${this.tableInfo.tableName}`,this.insertMode],`Rows Cached: ${this.rowCounters.cached()}.`);
    }
    if (this.batchComplete()) {
      await this.writeBatch(this.status);
	  if (this.skipTable) {
        await this.rollbackTransaction();
      }
      if (this.reportBatchWrites && !this.commitWork(this.rowCounters.received)) {
        this.yadamuLogger.info([`${this.tableInfo.tableName}`,this.insertMode],`Rows written:  ${this.rowCounters.written}.`);
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
	  // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber(),messageType,this.rowCounters.received,this.writableLength,this.writableHighWaterMark],'_write()')
      switch (messageType) {
        case 'data':
		  // processRow() becomes a No-op after calling abortTable()
          await this.processRow(obj.data)
		  break;
		case 'table':
		  await this.initialize(obj.table)
		  // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber(),messageType,this.rowCounters.received],'initialized')
		  break;  
	  case 'eod':
        // Used when processing serial data sources such as files to indicate that all records have been processed by the writer
	    // this.yadamuLogger.trace([this.constructor.name,`_write()`,this.dbi.DATABASE_VENDOR,messageType,this.tableName,'EMIT'],`"allDataReceived"`)  
	    this.emit('allDataReceived')
		break;	
  	  default:
      }
      callback();
    } catch (e) {
      this.yadamuLogger.handleException([`Writer`,this.dbi.DATABASE_VENDOR,this.tableName,this.dbi.yadamu.ON_ERROR,this.dbi.getWorkerNumber(),messageType],e);
  	  /*
  	  /*
	  **
	  ** Error Handling. 
	  **   ABORT: Rollback the current transaction. Pass the error to the cllback function, which should propogate the error.
	  **   SKIP:  
	  **   FLUSH: 
	  **
	  */
	  try {
        switch (this.dbi.yadamu.ON_ERROR) {
		  case 'SKIP':
		  case 'FLUSH':
            callback();
		    break;
		  case 'ABORT': 
		    // Treat anything but SKIP or FLUSH, including undefined as ABORT
		  default:
		    if (this.dbi.transactionInProgress === true) {
			  // No need to check for lost connction. Rollback will pass through a lost connection error without attempting to perform the rollback operation 
  		      await this.rollbackTransaction(e)
			}
            // Passing the exception to callback triggers the onError() event
		    callback(e);
		    break;
		}
      } catch (e) {
        // Passing the exception to callback triggers the onError() event
		callback(e)	
      }
    }
  }
  
  async finalize(cause) {
    // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.skipTable,this.dbi.transactionInProgress,this.hasPendingRows(),this.rowCounters.received,this.rowCounters.committed,this.rowCounters.written,this.rowCounters.cached],'finalize()')
    if (this.hasPendingRows() && !this.skipTable) {
      this.skipTable = await this.writeBatch();   
    }
	if (this.dbi.transactionInProgress === true) {
      if (this.skipTable === true) {
        await this.rollbackTransaction(cause)
      }
      else {
        await this.commitTransaction()
	  }
    }
    this.endTime = performance.now()
  }	  
  	   	  
  async _final(callback) {
    // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber()],'_final()')
	this.endTime = performance.now();
	try {
	  await this.finalize()
	  callback();
    } catch (e) {
      this.yadamuLogger.handleException([`${this.dbi.DATABASE_VENDOR}`,`"${this.tableName}"`],e);
	  // Passing the exception to callback triggers the onError() event
      callback(e);
    } 
  } 

  async forcedEnd(cause) {
	// Called when a writer fails. Once a writer has emitted an 'error' event calling the end() method does not appear to invoke that the _final() method.
	// this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber()],'forcedEnd()')
	try {
	  await this.finalize(cause);
    } catch (e) {
      this.yadamuLogger.handleException([`${this.dbi.DATABASE_VENDOR}`,`"${this.tableName}"`],e);
    } 
  }
  
  reportPerformance(readTimings) {
	const writeTimingss = this.getStatistics();
	const readElapsedTime = readTimings.parserEndTime - readTimings.readerStartTime;
    const writerElapsedTime = writeTimingss.endTime - writeTimingss.startTime;        
	const pipeElapsedTime = writeTimingss.endTime - readTimings.pipeStartTime;
	const readThroughput = isNaN(readElapsedTime) ? 'N/A' : Math.round((readTimings.rowsRead/readElapsedTime) * 1000)
    const writerThroughput = isNaN(writerElapsedTime) ? 'N/A' : Math.round((writeTimingss.counters.committed/writerElapsedTime) * 1000)
	
	let readStatus = ''
	let rowCountSummary = ''
	
    const readerTimings = `Reader Elapsed Time: ${YadamuLibrary.stringifyDuration(readElapsedTime)}s. Throughput ${Math.round(readThroughput)} rows/s.`
	const writerTimings = `Writer Elapsed Time: ${YadamuLibrary.stringifyDuration(writerElapsedTime)}s. SQL Exection Time: ${YadamuLibrary.stringifyDuration(Math.round(writeTimingss.sqlTime))}s. Throughput: ${Math.round(writerThroughput)} rows/s.`
    
	if ((readTimings.rowsRead === 0) || (readTimings.rowsRead === writeTimingss.counters.committed)) {
      rowCountSummary = `Rows ${readTimings.rowsRead}.`
    }
	else {
      rowCountSummary = `Read ${readTimings.rowsRead}. Written ${writeTimingss.counters.committed}.`
    }

	rowCountSummary = writeTimingss.counters.skipped > 0 ? `${rowCountSummary} Skipped ${writeTimingss.counters.skipped}.` : rowCountSummary
   
	if (readTimings.failed) {
	  rowCountSummary = readTimings.tableNotFound === true ? `Table not found.` : `Read operation failed. ${rowCountSummary} `  
      this.yadamuLogger.error([`${this.tableName}`,`${writeTimingss.insertMode}`],`${rowCountSummary} ${readTimings} ${writerTimings}`)  
	}
	else {
	  if (readTimings.rowsRead !== writeTimingss.counters.committed) {
        this.yadamuLogger.error([`${this.tableName}`,`${writeTimingss.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
      }
	  else {
        this.yadamuLogger.info([`${this.tableName}`,`${writeTimingss.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
	  }
    }	  
	
    const timings = {[this.tableName] : {rowCount: writeTimingss.counters.committed, insertMode: writeTimingss.insertMode,  rowsSkipped: writeTimingss.counters.skipped, elapsedTime: Math.round(writerElapsedTime).toString() + "ms", throughput: Math.round(writerThroughput).toString() + "/s", sqlExecutionTime: Math.round(writeTimingss.sqlTime)}};
    this.dbi.yadamu.recordTimings(timings);   
    return timings;
  }
  
  async _transform (data,encoding,callback)  {
    this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber()],'_transform()')
	callback()
  }
	 
}

module.exports = YadamuWriter;
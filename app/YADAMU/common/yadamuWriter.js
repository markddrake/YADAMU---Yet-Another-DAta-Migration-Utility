"use strict"

const Writable = require('stream').Writable
const { performance } = require('perf_hooks');

const YadamuLibrary = require('./yadamuLibrary.js');

class YadamuWriter extends Writable {

  constructor(options,dbi,primary,status,yadamuLogger) {
	super(options)
    this.dbi = dbi;
    this.schema = this.dbi.parameters.TO_USER;
    this.status = status;
    this.yadamuLogger = yadamuLogger;    
    this.primary = primary
    primary.registerWorker(this)
	this.maxErrors =  this.dbi.parameters.MAX_ERRORS ? this.dbi.parameters.MAX_ERRORS : 10
    this.rejectManager = this.dbi.yadamu.rejectManager
    this.configureFeedback(this.dbi.parameters.FEEDBACK); 	
	this.resetWriter();
	this.errorCallbacks = {}
	this.errorCallback = undefined
	
  }
  
  resetWriter() {
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
	// this.yadamuLogger.trace([this.constructor.name],`setTableInfo(${tableName})`)
	this.tableName = tableName  
    this.tableInfo = this.dbi.getTableInfo(tableName)
	this.supressBatchWriteLogging = (this.tableInfo.batchSize === this.tableInfo.commitSize) // Prevent duplicate logging if batchSize and Commit SIze are the same
 	
	// Set Skip Table to true if MODE is DDL_ONLY otherwise false.
	// Strictly defensive as YadamuWriter implementations should never receive rows during DDL_ONLY operations.
	
	this.resetWriter();
    // Register the table specific onError callback 
	this.errorCallback = this.errorCallbacks[tableName]
	delete this.errorCallbacks[tableName]
	this.on('error',(err) => {this.errorCallback(err)})
  }
  
  registerErrorCallback(tableName,callback) {
	// this.yadamuLogger.trace([this.constructor.name,tableName],`registerErrorCallback(${typeof callback})`)
    this.errorCallbacks[tableName] = callback;
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
    this.primary.write({timings: timings})
  }
  
  async flushCache(readerStatistics) {
    // this.yadamuLogger.trace([this.constructor.name,this.hasPendingRows(),this.batch.length,this.lobBatch.length],'_flushCahce()')
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
    this.reportPerformance(readerStatistics);

    // Remove the table specific onError callback
	this.removeListener('error',this.errorCallback);
  }	  
  	   
  async _write(obj, encoding, callback) {
	const messageType = Object.keys(obj)[0]
	try {
	  // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber(),messageType],'_write()')
      switch (messageType) {
	    case 'table':
  	      this.setTableInfo(obj.table);
		  await this.initialize()
		  break;
        case 'data':
          if (this.skipTable === false) {
			await this.processRow(obj.data)
          }
		  // ### Else Throw an Error to stop the reader ????
		  break;
		case 'eod':
		  await this.flushCache(obj.eod)
		  break;
  	    case 'eof':		 
		case 'dataComplete':
		   // 'eof' is generated by the JSON Parser (Text or HTML Parser) when the parsing operation detects the end of the outermost object or array. It is the equivilant of dataComplete for a Database based reader		  
		   // 'dataComplete' is generated by a database export when there is no more data for this worker to process.
	      this.end()
		  break;
		default:
      }
      callback();
    } catch (e) {
	  this.yadamuLogger.handleException([`Writer`,`${this.dbi.DATABASE_VENDOR}`,this.tableName,this.dbi.getWorkerNumber(),messageType],e);
	  this.skipTable = true;
	  try {
        await this.rollbackTransaction(e)
        callback();
      } catch (e) {
        // Passing the exception to callback triggers the onError() event
        callback(e); 
      }
    }
  }
 
  async _final(callback) {
	// this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber()],'final()')
	try {
	  if (!this.dbi.isPrimary()) {
		await this.dbi.releaseWorkerConnection();
      }
	  this.primary.write({workerComplete: this.dbi.getWorkerNumber()})
      callback();
    } catch (e) {
      this.yadamuLogger.logException([`${this.dbi.DATABASE_VENDOR}`,`"${this.currentTable}"`],e);
	  // Passing the exception to callback triggers the onError() event
      callback(e);
    } 
  } 

}

module.exports = YadamuWriter;
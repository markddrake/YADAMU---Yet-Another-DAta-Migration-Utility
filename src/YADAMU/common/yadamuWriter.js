"use strict"

const fs = require('fs')
const assert = require('assert').strict;
const {Readable, Writable, Transform, pipeline } = require('stream')
const { performance } = require('perf_hooks');

const YadamuConstants = require('./yadamuConstants.js');
const YadamuLibrary = require('./yadamuLibrary.js');
const {YadamuError, BatchInsertError, IterativeInsertError, DatabaseError} = require('./yadamuException.js')

class YadamuWriter extends Transform {

  get BATCH_SIZE()     {  return this.tableInfo._BATCH_SIZE }
  get COMMIT_COUNT()   {  return this.tableInfo._COMMIT_COUNT }
  get SPATIAL_FORMAT() {  return this.tableInfo._SPATIAL_FORMAT }
  get SOURCE_VENDOR()  {  return this.readerMetrics?.DATABASE_VENDOR || 'YABASC'}
  
  get FEEDBACK_MODEL()     { return this._FEEDBACK_MODEL }
  get FEEDBACK_INTERVAL()  { return this._FEEDBACK_INTERVAL }
  get FEEDBACK_DISABLED()  { return this.FEEDBACK_MODEL === undefined }
  get PARTITIONED_TABLE()  { return this.partitionInfo ? true : false }
  
  get REPORT_COMMITS()     { 
    return this._REPORT_COMMITS || (() => { 
	  this._REPORT_COMMITS = ((this._FEEDBACK_MODEL === 'COMMIT') || (this._FEEDBACK_MODEL === 'ALL')); 
	  return this._REPORT_COMMITS
	})() 
  }
  
  get REPORT_BATCHES()     { 
     return this._REPORT_BATCHES || (() => { 
	   this._REPORT_BATCHES = (((this._FEEDBACK_MODEL === 'BATCH') || (this._FEEDBACK_MODEL === 'ALL')) && (this.BATCH_SIZE === this.COMMIT_COUNT)); 
	   return this._REPORT_BATCHES
	 })() 
  }
  
  set FEEDBACK_MODEL(feedback)  {
    if (!isNaN(feedback)) {
	  this._FEEDBACK_MODEL    = 'ALL'
	  this._FEEDBACK_INTERVAL = parseInt(feedback)
	}
	else {
	  this._FEEDBACK_MODEL    = feedback
	  this._FEEDBACK_INTERVAL = 0
    }
  } 
  
  constructor(options,dbi,tableName,ddlComplete,status,yadamuLogger) {
    options.highWaterMark = 64
	options.emitClose = true
    super(options)
    this.dbi = dbi;
    this.schema = this.dbi.parameters.TO_USER;
    this.ddlComplete = ddlComplete;
    this.status = status;
    this.yadamuLogger = yadamuLogger;    
    
	this.FEEDBACK_MODEL = this.dbi.parameters.FEEDBACK
	
    this.tableName = tableName  
	this.displayName = tableName
	this.partitionInfo = undefined
	
    this.metrics = {
      received   : 0 // Rows accepted
    , batchCount : 0 // Batches created
    , cached     : 0 // Rows received and cached by appendRow(). Reset every time a batch of cached rows is written to disk
    , written    : 0 // Rows written to disk in the current transaction
    , committed  : 0 // Rows successfully committed to disk
    , skipped    : 0 // Rows not written to disk due to unrecoverable write errors
    , lost       : 0 // Rows written to disk and thene lost as a result of a rollback or lost connnection 
	, idleTime   : 0 // Time spent waiting for previous batch to complete before writing new batch
    }
    
    this.dbi.setMetrics(this.metrics)
    
    this.batch = [];
    this.skipTable = this.dbi.MODE === 'DDL_ONLY';
    this.sqlInitialTime = this.dbi.sqlCumlativeTime
    this.startTime = performance.now();
	this.endTime = undefined
    this.batchOperations = new Set()
    
    this.on('pipe',(src)=>  {
	  this.eventSource = src 
	})
    // this.on('unpipe',(src)=>{console.log('unpipe',src.constructor.name)})
  }
  
  setTableInfo(tableName) {
    this.skipTable = true
    this.tableInfo = this.dbi.getTableInfo(tableName)
    this.tableInfo.insertMode = this.tableInfo.insertMode || 'Batch';    
    this.skipTable = false;
  }
   
  async initialize(tableName) { 	
    // Do not start processing table until all DDL operations have completed.
    // this.yadamuLogger.trace([this.constructor.name,'DLL_COMPLETE',this.dbi.getWorkerNumber()],'WAITING')
	await this.ddlComplete
	// this.yadamuLogger.trace([this.constructor.name,'DLL_COMPLETE',this.dbi.getWorkerNumber(),tableName],'PROCESSING')
	
	// Workers need to reload their copy of the Statement Cache from the Manager before processing can begin
    this.dbi.reloadStatementCache()
	
    this.setTableInfo(tableName);
	
	// Partition based insert operations require a partition specific variable of the tales DML statement.
	this.dml = this.tableInfo.dml
	await this.beginTransaction()
  }
  
  async initializePartition(partitionInfo) {
	this.partitionInfo = partitionInfo
	const padSize = this.partitionInfo.partitionCount.toString().length
    await this.initialize(partitionInfo.tableName)
	if (partitionInfo.partitionNumber === 1) {
  	  this.tableInfo.partitionCount = partitionInfo.partitionCount
	  this.tableInfo.partitionsRemaining = partitionInfo.partitionCount
	}
	this.displayName = `${this.displayName}(${this.partitionInfo.partitionName || `#${partitionInfo.partitionNumber.toString().padStart(padSize,"0")}`})`
  }
  
  rowsLost() {
	return this.metrics.lost > 0
  }
        
  newBatch() {
	// this.batch.length = 0;
	this.batch = []
	this.metrics.cached = 0;
  }
  
  releaseBatch(batch) {
	if (Array.isArray(batch)) {
	  batch.length = 0;
	}
  }
  
  abortTable() {
    // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber()],'abortTable()')
	
   
    this.skipTable = true;
	
	// Abort means that records written but not committed are lost
	// If rollback occurs prior to abort, written is already added to lost and set to zero.
	// If abort occurs prior to rollback or abort is called multiple times written is already zeroed out.

    this.metrics.lost += this.metrics.written;
    this.metrics.written = 0;
	// this.releaseBatch(this.batch)	  

    // Disable the processRow() function.
    this.processRow = async () => {}
  }
 
  flushBatch() {
    return ((this.metrics.cached === this.BATCH_SIZE) && !this.skipTable)
  }
  
  batchRowCount() {
    return this.metrics.cached
  }
  
  commitWork() {
    // While COMMIT is defined as a multiple of BATCH_SIZE some drivers may write smaller batches.
    return (((this.COMMIT_COUNT > 0) && (this.metrics.written >= this.COMMIT_COUNT)) && !this.skipTable)
  }

  hasPendingRows() {
    return ((this.metrics.cached > 0) && !this.skipTable)
  }
              
  createBatchException(cause,batchSize,firstRow,lastRow,info) {

    try {
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
    } catch (e) {
	  cause.batchErrorIssue = e
      return cause
    }
  }
  
  createIterativeException(cause,batchSize,rowNumber,row,info) {
    // String to Object conversion takes place in the handleIterativeError since the JSON record is written to the Rejected Records file
    try {
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
    } catch (e) {
	  cause.IterativeErrorIssue = e
      return cause
    }
  }
    
  async rejectRow(tableName,row) {
    // Allows the rejection process to be overridden by a particular driver.
    await this.dbi.yadamu.REJECTION_MANAGER.rejectRow(tableName,row);
  }
  
  handleIterativeError(operation,cause,rowNumber,record,info) {
	  
	if (this.rowsLost()) {
	  throw cause
	}
	
    this.metrics.skipped++;
    
    try {
      this.rejectRow(this.tableName,record);
      const iterativeError = this.createIterativeException(cause,this.metrics.cached,rowNumber,record,info)
	  this.dbi.trackExceptions(iterativeError);
      this.yadamuLogger.logRejected([...(typeof cause.getTags === 'function' ? cause.getTags() : []),this.dbi.DATABASE_VENDOR,this.displayName,operation,this.metrics.cached,rowNumber],iterativeError);
    } catch (e) {
      this.yadamuLogger.handleException([this.dbi.DATABASE_VENDOR,'ITERATIVE_ERROR',this.displayName,this.tableInfo.insertMode],e)
    }
    
    if (this.metrics.skipped === this.dbi.TABLE_MAX_ERRORS) {
      this.yadamuLogger.error([this.dbi.DATABASE_VENDOR,this.displayName,this.tableInfo.insertMode],`Maximum Error Count exceeded. Skipping Table.`);
      this.abortTable()
    }
  }

  handleSpatialError(operation,cause,rowNumber,record,info) {
      
    this.metrics.skipped++;
    
    try {
      this.rejectRow(this.tableName,record);
      const iterativeError = this.createIterativeException(cause,this.metrics.cached,rowNumber,record,info)
	  this.dbi.trackExceptions(iterativeError);
      this.yadamuLogger.logRejectedAsWarning([this.dbi.DATABASE_VENDOR,this.displayName,operation,this.metrics.cached,rowNumber],iterativeError);
    } catch (e) {
      this.yadamuLogger.handleException([this.dbi.DATABASE_VENDOR,'ITERATIVE_ERROR',this.displayName,this.tableInfo.insertMode],e)
    }

    
    if (this.metrics.skipped === this.dbi.TABLE_MAX_ERRORS) {
      this.yadamuLogger.error([this.dbi.DATABASE_VENDOR,this.displayName,this.tableInfo.insertMode],`Maximum Error Count exceeded. Skipping Table.`);
      this.abortTable()
    }
  }

  reportBatchError(operation,cause,firstRow,lastRow,info) {
	 
    if (this.rowsLost() || YadamuError.missingTable(cause)) {
	  throw cause
    }
    const batchException = this.createBatchException(cause,this.metrics.cached,firstRow,lastRow,info)
	this.dbi.trackExceptions(batchException);
    this.yadamuLogger.handleWarning([...(cause.getTags?.() || []),this.dbi.DATABASE_VENDOR,this.displayName,operation,this.tableInfo.insertMode,this.metrics.cached],batchException)
  }
  

  checkColumnCount(row) {
      
    try {
      if (!this.skipTable) {
        assert.strictEqual(this.tableInfo.columnNames.length,row.length,`Table ${this.tableName}. Incorrect number of columns supplied.`)
      }
    } catch (cause) {
      const info = this.tableInfo === undefined  ? this.tableName : this.tableInfo
      this.handleIterativeError('CACHE',cause,this.metrics.received+1,row,info);
    }
  }
              
  cacheRow(row) {
	  
    // Apply transformations and cache transformed row.
    
    // Use forEach not Map as transformations are not required for most columns. 
    // Avoid uneccesary data copy at all cost as this code is executed for every column in every row.

    // this.yadamuLogger.trace([this.constructor.name,'YADAMU WRITER',this.metrics.cached],'cacheRow()')    
      
    this.rowTransformation(row)
    this.batch.push(row);
    this.metrics.cached++
    return this.skipTable;
  }  

   async beginTransaction() {
    await this.dbi.beginTransaction();
  }
  
  async commitTransaction() {
    // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber()],'commitTransaction()')
    if (!this.skipTable) {
      await this.dbi.commitTransaction()
      this.metrics.committed += this.metrics.written;
      this.metrics.written = 0;
    }
  }
  
  async rollbackTransaction(cause) {
	 
	 // Rollback means that records written but not committed are lost
	  // Accounting for in-flight records is dependant on the value of ON_ERROR.

      // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber()],'rollbackTransaction()')
	 
	  this.metrics.lost += this.metrics.written;
      this.metrics.written = 0;
      await this.dbi.rollbackTransaction(cause)
  }

  createWriteStream(filename) {
  
	return new Promise((resolve,reject) => {
      const outputStream = fs.createWriteStream(filename,{flags :"w"})
	  const stack = new Error().stack
      outputStream.on('open',() => {resolve(outputStream)}).on('error',(err) => {reject(err.code === 'ENOENT' ? new DirectoryNotFound(err,stack,filename) : new FileError(err,stack,filename) )})
	})
  }
  
  closeWriteStream(fs) {
    return new Promise((resolve,reject) => {
	  fs.end(null,null,()=>{resolve()})
	})
  }
  
  writeRowAsCSV(fs,row) {

	row.forEach((col,idx) => {
	  if (col === null) {
		fs.write((idx < (row.length-1)) ? ',' : '\n')
	  }
	  else {
        this.csvTransformations[idx](fs,col)
	  }
	})
  }
  
  writeBatchAsCSV(fs,batch) {  
    batch.forEach((row) => {
	   this.writeRowAsCSV(fs,row)
    })
  }   

  getCSVTransformation(col,isLastColumn) {

    switch (typeof col) {
	  case "number":
        return (isLastColumn)
 	    ? (fs,col) => {
   		    fs.write(col.toString());
			fs.write('\n');
		  } 
		: (fs,col) => {
   		    fs.write(col.toString());
			fs.write(',');
		  }
		case "boolean":
		  return (isLastColumn) 
		  ? (fs,col) => {
  		      fs.write(col ? 'true' : 'false')
			  fs.write('\n');
		    } 
		  : (fs,col) => {
  		      fs.write(col ? 'true' : 'false')
			  fs.write(',');
		    } 
		case "string":
		  return (isLastColumn)
		  ? (fs,col) => {
		      // sw.write(JSON.stringify(col));
              fs.write('"')
	  	      fs.write(col.indexOf('"') > -1 ? col.replace(/"/g,'""') : col)
		      fs.write('"\n')
		    } 
		  : (fs,col) => {
		      // sw.write(JSON.stringify(col));
              fs.write('"')
	  	      fs.write(col.indexOf('"') > -1 ? col.replace(/"/g,'""') : col)
		      fs.write('",')
		    } 
	    case "object":
		  switch (true) {
		    case (col instanceof Date):
		      return (isLastColumn)
		      ? (fs,col) => {
                  fs.write('"')
                  fs.write(col === null ? '' : col.toISOString())
		          fs.write('"\n')
		        } 
		      : (fs,col) => {
                fs.write('"')
		        fs.write(col === null ? '' : col.toISOString())
		        fs.write('",')
		      } 
			default:
		      return (isLastColumn)
		      ? (fs,col) => {
                  fs.write('"')
		          fs.write(col === null ? '' : JSON.stringify(col).replace(/"/g,'""'))
		          fs.write('"\n')
		        } 
		      : (fs,col) => {
                  fs.write('"')
		          fs.write(col === null ? '' : JSON.stringify(col).replace(/"/g,'""'))
		          fs.write('",')
		        } 
		  }
		default:
		  return (isLastColumn)
		  ? (fs,col) => {
  		      fs.write(col);
			  fs.write('\n');
		    } 
	      : (fs,col) => {
  		      fs.write(col);
		  	  fs.write(',');
		    } 
	}
  }	 
  
  setCSVTransformations(batch) {

    // RFC4180

    // Set the CSV Transformation functions based on the first non-null value for each column.

	const lastIdx = batch[0].length - 1
	this.csvTransformations = batch[0].map((col,colIdx) => {
       const lastColumn = colIdx === lastIdx
	   return col !== null ? this.getCSVTransformation(col,lastColumn) : this.getCSVTransformation(batch[batch.findIndex((row) => {return row[colIdx] !== null})]?.[colIdx],lastColumn)
	})
		
  }  

  async _writeBatch(batch,rowCount) {
    /*        
    if (this.tableInfo.insertMode === 'Batch') {
      try {    
        await this.dbi.createSavePoint();
        const results = await this.dbi.executeSQL(this.tableInfo.dml,batch);
        this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
        this.metrics.written += rowCount;
        return this.skipTable
      } catch (cause) {
        await this.dbi.restoreSavePoint(cause);
        this.reportBatchError(batch,`INSERT MANY`,cause)
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.displayName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
        this.tableInfo.insertMode = 'Iterative' 
        
      }
    }

    for (const row in batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,batch[row]);
        this.metrics.written++
      } catch (cause) {
        const errInfo = {}
        this.handleIterativeError(`INSERT ONE`,cause,row,batch[row],errInfo);
        if (this.skipTable) {
          break;
        }
      }
    }     
   
    this.endTime = performance.now();
    this.batch.length = 0;
    this.metrics.cached = 0;
    return this.skipTable          
	*/
  }
      
  async processBatch(batch,rowsReceived,rowsCached) {

    // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,this.displayName,rowsReceived,rowsCached],'processBatch()')
	
	if (!this.skipTable) {
	  try {
		 
	    this.skipTable = await this._writeBatch(batch,rowsCached)
        if (this.skipTable) {
          await this.rollbackTransaction();
        }	  
        if (this.REPORT_BATCHES && !this.commitWork(rowsReceived)) {
          this.yadamuLogger.info([`${this.displayName}`,this.tableInfo.insertMode],`Rows written:  ${this.metrics.written}.`);
        }                   
	    // Commit after a writing a batch if the Commit threshold has been reached or passed. Start a new Transaction
        if (this.commitWork(rowsReceived)) {
		  try {
            await this.commitTransaction()
            if (this.REPORT_COMMITS) {
              this.yadamuLogger.info([`${this.displayName}`,this.tableInfo.insertMode],`Rows committed: ${this.metrics.committed}.`);
            }          
		  } catch (commitFailure) {
			// Attempt to start a new transaction even if the commit failed.
			try {
              await this.beginTransaction();            
			} catch (beginFailure) {
			  // Append the Begin Failure to the Commit Failure
			  // ### Is This FATAL. Cannot establish a transaction context. 
			  commitFailure.beginFailure = beginFailure
			}
			throw commitFailure
		  }
          await this.beginTransaction();            	  
        }
	  } catch (err) { 

  	    /*
	    **
	    ** An unrecoverable error occured while writing a batch. Examples of unrecoverable errors include lost connections, missing tables, too many errors during iterative inserts
	    ** or anything else that causes the _writeBach implementation to throw an error. This will also catch issues with Transaction State (Errors during COMMIT and/or BEGIN transaction operations)
        **
	    ** processBatch() is typically executed outside of 'try', 'await', 'catch' block, so that Yadamu can prepare the next batch of rows while the current batch of rows in being written.
	    **
	    ** Emit 'close' here to terminate the current pipeline operation. Thowing an exception or a rejection will cause "Unhandled Exception" or "UnhandledRejection" errors.
	    **
	    */

        if (this.metrics.lost > 0) {
   	      this.underlyingError = err
          // ### Need to caculate lost rows correctly when TABLE_MAX_ERRORS exceeded
          if (YadamuConstants.ABORT_CURRENT_TABLE.includes(this.dbi.ON_ERROR)) {
	        // Aborting the current table (ABORT,SKIP) means that no more rows will be read and any records cached but not written will be lost. 
	        this.readerMetrics.rowsRead = this.eventSource.getRowCount()
			this.readerMetrics.lost = this.eventSource.writableLength
	        this.metrics.lost+= rowsCached
   	        this.releaseBatch(batch)
      	    this.abortTable()
      	    // Emit an explicit 'close' event. This will terminate the pipeline operation which will trigger 'error' and 'close' events on all of the upstream components.
    	    this.emit('close')
		  }
        }
	  }
	}
    else {
	  // Entire Batch is skipped ### Log the batch ???
	  this.metrics.lost+= rowsCached
	  this.releaseBatch(batch)
	}
    // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,this.displayName,rowsReceived,rowsCached],'processBatch(): emit('batchWritten')`)
	this.emit('batchWritten');
  }

  getBatchWritten(batchNumber) {
  
    const batchWritten = new Promise((resolve,reject) => {
      // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,'getBatchWritten()',batchNumber],'new batchWritten')
	  this.once('batchWritten',() => {
	    // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,'getBatchWritten()',batchNumber,err ? err.message : 'OK'],'batchWritten')
	    this.batchOperations.delete(this.batchWritten)
	    resolve();
	  })
    })
	this.batchOperations.add(batchWritten)
    return batchWritten
  }
  
  uncork(expected) {
	  
	 // Nasty Hack to prevent end() on the upstream stream uncorking the stream before the current batch is complete.
	 if (expected === true) {
	   return super.uncork()
	 }
  }		 
  
  async processRow(data) {
    // Be very careful about adding unecessary code here. This is executed once for each row processed by YADAMU. Keep it as lean as possible.
    this.checkColumnCount(data)
    this.cacheRow(data)
    this.metrics.received++;
    if ((this.metrics.received % this.FEEDBACK_INTERVAL === 0) & !this.flushBatch()) {
      this.yadamuLogger.info([`${this.tableName}`,this.tableInfo.insertMode],`Rows Cached: ${this.metrics.cached}.`);
    }
	if (this.flushBatch()) {
      this.cork()
	  // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,'processRow()',this.metrics.received,this.metrics.cached],'Cache Full')
	  const nextBatch = this.batch;
	  const rowsReceived = this.metrics.received
      const rowsCached = this.metrics.cached
      // Wait for any pending processBatch operations to complete
	  if (this.batchWritten) {
	    const startTime = performance.now()
	    await this.batchWritten;
	    const elapsedTime = performance.now() - startTime
	    this.metrics.idleTime+= elapsedTime;
	  }
      this.newBatch()
 	  this.uncork(true)
      this.batchWritten = this.getBatchWritten(this.metrics.batchCount)
	  this.processBatch(nextBatch,rowsReceived,rowsCached).catch((e) => {
        this.yadamuLogger.error([this.dbi.DATABASE_VENDOR,this.displayName,this.tableInfo.insertMode],'Write Batch Failed');
	    this.yadamuLogger.handleException([this.dbi.DATABASE_VENDOR,this.displayName,this.tableInfo.insertMode,'BATCH WRITE'],e)
      })
    }
  }
  
  getMetrics() {
    return {
      startTime     : this.startTime
    , endTime       : this.endTime
    , sqlTime       : this.dbi.sqlCumlativeTime - this.sqlInitialTime
    , insertMode    : this.tableInfo.insertMode
    , skipTable     : this.skipTable
    , metrics       : this.metrics
    }    
  }
  
  async endTable() {
  }
    
  async _write(obj, encoding, callback) {
    const messageType = Object.keys(obj)[0]
    try {
      // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber(),messageType,this.metrics.received,this.writableLength,this.writableHighWaterMark],'_write()')
      switch (messageType) {
        case 'data':
          // processRow() becomes a No-op after calling abortTable()
          this.processRow(obj.data)
          break;
        case 'table':
	      await this.initialize(obj.table)
          // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber(),messageType,this.metrics.received],'initialized')
          break;  
        case 'partition':
          // this.yadamuLogger.trace([this.constructor.name,this.displayName,obj.partition.partitionName,this.dbi.getWorkerNumber(),this.metrics.received],'Start ')
		  await this.initializePartition(obj.partition)
		  break;  
      case 'eod':
	  
	  
	  
        // Used when processing serial data sources such as files to indicate that all records have been processed by the writer
        // this.yadamuLogger.trace([this.constructor.name,`_write()`,this.dbi.DATABASE_VENDOR,messageType,this.displayName,'EMIT'],`"allDataReceived"`)  
		// await this.endTable()
        this.emit('allDataReceived')
        break;  
      default:
      }
      callback();
    } catch (e) {
      // this.yadamuLogger.trace([`WRITER`,this.dbi.DATABASE_VENDOR,this.displayName,this.dbi.ON_ERROR,this.dbi.getWorkerNumber(),messageType],e);    
	  this.yadamuLogger.handleException([`PIPELINE`,`WRITER`,this.SOURCE_VENDOR,this.dbi.DATABASE_VENDOR,`"${this.tableName}"`,messageType,this.dbi.ON_ERROR,this.dbi.getWorkerNumber()],e);    
	  try {
        switch (messageType) {
          case 'data':
            /*
            **
            ** Read Error Handling. 
            **   ABORT: Rollback the current transaction. Pass the exception to the callback function, which should propogate the exception.
            **   SKIP:  
            **   FLUSH: 
            **
            */
            try {
              switch (this.dbi.ON_ERROR) {
                case 'SKIP':
                case 'FLUSH':
                callback();
                break;
              case 'ABORT': 
                // Treat anything but SKIP or FLUSH, including undefined as ABORT
              default:
                if (this.dbi.TRANSACTION_IN_PROGRESS === true) {
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
          case 'table':
          case 'eod':
          default:
		    /*
			**
			** The pipeline operation does not appear to terminate after invoking the callback and passing an exception
		    ** emitting 'end' or 'finish' does not terminate the pipeline and causes the operation to hang.
			** emitting 'error' results in 'UNHANDLED REJECTION' conditions coming from each stream bit does cause the stream to stop
			** emitting 'close' results in 'PREMATURE CLOSE' getting thrown by the pipeline operation, so we need to make the underlying cause availalbe.
			**
			*/
		    // this.abortTable()
			this.underlyingError = e;
			// this.emit('close')
			callback(e)
        }
      } catch (err) {
		err.cause = e
		callback(err)    
      }
    }
  }

  setReaderMetrics(readerMetrics) {
    this.readerMetrics = readerMetrics
  }
  
  recordPartitionMetrics(partitionMetrics) {
	if (this.tableInfo.hasOwnProperty('metrics')) {
	  this.tableInfo.metrics.startTime = partitionMetrics.startTime < this.tableInfo.metrics.startTime ? partitionMetrics.startTime : this.tableInfo.metrics.startTime
	  this.tableInfo.metrics.endTime = partitionMetrics.endTime > this.tableInfo.metrics.endTime ? partitionMetrics.endTime : this.tableInfo.metrics.endTime
	  this.tableInfo.metrics.rowCount+= partitionMetrics.rowCount
	  this.tableInfo.metrics.rowsSkipped+= partitionMetrics.rowsSkipped
	  this.tableInfo.metrics.sqlExecutionTime+= partitionMetrics.sqlExecutionTime
	}
	else {
	 this.tableInfo.metrics = partitionMetrics
	}
  }
  
  reportPerformance(err) {
	
	const writerMetrics = this.getMetrics();
    writerMetrics.metrics.lost+= this.readerMetrics.lost + this.writableLength
	const readElapsedTime = this.readerMetrics.parserEndTime - this.readerMetrics.readerStartTime;
    const writerElapsedTime = writerMetrics.endTime - writerMetrics.startTime;        
    const pipeElapsedTime = writerMetrics.endTime - this.readerMetrics.pipeStartTime;
    const readThroughput = isNaN(readElapsedTime) ? 'N/A' : Math.round((this.readerMetrics.rowsRead/readElapsedTime) * 1000)
    const writerThroughput = isNaN(writerElapsedTime) ? 'N/A' : Math.round((writerMetrics.metrics.committed/writerElapsedTime) * 1000)
    
    let readStatus = ''
    let rowCountSummary = ''
    let idleTime = 0;
    
    const readerTimings = `Reader Elapsed Time: ${YadamuLibrary.stringifyDuration(readElapsedTime)}s. Throughput ${Math.round(readThroughput)} rows/s. ${(writerMetrics.metrics.idleTime * 20) > writerElapsedTime ? ` Idle Time: ${YadamuLibrary.stringifyDuration(writerMetrics.metrics.idleTime)}s.` : ''}`
    const writerTimings = `Writer Elapsed Time: ${YadamuLibrary.stringifyDuration(writerElapsedTime)}s. SQL Exection Time: ${YadamuLibrary.stringifyDuration(Math.round(writerMetrics.sqlTime))}s. Throughput: ${Math.round(writerThroughput)} rows/s.`
    
    if ((this.readerMetrics.rowsRead === 0) || (this.readerMetrics.rowsRead === writerMetrics.metrics.committed)) {
      rowCountSummary = `Rows ${this.readerMetrics.rowsRead}.`
    }
    else {
      rowCountSummary = `Read ${this.readerMetrics.rowsRead}. Written ${writerMetrics.metrics.committed}.`
    }
    rowCountSummary = writerMetrics.metrics.skipped > 0 ? `${rowCountSummary} Skipped ${writerMetrics.metrics.skipped}.` : rowCountSummary
    rowCountSummary = writerMetrics.metrics.lost > 0 ? `${rowCountSummary} Lost ${writerMetrics.metrics.lost}.` : rowCountSummary
    
    const cause = this.readerMetrics.readerError || this.readerMetrics.parserError ||  this.underlyingError || err
	if (cause) {
	  const tags = YadamuError.lostConnection(cause) ? ['LOST CONNECTION'] : []
      tags.push(this.readerMetrics.readerError || this.readerMetrics.parserError ? 'STREAM READER' : 'STREAM WRITER')
 	  this.yadamuLogger.handleException(['PIPELINE',...tags,this.displayName,this.SOURCE_VENDOR,this.dbi.DATABASE_VENDOR,this.dbi.ON_ERROR,this.dbi.getWorkerNumber()],cause)
	}

	// console.log(writerMetrics.metrics)
	
	if (this.readerMetrics.failed) {
      rowCountSummary = this.readerMetrics.tableNotFound === true ? `Table not found.` : `Read operation failed. ${rowCountSummary} `  
      this.yadamuLogger.error([`${this.displayName}`,`${writerMetrics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
    }
    else {
	  switch (true) {
		case (this.readerMetrics.rowsRead == writerMetrics.metrics.committed):
          this.yadamuLogger.info([`${this.displayName}`,`${writerMetrics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
		  break
	    case (this.readerMetrics.rowsRead === (writerMetrics.metrics.committed + writerMetrics.metrics.skipped)):
          this.yadamuLogger.warning([`${this.displayName}`,`${writerMetrics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
		  break
	    case (this.readerMetrics.rowsRead === (writerMetrics.metrics.committed + writerMetrics.metrics.lost)):
          this.yadamuLogger.error([`${this.displayName}`,`${writerMetrics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
		  break
		default:
          this.yadamuLogger.error([`${this.displayName}`,`${writerMetrics.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
      }
    }     
    
	let metrics = {
      rowCount: writerMetrics.metrics.committed
	, insertMode: writerMetrics.insertMode
	, rowsSkipped: writerMetrics.metrics.skipped
	, elapsedTime: Math.round(writerElapsedTime).toString() + "ms"
	, throughput: Math.round(writerThroughput).toString() + "/s"
	, sqlExecutionTime: Math.round(writerMetrics.sqlTime)
	}
	
    if (this.dbi.yadamu.YADAMU_QA && ((this.readerMetrics.rowsRead - (writerMetrics.metrics.committed + writerMetrics.metrics.lost + writerMetrics.metrics.skipped))!== 0)) {
      this.yadamuLogger.qa([`${this.displayName}`,`${writerMetrics.insertMode}`,this.readerMetrics.rowsRead,writerMetrics.metrics.committed,writerMetrics.metrics.lost,writerMetrics.metrics.skipped,this.writableLength,this.readerMetrics.lost],`Inconsistent Metrics detected.`)  
    } 

    if (this.PARTITIONED_TABLE) {
      this.tableInfo.partitionsRemaining--
	  metrics.startTime = writerMetrics.startTime
	  metrics.endTime =  writerMetrics.endTime
	  this.recordPartitionMetrics(metrics); 
	  if (this.tableInfo.partitionsRemaining === 0) {
		metrics = this.tableInfo.metrics
		metrics.elapsedTime = metrics.endTime - metrics.startTime
		const throughput = Math.round((metrics.rowCount/metrics.elapsedTime) * 1000)
		metrics.throughput =  throughput  + "/s"
	    const timings = `Writer Elapsed Time: ${YadamuLibrary.stringifyDuration(metrics.elapsedTime)}s. SQL Exection Time: ${YadamuLibrary.stringifyDuration(Math.round(metrics.sqlExecutionTime))}s. Throughput: ${throughput} rows/s.`
	    this.yadamuLogger.info([`${this.tableName}`],`Total Rows ${metrics.rowCount}. ${timings}`)  
		delete metrics.startTime
		delete metrics.endTime
		delete metrics.partitionCount
        const tableMetrics = {[this.tableName] : metrics};
	    this.dbi.yadamu.recordMetrics(tableMetrics);  
	  }
    }
    else {
      const tableMetrics = {[this.tableName] : metrics};
      this.dbi.yadamu.recordMetrics(tableMetrics);  
	}
    return (this.readerMetrics.failed || (this.readerMetrics.rowsRead !== (writerMetrics.metrics.committed + writerMetrics.metrics.skipped)))
  }
  
  async finalize(cause) {
    // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.skipTable,this.dbi.TRANSACTION_IN_PROGRESS,this.writableEnded,this.writableFinished,this.destroyed,this.hasPendingRows(),this.metrics.received,this.metrics.committed,this.metrics.written,this.metrics.cached],'finalize()')
	// Wait for any pending writeBatch operations to complete before proceeding 
    await this.batchWritten;
   
    if (this.hasPendingRows() && !this.skipTable) {
	  const nextBatch = this.batch;
	  const rowsReceived = this.metrics.received
      const rowsCached = this.metrics.cached
      this.batchWritten = this.getBatchWritten(this.metrics.batchCount)
	  this.processBatch(nextBatch,rowsReceived,rowsCached).catch((e) => {console.log(e)})
 	}
	else {
	  this.metrics.lost+= this.metrics.cached
	  this.releaseBatch(this.batch)
	}

    // Ensure all batchOperations are complete
    await Promise.allSettled(Array.from(this.batchOperations))
	
	/*
	**
	** Handle any errors raised during the final processBatch() operations.
	**
	*/
	
	this.underlyingError = this.underlyingError || cause
    if (this.dbi.TRANSACTION_IN_PROGRESS === true) {
      if (this.skipTable === true) {
		await this.rollbackTransaction(this.underlyingError)
      }
      else {
        await this.commitTransaction()
      }
    }
	
  }   
         
  async __destroy() { /* Proivde implementation where nescessary * */  }
    
  async _destroy (err,callback)  {

	/*
	**
	** _destroy is called when the stream terminates as a result of an error
	**
	** The pipeline operation may terminate and throw an exception before _destroy has completed....
	**
	*/
	  
	// this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber()],`_destroy(${err ? err.message : 'Normal'})`)

    if (err && YadamuConstants.ABORT_CURRENT_TABLE.includes(this.dbi.ON_ERROR)) {
      this.abortTable()
	}	
    let source

	try {
      await this.__destroy()
      await this.finalize(err)
	} catch (e) {
      // Handle Error during Finalize
	  source = 'STREAM WRITER'
	  err = e 
	  this.underlyingError = e
	  this.yadamuLogger.handleException([`PIPELINE`,`STREAM WRITER`,this.SOURCE_VENDOR,this.dbi.DATABASE_VENDOR,`"${this.tableName}"`,this.dbi.ON_ERROR,this.dbi.getWorkerNumber()],e);
    }
    this.endTime = performance.now()
	try {
	  await this.ddlComplete
	  this.reportPerformance(err)
    } catch (ddlFailure) {
	  err = ddlFailure
	}
	callback(err)
  } 

  async __final() { /* Proivde implementation where nescessary * */  }
 
  async _final(callback) {
	  
	/*
	**
	** _final is called when the stream terminates without an error
	**
	*/
	
    // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber(),,this.metrics.received,this.metrics.cached],'_final()')
	await this.__final()
	let exception
	try {
      await this.destroy()
	} catch (err) {
      this.yadamuLogger.handleException([`PIPELINE`,`WRITER`,`FINAL`,this.SOURCE_VENDOR,this.dbi.DATABASE_VENDOR,`"${this.tableName}"`,this.dbi.ON_ERROR,this.dbi.getWorkerNumber()],err);
	  exception = err
    } 
	callback(exception)
  } 
    
}

module.exports = YadamuWriter;
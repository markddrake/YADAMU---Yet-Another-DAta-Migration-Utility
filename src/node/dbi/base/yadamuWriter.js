"use strict"

import fs              from 'fs'
import assert          from 'assert'
import { 
  performance 
}                      from 'perf_hooks';

import {
  Readable, 
  Writable, 
  Transform 
}                      from 'stream'

import YadamuConstants from '../../lib/yadamuConstants.js';
import YadamuLibrary   from '../../lib/yadamuLibrary.js';

import {YadamuError, BatchInsertError, IterativeInsertError, DatabaseError} from '../../core/yadamuException.js'

import DBIConstants    from './dbiConstants.js';


class YadamuWriter extends Writable {

  get COPY_METRICS()       { return this._COPY_METRICS }
  set COPY_METRICS(v)      { this._COPY_METRICS =  v }

  get SPATIAL_FORMAT()     { return this.tableInfo._SPATIAL_FORMAT }
  
  get PARTITIONED_TABLE()  { return this.tableInfo?.hasOwnProperty('partitionCount')}

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
	const options = {
      highWaterMark : 10
	, objectMode    : true
    }
	super(options)

    this.dbi = dbi;
    this.tableName = tableName
	this.displayName = this.tableName
    
	this.COPY_METRICS = metrics
    this.status = status;
    this.yadamuLogger = yadamuLogger;    
    
    this.sqlInitialTime = this.dbi.SQL_CUMULATIVE_TIME
    this.skipTable = this.dbi.MODE === 'DDL_ONLY';    

    const writeOperation = new Promise((resolve,reject) => {
      this.on('close',() => {
		 resolve()
		 this.dbi.activeWriters.delete(writeOperation)
	  })
	})
    this.dbi.activeWriters.add(writeOperation)	
	this.setNotWriting()
  }

  setTableInfo(tableName) {
    this.tableInfo = this.dbi.getTableInfo(tableName)
  }
  
  setNotWriting() {
	this.batchCompleted = new Promise((resolve,reject) => {
      resolve(DBIConstants.BATCH_IDLE)
	})
  }	
  
  setWriting() {
  	this.batchCompleted = new Promise((resolve,reject) => {
	  this.once(DBIConstants.BATCH_WRITTEN,() => {
		 this.setNotWriting()
		 resolve(DBIConstants.BATCH_WRITTEN)
	  }).once(DBIConstants.BATCH_FAILED,() => {
		 this.setNotWriting()
		 resolve(DBIConstants.BATCH_FAILED)
	  })
	})
  }  	  
  
  async initializeTable() {
    this.setTableInfo(this.tableName)	
  }
   
  isValidPartition(partitionInfo) {
	 return false
  }
  
  async initializePartition(partitionInfo) {	
	this.partitionInfo = partitionInfo
	this.displayName = partitionInfo.displayName
	await this.initializeTable()
	this.tableInfo.partitionCount = partitionInfo.partitionCount
	this.tableInfo.partitionsRemaining = this.tableInfo.partitionsRemaining || partitionInfo.partitionCount
  }
     
  releaseBatch(batch) {
	this.dbi.releaseBatch(batch)
  }
  
  abortTable() {
	  
    // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber()],'abortTable()')
	
	// Abort means that records cached will never be written
	// If a rollback occurs prior to abort, written is already added to lost and set to zero.
	// If abort occurs prior to rollback or abort is called multiple times written is already zeroed out.

    this.skipTable = true;
	this.COPY_METRICS.lost += this.COPY_METRICS.written;
	this.COPY_METRICS.skipped += (this.COPY_METRICS.pending - this.COPY_METRICS.cached);
	this.COPY_METRICS.pending = 0;
	this.COPY_METRICS.written = 0;
	this.COPY_METRICS.cached = 0;	
  }
  
  adjustRowCounts(rowCount) {
	this.COPY_METRICS.written+= rowCount
	this.COPY_METRICS.pending-= rowCount
  }
  
  async beginTransaction() {
    await this.dbi.beginTransaction();
  }
  
  async commitTransaction() {
    // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber(),this.COPY_METRICS.committed,this.COPY_METRICS.written],'commitTransaction()')
    if (!this.skipTable) {
      await this.dbi.commitTransaction()
      this.COPY_METRICS.committed += this.COPY_METRICS.written;
      this.COPY_METRICS.written = 0;
    }
  }
  
  async rollbackTransaction(cause) {
	 
	 // Rollback means that records written but not committed are lost
	 // Handing for in-flight records is dependant on the value of ON_ERROR.

      // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber()],'rollbackTransaction()')
	 
	  this.COPY_METRICS.lost += this.COPY_METRICS.written;
      this.COPY_METRICS.written = 0;
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
    
  lostRows() {
	return this.COPY_METRICS.lost > 0
  }
        
  createIterativeException(cause,batchSize,rowNumber,row,info) {
	 
	return YadamuError.createIterativeException(this.dbi,this.tableInfo,cause,batchSize,rowNumber,row,info)

  }
    
  async rejectRow(tableName,row) {
    // Allows the rejection process to be overridden by a particular driver.
    await this.dbi.yadamu.REJECTION_MANAGER.rejectRow(tableName,row);
  }
  
  handleIterativeError(operation,cause,rowNumber,record,info) {
	  
	if (this.lostRows()) {
	  throw cause
	}
	
    this.COPY_METRICS.skipped++;
	this.COPY_METRICS.pending--
    
    try {
      this.rejectRow(this.tableName,record);
      const iterativeError = this.createIterativeException(cause,this.COPY_METRICS.cached,rowNumber,record,info)
	  this.dbi.trackExceptions(iterativeError);
      this.yadamuLogger.logRejected([...(typeof cause.getTags === 'function' ? cause.getTags() : []),this.dbi.DATABASE_VENDOR,this.displayName,operation,this.COPY_METRICS.cached,rowNumber],iterativeError);
    } catch (e) {
      this.yadamuLogger.handleException([this.dbi.DATABASE_VENDOR,'ITERATIVE_ERROR',this.displayName,this.tableInfo.insertMode],e)
    }
    
    if (this.COPY_METRICS.skipped === this.dbi.TABLE_MAX_ERRORS) {
      this.yadamuLogger.error([this.dbi.DATABASE_VENDOR,this.displayName,this.tableInfo.insertMode],`Maximum Error Count exceeded. Skipping Table.`);
      this.abortTable()
    }
  }

  createBatchException(cause,batchSize,firstRow,lastRow,info) {

	return YadamuError.createBatchException(this.dbi,this.tableInfo,cause,this.BATCH_METRICS.batchNumber,batchSize,firstRow,lastRow,info)

  }
  
  reportBatchError(operation,cause,firstRow,lastRow,info) {
	 
    if (this.lostRows() || YadamuError.missingTable(cause)) {
	  throw cause
    }
	
    const batchException = this.createBatchException(cause,this.COPY_METRICS.cached,firstRow,lastRow,info)
	this.dbi.trackExceptions(batchException);
    this.yadamuLogger.handleWarning([...(cause.getTags?.() || []),this.dbi.DATABASE_VENDOR,this.displayName,operation,this.tableInfo.insertMode,this.BATCH_METRICS.batchNumber,this.BATCH_METRICS.cached],batchException)
  }
  
  handleSpatialError(operation,cause,rowNumber,record,info) {
      
    this.COPY_METRICS.skipped++;
    
    try {
      this.rejectRow(this.tableName,record);
      const iterativeError = this.createIterativeException(cause,this.COPY_METRICS.cached,rowNumber,record,info)
	  this.dbi.trackExceptions(iterativeError);
      this.yadamuLogger.logRejectedAsWarning([this.dbi.DATABASE_VENDOR,this.displayName,operation,this.BATCH_METRICS.cached,rowNumber],iterativeError);
    } catch (e) {
      this.yadamuLogger.handleException([this.dbi.DATABASE_VENDOR,'ITERATIVE_ERROR',this.displayName,this.tableInfo.insertMode],e)
    }

    
    if (this.COPY_METRICS.skipped === this.dbi.TABLE_MAX_ERRORS) {
      this.yadamuLogger.error([this.dbi.DATABASE_VENDOR,this.displayName,this.tableInfo.insertMode],`Maximum Error Count exceeded. Skipping Table.`);
      this.abortTable()
    }
  }
    
  async doConstruct() {
   
	// Workers need to reload their copy of the Statement Cache from the Manager before processing can begin
	// this.yadamuLogger.trace([this.constructor.name,'CACHE_LOADED',this.dbi.getWorkerNumber(),this.tableName],'WAITING')
    await this.dbi.cacheLoaded
    this.dbi.reloadStatementCache()
    // this.yadamuLogger.trace([this.constructor.name,'CACHE_LOADED',this.dbi.getWorkerNumber(),this.tableName],'PROCESSING')

    // Do not start processing data until all DDL operations have completed and a transaction has been started.	
	// ### Actually must not push() until DDL_COMPLETE, however if DDL operations are transactional, then we must not start the transaction until DDL Complete.
	
	// this.yadamuLogger.trace([this.constructor.name,'DLL_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'WAITING')
    await this.dbi.ddlComplete
    // this.yadamuLogger.trace([this.constructor.name,'DLL_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'PROCESSING')

	await this.beginTransaction()
	
  }
    
  _construct(callback) {
	  
	this.doConstruct().then(() => { callback() }).catch((err) => { callback(err) })

  }

  commitWork() {
    // While COMMIT is defined as a multiple of BATCH_SIZE some drivers may write smaller batches.
    return (((this.dbi.COMMIT_COUNT > 0) && (this.COPY_METRICS.written >= this.dbi.COMMIT_COUNT)) && !this.skipTable)
  }
	
  async processBatch(batch, snapshot) {
	  
	this.BATCH_METRICS = snapshot
	
	if (!this.skipTable) {
	  try {
	    this.skipTable = await this._writeBatch(batch,this.BATCH_METRICS.cached)
        if (this.skipTable) {
          await this.rollbackTransaction();
        }	                     
	    // Commit after a writing a batch if the Commit threshold has been reached or passed. Start a new Transaction
        if (this.commitWork(this.BATCH_METRICS.received)) {
		  try {
            await this.commitTransaction()
            if (this.dbi.REPORT_COMMITS) {
              this.yadamuLogger.info([`${this.displayName}`,this.tableInfo.insertMode],`Rows committed: ${this.COPY_METRICS.committed}.`);
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
		else {
          if (this.dbi.REPORT_BATCHES) {
            this.yadamuLogger.info([`${this.displayName}`,this.tableInfo.insertMode],`Rows written:  ${this.COPY_METRICS.written}.`);
          }
  	    }
	  } catch (err) { 

  	    /*
	    **
	    ** An unrecoverable error occured while writing a batch. Examples of unrecoverable errors include lost connections, missing tables, too many errors during iterative inserts
	    ** or anything else that causes the _writeBach implementation to throw an error. Also catches issues with Transaction State (Errors during COMMIT and/or BEGIN transaction operations)
        **
	    */
        if (this.lostRows()) {
          // ### Need to caculate lost rows correctly when TABLE_MAX_ERRORS exceeded
          if (YadamuConstants.ABORT_CURRENT_TABLE.includes(this.dbi.ON_ERROR)) {
	        // Aborting the current table (ABORT,SKIP) means that no more rows will be read and any records cached but not written will be lost. 
   	        this.releaseBatch(batch)
      	    this.abortTable()
		  }
		  else {
			this.COPY_METRICS.lost+= this.COPY_METRICS.pending
			this.COPY_METRICS.pending = 0
		  }
        }
		// If we cannot continue processing the table for any reason throw the err to abort the current pipeline operation.
		if (this.skipTable) {
	      throw err
		}
	  }
	}
    else {
	  // Entire Batch is skipped ### Log the batch ???
	  this.COPY_METRICS.lost += this.BATCH_METRICS.cached
	  this.releaseBatch(batch)
	}

  }

  async doWrite(obj) {
	
    // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,this.displayName || this.tableName,],`doWrite(${Object.keys(obj)[0]})`)
		
	switch (true) {
	  case obj.hasOwnProperty('batch'):
        // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,this.displayName,obj.this.BATCH_METRICS.received,obj.this.BATCH_METRICS.cached],'doWrite()')
	    this.COPY_METRICS.idleTime+= ( this.waitTime - performance.now() )  
		this.setWriting();
	    await this.processBatch(obj.batch,obj.snapshot)
		this.emit(DBIConstants.BATCH_WRITTEN)
		this.waitTime = performance.now()
		break
	  case obj.hasOwnProperty('table'):
	    await this.initializeTable()
		this.waitTime = performance.now()
		return
	  case obj.hasOwnProperty('partition'):
	    await this.initializePartition(obj.partition)
        break
      default:
	}
	   
  }

 
  _write(batch, encoding, callback) {
	 
    // this.yadamuLogger.trace([this.constructor.name,this.dbi.ROLE,this.displayName,this.dbi.getWorkerNumber(),this.COPY_METRICS.received,this.COPY_METRICS.cached,this.COPY_METRICS.written,this.COPY_METRICS.skipped,this.COPY_METRICS.lost,this.writableEnded,this.writableFinished],'YadamuWriter._write()')
    this.doWrite(batch).then(()=> { callback() }).catch((e) => { this.emit(DBIConstants.BATCH_FAILED); callback(e) })

  }
  
  setReader(reader) {
	this.reader = reader
  }

  recordPartitionMetrics() {

    if (this.tableInfo.partitionsRemaining == this.tableInfo.partitionCount) {
	  this.tableInfo.TABLE_METRICS = Object.assign({},this.COPY_METRICS)
	}
	else {
	  this.tableInfo.TABLE_METRICS.pipeStartTime = this.COPY_METRICS.pipeStartTime < this.tableInfo.TABLE_METRICS.pipeStartTime ? this.COPY_METRICS.pipeStartTime : this.tableInfo.TABLE_METRICS.pipeStartTime
	  this.tableInfo.TABLE_METRICS.writerEndTime = this.COPY_METRICS.writerEndTime > this.tableInfo.TABLE_METRICS.writerEndTime ? this.COPY_METRICS.writerEndTime : this.tableInfo.TABLE_METRICS.writerEndTime
	  this.tableInfo.TABLE_METRICS.read+=      this.COPY_METRICS.read
	  this.tableInfo.TABLE_METRICS.committed+= this.COPY_METRICS.committed
	  this.tableInfo.TABLE_METRICS.lost +=     this.COPY_METRICS.lost
	  this.tableInfo.TABLE_METRICS.skipped+=   this.COPY_METRICS.skipped
	  this.tableInfo.TABLE_METRICS.sqlTime+=   this.COPY_METRICS.sqlTime
	}
    this.tableInfo.partitionsRemaining--
  }
  
  reportPerformance(err) {
	
	this.COPY_METRICS.writerEndTime = performance.now()
	this.COPY_METRICS.read = this.COPY_METRICS.read || this.COPY_METRICS.parsed

    // console.log(this.COPY_METRICS)
	
	if (err) {
	  if (this.reader && this.reader.readableLength && !isNaN(this.reader.readableLength)) {
		// 'readable-stream' based Readble implementations (e.g. MySQL) may not maintain this value.
        this.COPY_METRICS.read  += this.reader.readableLength
	  }
      this.COPY_METRICS.skipped += this.COPY_METRICS.cached   // .cached will be zero following AbortTable()
	  this.COPY_METRICS.cached  = 0
      this.COPY_METRICS.skipped += this.COPY_METRICS.pending  // .pending will be zero if all batches have been written
	  this.COPY_METRICS.pending = 0
	  this.COPY_METRICS.lost    += this.COPY_METRICS.written  // .written will be zero following a commit or rollback transaction or following AbortTable()
	  this.COPY_METRICS.written = 0
      this.COPY_METRICS.skipped += (this.COPY_METRICS.read - this.COPY_METRICS.parsed) 
	  this.COPY_METRICS.skipped += (this.COPY_METRICS.parsed - this.COPY_METRICS.received)
    }

	this.COPY_METRICS.sqlTime       = this.dbi.SQL_CUMULATIVE_TIME - this.sqlInitialTime
    this.COPY_METRICS.insertMode    = this.COPY_METRICS.insertMode || this.tableInfo?.insertMode || 'DDL Error'
    this.COPY_METRICS.skipTable     = this.skipTable
 	    
	const readElapsedTime = this.COPY_METRICS.parserEndTime - this.COPY_METRICS.readerStartTime;
    const writerElapsedTime = this.COPY_METRICS.writerEndTime - this.COPY_METRICS.writerStartTime;        
    const pipeElapsedTime = this.COPY_METRICS.writerEndTime - this.COPY_METRICS.pipeStartTime;
    const readThroughput = isNaN(readElapsedTime) ? 'N/A' : Math.round((this.COPY_METRICS.read/readElapsedTime) * 1000)
    const writerThroughput = isNaN(writerElapsedTime) ? 'N/A' : Math.round((this.COPY_METRICS.committed/writerElapsedTime) * 1000)
    
    let readStatus = ''
    let rowCountSummary = ''
    
    const readerTimings = `Reader Elapsed Time: ${YadamuLibrary.stringifyDuration(readElapsedTime)}s. Throughput ${Math.round(readThroughput)} rows/s.`
    const writerTimings = `Writer Elapsed Time: ${YadamuLibrary.stringifyDuration(writerElapsedTime)}s.  Idle Time: ${YadamuLibrary.stringifyDuration(this.COPY_METRICS.idleTime)}s. SQL Exection Time: ${YadamuLibrary.stringifyDuration(Math.round(this.COPY_METRICS.sqlTime))}s. Throughput: ${Math.round(writerThroughput)} rows/s.`
    
    if ((this.COPY_METRICS.read === 0) || (this.COPY_METRICS.read === this.COPY_METRICS.committed)) {
      rowCountSummary = `Rows ${this.COPY_METRICS.read}.`
    }
    else {
      rowCountSummary = `Read ${this.COPY_METRICS.read}. Written ${this.COPY_METRICS.committed}.`
    }
    rowCountSummary = this.COPY_METRICS.skipped > 0 ? `${rowCountSummary} Skipped ${this.COPY_METRICS.skipped}.` : rowCountSummary
    rowCountSummary = this.COPY_METRICS.lost > 0 ? `${rowCountSummary} Lost ${this.COPY_METRICS.lost}.` : rowCountSummary
    rowCountSummary = (this.dbi.yadamu.QA_TEST && this.COPY_METRICS.receivedOoS > 0) ? `${rowCountSummary} [Out of Sequence ${this.COPY_METRICS.receivedOoS}].` : rowCountSummary
    
    const cause = this.COPY_METRICS.readerError || this.COPY_METRICS.parserError ||  this.underlyingError || err
	if (cause) {
	  const tags = YadamuError.lostConnection(cause) ? ['LOST CONNECTION'] : []
      tags.push(this.COPY_METRICS.readerError || this.COPY_METRICS.parserError ? 'STREAM READER' : 'STREAM WRITER')
	  this.yadamuLogger.handleException(['PIPELINE',...tags,this.displayName,this.COPY_METRICS.SOURCE_DATABASE_VENDOR,this.dbi.DATABASE_VENDOR,this.dbi.ON_ERROR,this.dbi.getWorkerNumber()],cause)
	}
	
	if (this.COPY_METRICS.failed) {
      rowCountSummary = ((this.tableInfo === undefined) || (this.COPY_METRICS.tableNotFound === true)) ? `Table not found.` : `Read operation failed. ${rowCountSummary} ` 
      this.yadamuLogger.error([`${this.displayName}`,`${this.COPY_METRICS.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
    }
    else {
	  switch (true) {
		case (this.COPY_METRICS.read == this.COPY_METRICS.committed):
          this.yadamuLogger.info([`${this.displayName}`,`${this.COPY_METRICS.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
		  break
	    case (this.COPY_METRICS.read === (this.COPY_METRICS.committed + this.COPY_METRICS.skipped)):
          this.yadamuLogger.warning([`${this.displayName}`,`${this.COPY_METRICS.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
		  break
		default:
          this.yadamuLogger.error([`${this.displayName}`,`${this.COPY_METRICS.insertMode}`],`${rowCountSummary} ${readerTimings} ${writerTimings}`)  
      }
    }     
	
    if (this.PARTITIONED_TABLE) {
	  this.recordPartitionMetrics()
	  if (this.tableInfo.partitionsRemaining === 0) {
		const summary = this.dbi.yadamu.recordMetrics(this.tableName,this.tableInfo.TABLE_METRICS);  
	    const timings = `Writer Elapsed Time: ${YadamuLibrary.stringifyDuration(summary.elapsedTime)}s. SQL Exection Time: ${YadamuLibrary.stringifyDuration(Math.round(this.tableInfo.TABLE_METRICS.sqlExecutionTime))}s. Throughput: ${summary.throughput} rows/s.`
	    this.yadamuLogger.info([`${this.tableName}`,`${this.COPY_METRICS.insertMode}`],`Total Rows ${this.tableInfo.TABLE_METRICS.committed}. ${timings}`)  
	  }
    }
    else {
      this.dbi.yadamu.recordMetrics(this.tableName,this.COPY_METRICS)
	}
	
    return (this.COPY_METRICS.failed || (this.COPY_METRICS.read !== (this.COPY_METRICS.committed + this.COPY_METRICS.skipped)))
  }
  
  async endTransaction(err) {
	  
	if (this.dbi.TRANSACTION_IN_PROGRESS === true) {
      if (this.skipTable === true) {
		await this.rollbackTransaction(err)
      }
      else {
        await this.commitTransaction()
      }
    }
  }
  
  async doFinal() {

    // this.yadamuLogger.trace([this.constructor.name,'doFinal()','BATCH_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'WAITING')
    await this.batchCompleted
    // this.yadamuLogger.trace([this.constructor.name,'doFinal()','BATCH_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'PROCESSING')

    await this.endTransaction()
    this.reportPerformance()
  }

  _final(callback) {

    // this.yadamuLogger.trace([this.constructor.name,this.dbi.ROLE,this.displayName,this.dbi.getWorkerNumber(),this.COPY_METRICS.received,this.COPY_METRICS.cached,this.COPY_METRICS.written,this.COPY_METRICS.skipped,this.COPY_METRICS.lost,this.writableEnded,this.writableFinished],'YadamuWriter._final()')
    this.doFinal().then(() => { callback() }).catch((e) => { callback(e) })
	
  }
  
  async doDestroy(err) {
    if (err) {
	  this.COPY_METRICS.failed = true
      this.COPY_METRICS.writerError = err
      this.COPY_METRICS.writerEndTime = performance.now()	  
	  try {
        // this.yadamuLogger.trace([this.constructor.name,'doDestroy()','BATCH_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'WAITING')
		await this.batchCompleted
        // this.yadamuLogger.trace([this.constructor.name,'doDestroy()','BATCH_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'PROCESSING')
	    await this.endTransaction(err)
        this.reportPerformance(err)
	  }
	  catch (e) {
		e.rootCause = err
        this.reportPerformance(e)
		throw e
	  }
	}

  }

  _destroy(err,callback)  {
	
    // this.yadamuLogger.trace([this.constructor.name,this.dbi.ROLE,this.displayName,this.dbi.getWorkerNumber(),this.writableLength],`YadamuWriter._destroy(${err ? err.message : 'Normal'})`)
    this.doDestroy(err).then(() => { callback(err) }).catch((e) => { e.rootCause = err; callback(e) })
	
  }

  _writeBatch(batch,cached) {
  
    /*
	**
	**
	
    Writes a batch of records to the database using an appropriate multirow insert.
	If the multi-row insert fails, should fall back to an interative approach.
	
	Throw an exception under the follwing conditions
	 - The implementer forgets to implement this method :)
	 - The target table cannot be found
	 - The Maximum Number of iterative errors threshold is exceeded
	 - The connection fails and rows have been lost e.g. rows have been written successfully but not yet committed and the ON_ERROR option is ABORT or SKIP
	   Errors encountered during Begin Transaction and CREATE SAVE POINT operations errors are retryable
	   Errors encountered during Commit Transaction records are not retryable.
	   Rows written but not committed are treated as LOST if a commit operations fails. 
	   Save Point errors are treated as recoverable but may lead to unrecoverable errors during subsequent commit or rollback operations
	   Rows from prior batches that have not been committed are not recoverable. Setting COMMIT_RATIO to 1 prevents multiple batch transactions.
	   Currently rows from the current batch are considered unreocverable in the event of a rollback. 
	   
	 
	 It is critical that the method throws in these circumstances so that the _write callback is invoked with the exception.
	 This ensures that the pipeline terminates and additional records are not processed.
	 
	 Note that when the ON_ERROR behavoir is FLUSH _writeBatch should not throw unless it is absolutely certain that it will not be possible to
	 process addtional batches of records. Lost records are an acceptable consequence of under FLUSH.
	 
	 LOST rows are not skipped records, lost records are records written to the database but not committed at the point an unrecoverable error occurred.

    **
    */

    throw new UnimplementedMethod('_writeBatch()',`YadamuWriter`,this.constructor.name)
  }

}

export { YadamuWriter as default}

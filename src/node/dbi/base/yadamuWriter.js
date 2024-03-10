
import fs                from 'fs'
import assert            from 'assert'

import { 
  performance 
}                        from 'perf_hooks';

import {
  Readable, 
  Writable, 
  Transform 
}                        from 'stream'

import DBIConstants      from './dbiConstants.js';

import YadamuConstants   from '../../lib/yadamuConstants.js';
import YadamuLibrary     from '../../lib/yadamuLibrary.js';

import {
  YadamuError, 
  BatchInsertError, 
  IterativeInsertError, 
  DatabaseError
}                        from '../../core/yadamuException.js'


class YadamuWriter extends Writable {

  get LOGGER()             { return this._LOGGER }
  set LOGGER(v)            { this._LOGGER = v }
  get DEBUGGER()           { return this._DEBUGGER }
  set DEBUGGER(v)          { this._DEBUGGER = v }

  get PIPELINE_STATE()       { return this._PIPELINE_STATE }
  set PIPELINE_STATE(v)      { this._PIPELINE_STATE =  v }

  get STREAM_STATE()       { return this.PIPELINE_STATE[DBIConstants.OUTPUT_STREAM_ID] }
  set STREAM_STATE(v)      { this.PIPELINE_STATE[DBIConstants.OUTPUT_STREAM_ID] = v }

  get SPATIAL_FORMAT()     { return this.tableInfo._SPATIAL_FORMAT }
  
  get PARTITIONED_TABLE()  { return this.tableInfo?.hasOwnProperty('partitionCount')}
  
  set SQL_START_TIME(v)    { this._SQL_START_TIME = v }
  get SQL_START_TIME()     { return this._SQL_START_TIME }
   
  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {

    const options = {
      highWaterMark : dbi.BATCH_LIMIT
    , objectMode    : true
    }
    super(options)

    this.dbi = dbi;
    
    this.PIPELINE_STATE = pipelineState
    this.STREAM_STATE = { vendor : dbi.DATABASE_VENDOR }
    this.status   = status;
    this.LOGGER   = yadamuLogger || this.dbi.LOGGER
    this.DEBUGGER = this.dbi.DEBUGGER
    
    this.tableName = tableName
    // this.PIPELINE_STATE.tableName = this.tableName
    this.PIPELINE_STATE.displayName = this.tableName
        
    this.SQL_START_TIME = this.dbi.SQL_CUMLATIVE_TIME
    
    this.skipTable = this.dbi.MODE === 'DDL_ONLY';    

    const writeOperation = new Promise((resolve,reject) => {
      this.on('close',() => {
         resolve()
         this.dbi.activeWriters.delete(writeOperation)
      })
      this.on('error',(e) => {
         // Rejecting here will cause unhandled exception....
         // reject(e)
         resolve(e)
         this.dbi.activeWriters.delete(writeOperation)
      })
    })
    
    this.dbi.activeWriters.add(writeOperation)   
        
    this.on('pipe',(src) => {
      this.batchManager = src
    })

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
      this.once(DBIConstants.BATCH_COMPLETED,(result) => {
         this.setNotWriting()
         resolve(result)
      })
    })
  }       
  
  rowsLost() {
	return this.PIPELINE_STATE.lost > 0
  }

  async initializeTable() {
    this.setTableInfo(this.tableName)   

    // Support TRUNCATE_BEFORE_LOAD
      
    if (this.dbi.yadamu.parameters.TRUNCATE_ON_LOAD === true) {
      await this.dbi.truncateTable(this.dbi.CURRENT_SCHEMA,this.tableName)
    }
  }
   
  isValidPartition(partitionInfo) {
     return false
  }
  
  async initializePartition(partitionInfo) {    
    this.partitionInfo = partitionInfo
    this.PIPELINE_STATE.displayName = partitionInfo.displayName
    this.PIPELINE_STATE.partitionCount = partitionInfo.partitionCount
    this.PIPELINE_STATE.partitionedTableName = this.tableName
    await this.initializeTable()
    this.tableInfo.partitionCount = partitionInfo.partitionCount
    this.tableInfo.partitionsRemaining = this.tableInfo.partitionsRemaining || partitionInfo.partitionCount
  }
     
  releaseBatch(batch) {
    this.batchManager.releaseBatch(batch)
  }
  
  abortTable() {
      
    // this.LOGGER.trace([this.constructor.name,this.PIPELINE_STATE.displayName,this.dbi.getWorkerNumber()],'abortTable()')
    
    // Abort means that records cached will never be written
    // If a rollback occurs prior to abort, written is already added to lost and set to zero.
    // If abort occurs prior to rollback or abort is called multiple times written is already zeroed out.

    this.skipTable = true;
    this.PIPELINE_STATE.lost += this.PIPELINE_STATE.written;
    this.PIPELINE_STATE.skipped += (this.PIPELINE_STATE.pending + this.PIPELINE_STATE.cached);
    this.PIPELINE_STATE.pending = 0;
    this.PIPELINE_STATE.written = 0;
    this.PIPELINE_STATE.cached = 0; 
  }
  
  adjustRowCounts(rowCount) {
    this.PIPELINE_STATE.written+= rowCount
    this.PIPELINE_STATE.pending-= rowCount
  }
  
  async beginTransaction() {
    await this.dbi.beginTransaction();
  }
  
  async commitTransaction() {
    // this.LOGGER.trace([this.constructor.name,this.PIPELINE_STATE.displayName,this.dbi.getWorkerNumber(),this.PIPELINE_STATE.committed,this.PIPELINE_STATE.written],`commitTransaction(${this.skipTable})`)
    if (!this.skipTable) {
      await this.dbi.commitTransaction()
      this.PIPELINE_STATE.committed += this.PIPELINE_STATE.written;
      this.PIPELINE_STATE.written = 0;
    }
  }
  
  async rollbackTransaction(cause) {
     
     // Rollback means that records written but not committed are lost
     // Handing for in-flight records is dependant on the value of ON_ERROR.

      // this.LOGGER.trace([this.constructor.name,this.PIPELINE_STATE.displayName,this.dbi.getWorkerNumber()],'rollbackTransaction()')
     
      this.PIPELINE_STATE.lost += this.PIPELINE_STATE.written;
      this.PIPELINE_STATE.written = 0;
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
    return this.PIPELINE_STATE.lost > 0
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
    
    this.PIPELINE_STATE.skipped++;
    this.PIPELINE_STATE.pending--
    
    try {
      this.rejectRow(this.tableName,record);
      const iterativeError = this.createIterativeException(cause,this.PIPELINE_STATE.cached,rowNumber,record,info)
      this.dbi.trackExceptions(iterativeError);
      this.LOGGER.logRejected([...(typeof cause.getTags === 'function' ? cause.getTags() : []),this.dbi.DATABASE_VENDOR,this.PIPELINE_STATE.displayName,operation,this.PIPELINE_STATE.cached,rowNumber],iterativeError);
    } catch (e) {
      this.LOGGER.handleException([this.dbi.DATABASE_VENDOR,'ITERATIVE_ERROR',this.PIPELINE_STATE.displayName,this.tableInfo.insertMode],e)
    }
    
    if (this.PIPELINE_STATE.skipped === this.dbi.TABLE_MAX_ERRORS) {
      this.LOGGER.error([this.dbi.DATABASE_VENDOR,this.PIPELINE_STATE.displayName,this.tableInfo.insertMode],`Maximum Error Count exceeded. Skipping Table.`);
      this.abortTable()
    }
  }

  createBatchException(cause,batchSize,firstRow,lastRow,info) {

    return YadamuError.createBatchException(this.dbi,this.tableInfo,cause,this.BATCH_SNAPSHOT.batchNumber,batchSize,firstRow,lastRow,info)

  }
  
  reportBatchError(operation,cause,firstRow,lastRow,info) {
     
    if (this.lostRows() || YadamuError.missingTable(cause)) {
      throw cause
    }
    
    const batchException = this.createBatchException(cause,this.PIPELINE_STATE.cached,firstRow,lastRow,info)
    this.dbi.trackExceptions(batchException);
    this.LOGGER.handleWarning([...(cause.getTags?.() || []),this.dbi.DATABASE_VENDOR,this.PIPELINE_STATE.displayName,operation,this.tableInfo.insertMode,this.BATCH_SNAPSHOT.batchNumber,this.BATCH_SNAPSHOT.cached],batchException)
  }
  
  handleSpatialError(operation,cause,rowNumber,record,info) {
      
    this.PIPELINE_STATE.skipped++;
    this.PIPELINE_STATE.pending--
    
    try {
      this.rejectRow(this.tableName,record);
      const iterativeError = this.createIterativeException(cause,this.PIPELINE_STATE.cached,rowNumber,record,info)
      this.dbi.trackExceptions(iterativeError);
      this.LOGGER.logRejectedAsWarning([this.dbi.DATABASE_VENDOR,this.PIPELINE_STATE.displayName,operation,this.BATCH_SNAPSHOT.cached,rowNumber],iterativeError);
    } catch (e) {
      this.LOGGER.handleException([this.dbi.DATABASE_VENDOR,'ITERATIVE_ERROR',this.PIPELINE_STATE.displayName,this.tableInfo.insertMode],e)
    }

    
    if (this.PIPELINE_STATE.skipped === this.dbi.TABLE_MAX_ERRORS) {
      this.LOGGER.error([this.dbi.DATABASE_VENDOR,this.PIPELINE_STATE.displayName,this.tableInfo.insertMode],`Maximum Error Count exceeded. Skipping Table.`);
      this.abortTable()
    }
  }
    
  async doConstruct() {

    this.STREAM_STATE.startTime = performance.now()
   
    // Workers need to reload their copy of the Statement Cache from the Manager before processing can begin
    // this.LOGGER.trace([this.constructor.name,'CACHE_LOADED',this.dbi.getWorkerNumber(),this.tableName],'WAITING')
    await this.dbi.cacheLoaded
    this.dbi.reloadStatementCache()
    // this.LOGGER.trace([this.constructor.name,'CACHE_LOADED',this.dbi.getWorkerNumber(),this.tableName],'PROCESSING')

    // Do not start processing data until all DDL operations have completed and a transaction has been started. 
    // ### Actually must not push() until DDL_COMPLETE, however if DDL operations are transactional, then we must not start the transaction until DDL Complete.
    
    // this.LOGGER.trace([this.constructor.name,'DLL_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'WAITING')
    await Promise.allSettled([this.dbi.ddlComplete,this.dbi.isReadyForData()])
    this.PIPELINE_STATE.ddlComplete = performance.now();
    // this.LOGGER.trace([this.constructor.name,'DLL_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'PROCESSING')
    
    await this.beginTransaction()
    
  }
    
  _construct(callback) {
      
    this.doConstruct().then(() => { callback() }).catch((err) => { callback(err) })

  }

  commitWork() {
    // While COMMIT is defined as a multiple of BATCH_SIZE some drivers may write smaller batches.
    return (((this.dbi.COMMIT_COUNT > 0) && (this.PIPELINE_STATE.written >= this.dbi.COMMIT_COUNT)) && !this.skipTable)
  }
    
  async processBatch(batch, snapshot) {
      
    this.BATCH_SNAPSHOT = snapshot
    
    if (!this.skipTable) {
      try {
        this.skipTable = await this._writeBatch(batch,this.BATCH_SNAPSHOT.cached)
        this.PIPELINE_STATE.batchWritten++
        if (this.skipTable) {
          await this.rollbackTransaction();
        }                        
        // Commit after a writing a batch if the Commit threshold has been reached or passed. Start a new Transaction
        if (this.commitWork(this.BATCH_SNAPSHOT.received)) {
          try {
            await this.commitTransaction()
            if (this.dbi.REPORT_COMMITS) {
              this.LOGGER.info([`${this.PIPELINE_STATE.displayName}`,this.tableInfo.insertMode],`Rows committed: ${this.PIPELINE_STATE.committed}.`);
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
            this.LOGGER.info([`${this.PIPELINE_STATE.displayName}`,this.tableInfo.insertMode],`Rows written:  ${this.PIPELINE_STATE.written}.`);
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
			if (this.skipTable) {
              this.PIPELINE_STATE.lost+= this.PIPELINE_STATE.pending
              this.PIPELINE_STATE.pending = 0
			}
          }
        }
        // If we cannot continue processing the table for any reason throw the err to abort the current pipeline operation.
        if (this.skipTable) {
          throw err
        }
        else {
          this.LOGGER.handleException([this.dbi.DATABASE_VENDOR,this.PIPELINE_STATE.displayName,this.tableInfo.insertMode],err)
        }
      }
    }
    else {
      // Entire Batch is skipped ### Log the batch ???
      this.PIPELINE_STATE.lost += this.BATCH_SNAPSHOT.cached
      this.releaseBatch(batch)
    }

  }

  async doWrite(obj) {
    
    // this.LOGGER.trace([this.dbi.DATABASE_VENDOR,this.PIPELINE_STATE.displayName || this.tableName,],`doWrite(${Object.keys(obj)[0]})`)
        
    switch (true) {
      case obj.hasOwnProperty('batch'):
        // this.LOGGER.trace([this.dbi.DATABASE_VENDOR,this.PIPELINE_STATE.displayName,obj.this.BATCH_SNAPSHOT.received,obj.this.BATCH_SNAPSHOT.cached],'doWrite()')
        this.PIPELINE_STATE.idleTime+= ( this.waitTime - performance.now() )  
        this.setWriting();
        await this.processBatch(obj.batch,obj.snapshot)
        this.emit(DBIConstants.BATCH_COMPLETED,DBIConstants.BATCH_WRITTEN)
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
     
    // this.LOGGER.trace([this.constructor.name,this.dbi.ROLE,this.PIPELINE_STATE.displayName,this.dbi.getWorkerNumber(),this.PIPELINE_STATE.received,this.PIPELINE_STATE.cached,this.PIPELINE_STATE.written,this.PIPELINE_STATE.skipped,this.PIPELINE_STATE.lost,this.writableEnded,this.writableFinished],'YadamuWriter._write()')
    this.doWrite(batch).then(()=> { 
      callback() 
    }).catch((e) => {
      this.STREAM_STATE.error = e
      this.emit(DBIConstants.BATCH_COMPLETED,DBIConstants.BATCH_FAILED); 
      callback(e) 
    })

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

    // this.LOGGER.trace([this.constructor.name,'doFinal()','BATCH_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'WAITING')
    await this.batchCompleted
    // this.LOGGER.trace([this.constructor.name,'doFinal()','BATCH_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'PROCESSING')

    await this.endTransaction()
  }

  _final(callback) {

    // this.LOGGER.trace([this.constructor.name,this.dbi.ROLE,this.PIPELINE_STATE.displayName,this.dbi.getWorkerNumber(),this.PIPELINE_STATE.received,this.PIPELINE_STATE.cached,this.PIPELINE_STATE.written,this.PIPELINE_STATE.skipped,this.PIPELINE_STATE.lost,this.writableEnded,this.writableFinished],'YadamuWriter._final()')
    this.doFinal().then(() => { callback() }).catch((e) => { callback(e) })
    
  }
  
  async doDestroy(err) {

    // this.LOGGER.trace([this.constructor.name,this.PIPELINE_STATE.displayName,this.dbi.getWorkerNumber(),this.PIPELINE_STATE.received,this.PIPELINE_STATE.cached,this.PIPELINE_STATE.written,this.PIPELINE_STATE.skipped,this.PIPELINE_STATE.lost,this.dbi.activeWriters.size],'doDestroy()')

    this.STREAM_STATE.endTime        = performance.now()
    this.STREAM_STATE.readableLength = this.readableLength || 0
    this.STREAM_STATE.writableLength = this.writableLength || 0
    this.PIPELINE_STATE.insertMode   = this.tableInfo ? this.tableInfo.insertMode : this.dbi.insertMode
    this.PIPELINE_STATE.skipTable    = this.tableInfo.skipTable
    this.PIPELINE_STATE.sqlTime      = this.dbi.SQL_CUMLATIVE_TIME - this.SQL_START_TIME

    if (err) {
      this.PIPELINE_STATE.failed = true
      this.PIPELINE_STATE.errorSource = this.PIPELINE_STATE.errorSource || DBIConstants.OUTPUT_STREAM_ID
      this.STREAM_STATE.error = this.STREAM_STATE.error || (this.PIPELINE_STATE.errorSource === DBIConstants.OUTPUT_STREAM_ID) ? err : this.STREAM_STATE.error
	  err.pipelineState = YadamuError.clonePipelineState(this.PIPELINE_STATE)
      
      try {
        // this.LOGGER.trace([this.constructor.name,'doDestroy()','BATCH_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'WAITING')
        await this.batchCompleted
        // this.LOGGER.trace([this.constructor.name,'doDestroy()','BATCH_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'PROCESSING')
        await this.endTransaction(err)
      }
      catch (e) {
        e.rootCause = err
        this.LOGGER.handleException(['PIPELINE',this.PIPELINE_STATE.displayName,this.PIPELINE_STATE[DBIConstants.INPUT_STREAM_ID].vendor,this.dbi.DATABASE_VENDOR,this.dbi.ON_ERROR,this.dbi.getWorkerNumber()],e)
        throw e
      }
    }
  }

  _destroy(err,callback)  {
    
    // this.LOGGER.trace([this.constructor.name,this.dbi.ROLE,this.PIPELINE_STATE.displayName,this.dbi.getWorkerNumber(),this.writableLength],`YadamuWriter._destroy(${err ? err.message : 'Normal'})`)
    
    this.doDestroy(err).then(() => { 
      callback(err) 
    }).catch((e) => { 
      e.rootCause = err; 
      callback(e) 
    })
    
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

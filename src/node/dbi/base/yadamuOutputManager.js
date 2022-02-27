"use strict"

import fs              from 'fs'
import assert          from 'assert'
import { performance } from 'perf_hooks';

import {Readable, Writable, Transform, pipeline } from 'stream'

import YadamuLibrary   from '../../lib/yadamuLibrary.js';
import YadamuConstants from '../../lib/yadamuConstants.js';

import {YadamuError, BatchInsertError, IterativeInsertError, DatabaseError, InvalidMessageSequence} from '../../core/yadamuException.js'

class YadamuOutputManager extends Transform {

  get BATCH_SIZE()         { return this.tableInfo._BATCH_SIZE }
  get SPATIAL_FORMAT()     { return this.tableInfo._SPATIAL_FORMAT }
  get SOURCE_VENDOR()      { return this.COPY_METRICS?.DATABASE_VENDOR || 'YABASC'  }
  
  get COPY_METRICS()       { return this._COPY_METRICS }
  set COPY_METRICS(v)      { this._COPY_METRICS =  v }

  get PARTITIONED_TABLE()  { return this.tableInfo?.hasOwnProperty('partitionCount')}    
  
  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    const options = {
	  objectMode             : true
	// , readableHighWaterMark  : 1024
	// , writableHighWaterMark  : 16
	}
    super(options)
    this.dbi = dbi;
    this.tableName = tableName
	this.displayName = tableName
	this.COPY_METRICS = metrics
	this.status = status;
    this.yadamuLogger = yadamuLogger;    

    this.startTime = performance.now();
	this.endTime = undefined
    this.batchOperations = new Set()

    this.skipTable = this.dbi.MODE === 'DDL_ONLY';    
	// this.processRow = this._invalidMessageSequence
	this.processRow = this._cacheOutOfSequenceMessages
    this.outOfSequenceMessageCache = []
  }
  
  newBatch() {
	this.COPY_METRICS.cached = 0;
    this.COPY_METRICS.batchNumber++;
	return this.dbi.newBatch()
  }
  
  releaseBatch(batch) {
	this.dbi.releaseBatch(batch)
  }

  async setTableInfo(tableName) {
	await this.dbi.cacheLoaded
    this.tableInfo = this.dbi.getTableInfo(tableName)
    this.batch = this.newBatch()
	this.skipTable = this.dbi.MODE === 'DDL_ONLY';	
	this.rowTransformation  = this.setTransformations(this.tableInfo.targetDataTypes)
  }
     
  generateTransformations(targetDataTypes) {
	
	return Array(targetDataTypes.columnNames.length).fill(null);

  }
  
  setTransformations(targetDataTypes) {
	 	  
	this.transformations = this.generateTransformations(targetDataTypes) 
	
     // Use a dummy rowTransformation function if there are no transformations required.

	return this.transformations.every((currentValue) => { return currentValue === null})
	? (row) => {}
	: (row) => {
      this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          row[idx] = transformation(row[idx],idx)
        }
      }) 
    }

  }
   
  abortTable() {
	// this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber()],'abortTable()')
	
    this.skipTable = true;
	
	// Abort means that records written but not committed are lost
	// If rollback occurs prior to abort, written is already added to lost and set to zero.
	// If abort occurs prior to rollback or abort is called multiple times written is already zeroed out.

    this.COPY_METRICS.skipped += this.COPY_METRICS.cached;
    this.COPY_METRICS.cached = 0;	  

    // Disable the processRow() function.
    this.processRow = this._skipRow
	
  }
 
  rowsLost() {
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
	  
	if (this.rowsLost()) {
	  throw cause
	}
	
    this.COPY_METRICS.skipped++;
    
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

  flushBatch() {
    return ((this.COPY_METRICS.cached === this.BATCH_SIZE) && !this.skipTable)
  }
  
  batchRowCount() {
    return this.COPY_METRICS.cached
  }
  
  hasPendingRows() {
    return ((this.COPY_METRICS.cached > 0) && !this.skipTable)
  }
    
  async rejectRow(tableName,row) {
    // Allows the rejection process to be overridden by a particular driver.
    await this.dbi.yadamu.REJECTION_MANAGER.rejectRow(tableName,row);
  }
  
  checkColumnCount(row) {
    try {
      if (!this.skipTable) {
        assert.strictEqual(row.length,this.tableInfo.columnCount,`Table ${this.tableName}. Incorrect number of columns supplied.`)
      }
    } catch (cause) {
	  const info = this.tableInfo === undefined  ? this.tableName : this.tableInfo
      this.handleIterativeError('CACHE',cause,this.COPY_METRICS.received+1,row,info);
    }
  }
              
  cacheRow(row) {
	  
    // Apply transformations and cache transformed row.
    
    // Use forEach not Map as transformations are not required for most columns. 
    // Avoid uneccesary data copy at all cost as this code is executed for every column in every row.

    // this.yadamuLogger.trace([this.constructor.name,'YADAMU WRITER',this.COPY_METRICS.cached],'cacheRow()')    
      
    this.rowTransformation(row)
    this.batch.push(row);
    this.COPY_METRICS.cached++
    return this.skipTable;
  }  

  async _cacheOutOfSequenceMessages(data) {
	if (this.outOfSequenceMessageCache.length < 10) {
      this.outOfSequenceMessageCache.push(data)
	}
	else {
	  this._invalidMessageSequence()
	}
  }

  processOutOfSequenceMessages() {

    if (this.outOfSequenceMessageCache.length > 0) {
  	  // this.yadamuLogger.qa(['WARNING',this.tableName,`INCORRECT MESSAGE SEQUENCE`],`Encounted ${this.outOfSequenceMessageCache.length} out of sequence 'data' messages prior to receiving 'table' message.`)  
	  this.COPY_METRICS.receivedOoS = this.outOfSequenceMessageCache.length
	}
	
    for (const data of this.outOfSequenceMessageCache) {
	  this.processRow(data)
	}
	
	// this.outOfSequenceMessageCache.length = 0
  }
  
  async _invalidMessageSequence(data) {
	throw new InvalidMessageSequence(this.tableName,'data','table')
  }
  
  async _skipRow(data) {
  }
  
  async _processRow(data) {
    // Be very careful about adding unecessary code here. This is executed once for each row processed by YADAMU. Keep it as lean as possible.
    this.checkColumnCount(data)
    this.cacheRow(data)
    this.COPY_METRICS.received++;
	if (this.flushBatch()) {
      if (this.dbi.REPORT_BATCHES) {
        this.yadamuLogger.info([`${this.displayName}`,this.tableInfo.insertMode],`Rows written:  ${this.COPY_METRICS.written}.`);
	  }
	  this.COPY_METRICS.pending+= this.COPY_METRICS.cached
	  this.push({batch:this.batch, snapshot: Object.assign({}, this.COPY_METRICS)})
      this.batch = this.newBatch();	
    }	  
    else {
	  if ((this.dbi.FEEDBACK_MODEL === 'ALL') && (this.COPY_METRICS.received % this.dbi.FEEDBACK_INTERVAL === 0)) {
        this.yadamuLogger.info([`${this.displayName}`,this.tableInfo.insertMode],`Rows Cached: ${this.COPY_METRICS.cached}.`);
      }
	}
  }
  
 
  async endTable() {}
        
  async doConstruct() {}
 
  _construct(callback) {

	this.doConstruct().then(() => { callback() }).catch((err) => { callback(err)  })
	
  }
  
  async initializeTable() {
    await this.setTableInfo(this.tableName)
    this.processRow =  this._processRow
	this.processOutOfSequenceMessages()
  }
 
  async initializePartition(partitionInfo) {
	this.partitionInfo = partitionInfo
	this.displayName = partitionInfo.displayName
	await this.initializeTable()
	this.tableInfo.partitionCount = partitionInfo.partitionCount
	this.tableInfo.partitionsRemaining = this.tableInfo.partitionsRemaining || partitionInfo.partitionCount
  }
 
  async doTransform(messageType,obj) {
	 
	// this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber(),messageType,this.COPY_METRICS.received,this.writableLength,this.writableHighWaterMark],'doTransform()')
	
	/* 
	**
	** Get a 'table' or a 'partition' message followed the 'data'
	**
	*/
	
    switch (messageType) {
      case 'data':
        try {
          // processRow() becomes a No-op after calling abortTable()
          await this.processRow(obj.data)
	    } catch (e) {
          switch (this.dbi.ON_ERROR) {
            case 'SKIP':
            case 'FLUSH':
        	  // Ignore the error 
	          this.yadamuLogger.handleException([`PIPELINE`,`WRITER`,this.SOURCE_VENDOR,this.dbi.DATABASE_VENDOR,`"${this.tableName}"`,messageType,this.dbi.ON_ERROR,this.dbi.getWorkerNumber()],e);    
		      return
            case 'ABORT': 
            default:
              // Treat anything but SKIP or FLUSH, including undefined as ABORT, Rollback any Active Transaction.
              if (this.dbi.TRANSACTION_IN_PROGRESS === true) {
                // Pass the cause to Rollback. Rollback will no-op on a lost connection.
				try {
                  await this.rollbackTransaction(e)
                } catch (rollbackError) {
				  rollbackError.cause = e
			      throw rollbackError
				}
		      }
    		  throw e
		  }
		}
     	break;  
      case 'table':
	    await this.initializeTable(obj.table)
        this.push(obj)
	    break;  
	  case 'partition':
        await this.initializePartition(obj.partition)
        this.push(obj)
	    break;  
	  case 'eod':
        // Used when processing serial data sources such as files to indicate that all records have been processed by the writer
        // this.yadamuLogger.trace([this.constructor.name,`_write()`,this.dbi.DATABASE_VENDOR,messageType,this.displayName,'EMIT'],`"allDataReceived"`)  
        this.emit(YadamuConstants.END_OF_DATA)
        break;  
      default:
    }
		
  }
  
  _transform(obj, encoding, callback) {
	 
    const messageType = Object.keys(obj)[0]
	
	this.doTransform(messageType,obj).then(() => { 
	  callback() 
	}).catch((e) => { 
      this.yadamuLogger.handleException([`PIPELINE`,`WRITER`,this.SOURCE_VENDOR,this.dbi.DATABASE_VENDOR,`"${this.tableName}"`,messageType,this.dbi.ON_ERROR,this.dbi.getWorkerNumber()],e);    
      this.underlyingError = e;
      callback(e) 
    })
	
  }

  async processPendingRows() {
	  
	// this.yadamuLogger.trace([this.constructor.name,this.displayName,this.skipTable,this.dbi.TRANSACTION_IN_PROGRESS,this.writableEnded,this.writableFinished,this.destroyed,this.COPY_METRICS.received,this.COPY_METRICS.committed,this.COPY_METRICS.written,this.COPY_METRICS.cached],`processPendingRows(${this.hasPendingRows()})`)
	
    if (this.hasPendingRows()) {
      this.COPY_METRICS.pending+= this.COPY_METRICS.cached
	  this.push({batch:this.batch, snapshot: Object.assign({}, this.COPY_METRICS)})
 	}
	else {
	  this.COPY_METRICS.lost += this.COPY_METRICS.cached
	  this.releaseBatch(this.batch)
	}
    this.COPY_METRICS.cached = 0
   
  }

  async doFlush() {
	
	await this.processPendingRows()
	
  }

  _flush(callback) {

    // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber(),this.COPY_METRICS.received,this.COPY_METRICS.cached,this.COPY_METRICS.written,this.COPY_METRICS.skipped,this.COPY_METRICS.lost,this.writableEnded,this.writableFinished],'YadamuOutputManager._flush()')
	this.doFlush().then(() => { callback()}).catch((e) => { callback(e)})
    
  }
   
  async doDestroy(err) {
	
	try {
	  await this.dbi.ddlComplete
	} catch (ddlFailure) {
	  if (err) ddlFailure.cause = err
	  throw ddlFailure
	}
	
    if (err) {
	  this.underlyingError = this.underlyingError || err
  	  if (YadamuConstants.ABORT_CURRENT_TABLE.includes(this.dbi.ON_ERROR)) {
        this.abortTable()
	  }	
	  try {
  	    await this.processPendingRows()
	  } catch(e) {
	   this.underlyingError = e
	   e.cause = err
	   this.yadamuLogger.handleException([`PIPELINE`,`STREAM WRITER`,this.SOURCE_VENDOR,this.dbi.DATABASE_VENDOR,`"${this.tableName}"`,this.dbi.ON_ERROR,this.dbi.getWorkerNumber()],e);
	   throw e
	  }	
	}
  }
	
  _destroy (err,callback)  {
	  
    // this.yadamuLogger.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber(),this.readableLength,this.writableLength],`YadamuOutputManager._destroy(${err ? err.message : 'Normal'})`)
    this.doDestroy(err).then(() => { callback(err) }).catch((e) => { callback(e) })
  }
}

export { YadamuOutputManager as default}

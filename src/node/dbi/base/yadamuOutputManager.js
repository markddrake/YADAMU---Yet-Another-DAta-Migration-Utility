
import fs                from 'fs'
import assert            from 'assert'
import { 
  performance 
}                        from 'perf_hooks';

import {
  Readable, 
  Writable, 
  Transform, 
  pipeline 
}                        from 'stream'

import YadamuLibrary     from '../../lib/yadamuLibrary.js';
import YadamuConstants   from '../../lib/yadamuConstants.js';

import DBIConstants      from './dbiConstants.js'

import {
  YadamuError, 
  BatchInsertError, 
  IterativeInsertError, 
  DatabaseError, 
  InvalidMessageSequence
}                        from '../../core/yadamuException.js'

class YadamuOutputManager extends Transform {

  get LOGGER()             { return this._LOGGER }
  set LOGGER(v)            { this._LOGGER = v }
  get DEBUGGER()           { return this._DEBUGGER }
  set DEBUGGER(v)          { this._DEBUGGER = v }
  
  get PIPELINE_STATE()     { return this._PIPELINE_STATE }
  set PIPELINE_STATE(v)    { this._PIPELINE_STATE =  v }
      
  get STREAM_STATE()       { return this.PIPELINE_STATE[DBIConstants.TRANSFORMATION_STREAM_ID] }
  set STREAM_STATE(v)      { this.PIPELINE_STATE[DBIConstants.TRANSFORMATION_STREAM_ID] = v }
  
  get BATCH_SIZE()         { return this.tableInfo._BATCH_SIZE }
  get SPATIAL_FORMAT()     { return this.tableInfo._SPATIAL_FORMAT }
  get SOURCE_VENDOR()      { return this.PIPELINE_STATE[DBIConstants.INPUT_STREAM_ID].vendor || 'YABASC'  }
  
  get PIPELINE_STATE()       { return this._PIPELINE_STATE }
  set PIPELINE_STATE(v)      { this._PIPELINE_STATE =  v }

  get PARTITIONED_TABLE()  { return this.tableInfo?.hasOwnProperty('partitionCount')}    

  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {
    const options = {
	  objectMode               : true
	  , readableHighWaterMark  : 1024
	  , writableHighWaterMark  : dbi.BATCH_LIMIT
	}
    super(options)
    this.dbi = dbi;
    this.tableName = tableName
	this.displayName = tableName
	this.PIPELINE_STATE = pipelineState
	this.STREAM_STATE = { vendor : dbi.DATABASE_VENDOR }
	this.status = status;
  
  	this.LOGGER   = yadamuLogger || this.dbi.LOGGER
	this.DEBUGGER = this.dbi.DEBUGGER

    this.batchOperations = new Set()

    this.skipTable = this.dbi.MODE === 'DDL_ONLY';    
	// this.processRow = this._invalidMessageSequence
	this.processRow = this._cacheOutOfSequenceMessages
    this.outOfSequenceMessageCache = []
  }
  
  createBatch() {
	return []
  }
  
  resetBatch(batch) {  
	batch.length = 0
  }

  initializeBatchCache() {
	this.batchCache = Array(this.dbi.BATCH_LIMIT || 1).fill(0).map((x) => { return this.createBatch()})	
  }	  
  
  async nextBatch() {
    this.PIPELINE_STATE.batchNumber++;
	if (this.batchCache.length === 0) {  
      // this.LOGGER.trace([this.constructor.name,'newBatch()',this.dbi.DATABASE_VENDOR,this.dbi.ROLE,this.dbi.getWorkerNumber(),this.tableName,this.PIPELINE_STATE.batchNumber,'BATCH_RELEASED'],'WAITING')       
	  await new Promise((resolve,reject) => {
		this.once(DBIConstants.BATCH_RELEASED,() => {
	      resolve()
		})
	  })
      // this.LOGGER.trace([this.constructor.name,'newBatch()',this.dbi.DATABASE_VENDOR,this.dbi.ROLE,this.dbi.getWorkerNumber(),this.tableName,this.PIPELINE_STATE.batchNumber,'BATCH_RELEASED'],'PROCESSING')
	}
	this.PIPELINE_STATE.cached = 0;
	const nextBatch = this.batchCache.shift()
  	return nextBatch
  }
  
  releaseBatch(batch) {
	if (batch) {
	  this.resetBatch(batch)
	  this.batchCache.push(batch)
	}
	this.emit(DBIConstants.BATCH_RELEASED)
  }

  async setTableInfo(tableName) {
    // this.LOGGER.trace([this.constructor.name,this.dbi.DATABASE_VENDOR,this.dbi.ROLE,this.dbi.getWorkerNumber(),'CACHE_LOADED'],'WAITING')
    await this.dbi.cacheLoaded
    // this.LOGGER.trace([this.constructor.name,this.dbi.DATABASE_VENDOR,this.dbi.ROLE,this.dbi.getWorkerNumber(),'CACHE_LOADED'],'PROCESSING')
    this.tableInfo = this.dbi.getTableInfo(tableName)
	this.skipTable = this.dbi.MODE === 'DDL_ONLY';	
	this.rowTransformation  = this.setTransformations(this.tableInfo.targetDataTypes)
    this.initializeBatchCache() 
    this.batch = await this.nextBatch()
  }
     
  generateTransformations(dataTypes) {
	
	return Array(dataTypes.columnNames.length).fill(null);

  }
  
  setTransformations(dataTypes) {
	 	  
	this.transformations = this.generateTransformations(dataTypes) 
	
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
	// this.LOGGER.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber()],'abortTable()')
	
    this.skipTable = true;
	
	// Abort means that records written but not committed are lost
	// If rollback occurs prior to abort, written is already added to lost and set to zero.
	// If abort occurs prior to rollback or abort is called multiple times written is already zeroed out.

    this.PIPELINE_STATE.skipped += this.PIPELINE_STATE.cached;
    this.PIPELINE_STATE.cached = 0;	  

    // Disable the processRow() function.
    this.processRow = this._skipRow
	
  }
 
  rowsLost() {
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
	  
	if (this.rowsLost()) {
	  throw cause
	}
	
    this.PIPELINE_STATE.skipped++;
    
    try {
      this.rejectRow(this.tableName,record);
      const iterativeError = this.createIterativeException(cause,this.PIPELINE_STATE.cached,rowNumber,record,info)
	  this.dbi.trackExceptions(iterativeError);
      this.LOGGER.logRejected([...(typeof cause.getTags === 'function' ? cause.getTags() : []),this.dbi.DATABASE_VENDOR,this.displayName,operation,this.PIPELINE_STATE.cached,rowNumber],iterativeError);
    } catch (e) {
	  this.LOGGER.handleException([this.dbi.DATABASE_VENDOR,'ITERATIVE_ERROR',this.displayName,this.tableInfo.insertMode],e)
    }
    
    if (this.PIPELINE_STATE.skipped === this.dbi.TABLE_MAX_ERRORS) {
      this.LOGGER.error([this.dbi.DATABASE_VENDOR,this.displayName,this.tableInfo.insertMode],`Maximum Error Count exceeded. Skipping Table.`);
      this.abortTable()
    }
  }

  flushBatch() {
    return ((this.PIPELINE_STATE.cached === this.BATCH_SIZE) && !this.skipTable)
  }
  
  batchRowCount() {
    return this.PIPELINE_STATE.cached
  }
  
  hasPendingRows() {
    return ((this.PIPELINE_STATE.cached > 0) && !this.skipTable)
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
	  if (row === undefined) {
        this.LOGGER.logInternalError([this.constructor.name,`checkColumnCount()`,this.tableName,],`${this.constructor.name}.checkColumnCount(). Received "undefined" row.`)
		this.abortTable()
		row = []
	  }
	  const info = this.tableInfo === undefined  ? this.tableName : this.tableInfo
      this.handleIterativeError('CACHE',cause,this.PIPELINE_STATE.received,row,info);
    }
  }
              
  cacheRow(row) {
	  
    // Apply transformations and cache transformed row.
    
    // Use forEach not Map as transformations are not required for most columns. 
    // Avoid uneccesary data copy at all cost as this code is executed for every column in every row.

    // this.LOGGER.trace([this.constructor.name,'YADAMU WRITER',this.PIPELINE_STATE.cached],'cacheRow()')    

	// if (this.PIPELINE_STATE.cached === 0) console.log('CR1',row)      
    this.rowTransformation(row)
	// if (this.PIPELINE_STATE.cached === 0) console.log('CR2',row)
    this.batch.push(row);
    this.PIPELINE_STATE.cached++
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
  	  // this.LOGGER.qa(['WARNING',this.tableName,`INCORRECT MESSAGE SEQUENCE`],`Encounted ${this.outOfSequenceMessageCache.length} out of sequence 'data' messages prior to receiving 'table' message.`)  
	  this.PIPELINE_STATE.receivedOoS = this.outOfSequenceMessageCache.length
	}
	
    for (const data of this.outOfSequenceMessageCache) {
	  this.processRow(data)
	}
	
	// this.outOfSequenceMessageCache.length = 0
  }
  
  async _invalidMessageSequence(data) {
	throw new InvalidMessageSequence(this.tableName,'data','table')
  }
  
  async _skipRow(data) {}
  
  async _processRow(data) {
    // Be very careful about adding unecessary code here. This is executed once for each row processed by YADAMU. Keep it as lean as possible.
    this.PIPELINE_STATE.received++;
    this.checkColumnCount(data)
    this.cacheRow(data)
	if (this.flushBatch()) {
      if (this.dbi.REPORT_BATCHES) {
		// Push the Batch and a copy of the current pipeline state.
        this.LOGGER.info([`${this.displayName}`,this.tableInfo.insertMode],`Rows written:  ${this.PIPELINE_STATE.written}.`);
	  }
	  this.PIPELINE_STATE.pending+= this.PIPELINE_STATE.cached
	  const pushResult = this.push({batch:this.batch, snapshot: Object.assign({}, this.PIPELINE_STATE)})
	  this.PIPELINE_STATE.cached = 0
      this.batch = await this.nextBatch();	
	  }	  
    else {
	  if ((this.dbi.FEEDBACK_MODEL === 'ALL') && (this.PIPELINE_STATE.received % this.dbi.FEEDBACK_INTERVAL === 0)) {
        this.LOGGER.info([`${this.displayName}`,this.tableInfo.insertMode],`Rows Cached: ${this.PIPELINE_STATE.cached}.`);
      }
	}
  }
  
  async endTable() {}
        
  async doConstruct() {
	this.STREAM_STATE.startTime = performance.now()
  }
 
  _construct(callback) {

	this.doConstruct().then(() => {
      callback() 
	}).catch((err) => { 
   	  this.STREAM_STATE.error = err
	  callback(err)  
    })
	
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
	 
	// this.LOGGER.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber(),messageType,this.PIPELINE_STATE.received,this.writableLength,this.writableHighWaterMark],'doTransform()')
	
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
			case 'RETRY':
            case 'SKIP':
            case 'FLUSH':
        	  // Ignore the error 
	          this.LOGGER.handleException([`PIPELINE`,`TRANSFORMER`,this.SOURCE_VENDOR,this.dbi.DATABASE_VENDOR,`"${this.tableName}"`,messageType,this.dbi.ON_ERROR,this.dbi.getWorkerNumber()],e);    
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
        // Used when processing serial data sources such as files, which may contain data for more than one table, to indicate that all records for a given table have been processed by the writer
        // this.LOGGER.trace([this.constructor.name,`_write()`,this.dbi.DATABASE_VENDOR,messageType,this.displayName,'EMIT'],`"allDataReceived"`)  
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
      this.LOGGER.handleException([`PIPELINE`,`TRANSFORMER`,this.SOURCE_VENDOR,this.dbi.DATABASE_VENDOR,`"${this.tableName}"`,messageType,this.dbi.ON_ERROR,this.dbi.getWorkerNumber()],e);    
  	  this.STREAM_STATE.error = e
      callback(e) 
    })
	
  }

  async processPendingRows() {
	  
	// this.LOGGER.trace([this.constructor.name,this.displayName,this.skipTable,this.dbi.TRANSACTION_IN_PROGRESS,this.writableEnded,this.writableFinished,this.destroyed,this.PIPELINE_STATE.received,this.PIPELINE_STATE.committed,this.PIPELINE_STATE.written,this.PIPELINE_STATE.cached],`processPendingRows(${this.hasPendingRows()})`)
	
    if (this.hasPendingRows()) {
      this.PIPELINE_STATE.pending+= this.PIPELINE_STATE.cached
 	  // Push the Final Batch and a copy of the current pipeline state.
	  this.push({batch:this.batch, snapshot: Object.assign({}, this.PIPELINE_STATE)})
	  this.PIPELINE_STATE.cached = 0
 	}
	else {
	  this.PIPELINE_STATE.lost += this.PIPELINE_STATE.cached
      this.PIPELINE_STATE.cached = 0
	  this.releaseBatch(this.batch)
	}
   
  }

  async doFlush() {
	
	await this.processPendingRows()
	
  }

  _flush(callback) {

    // this.LOGGER.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber(),this.PIPELINE_STATE.received,this.PIPELINE_STATE.cached,this.PIPELINE_STATE.written,this.PIPELINE_STATE.skipped,this.PIPELINE_STATE.lost,this.writableEnded,this.writableFinished],'YadamuOutputManager._flush()')
	this.doFlush().then(() => { callback()}).catch((e) => { callback(e)})
    
  }
   
  async doDestroy(err) {

    this.STREAM_STATE.endTime = performance.now()	
	this.STREAM_STATE.readableLength = this.readableLength || 0
    this.STREAM_STATE.writableLength = this.writableLength || 0
	this.PIPELINE_STATE.skipTable = this.tableInfo.skipTable

    if (err) {
	  this.PIPELINE_STATE.failed = true
      this.PIPELINE_STATE.errorSource = this.PIPELINE_STATE.errorSource || DBIConstants.TRANSFORMATION_STREAM_ID
	  this.STREAM_STATE.error = this.STREAM_STATE.error || (this.PIPELINE_STATE.errorSource === DBIConstants.TRANSFORMATION_STREAM_ID) ? err : this.STREAM_STATE.error
	  err.pipelineState = YadamuError.clonePipelineState(this.PIPELINE_STATE)
    }
	
	try {
	  await this.dbi.ddlComplete
	} catch (ddlFailure) {
	  if (err && !Object.is(ddlFailure,err)) ddlFailure.cause = err
	  throw ddlFailure
	}

	if (err) {
	  if (YadamuConstants.ABORT_CURRENT_TABLE.includes(this.dbi.ON_ERROR)) {
        this.abortTable()
	  }	
	  try {
  	    await this.processPendingRows()
	  } catch(e) {
	    e.cause = err
	    this.LOGGER.handleException([`PIPELINE`,`TRANSFORMER`,this.SOURCE_VENDOR,this.dbi.DATABASE_VENDOR,`"${this.tableName}"`,this.dbi.ON_ERROR,this.dbi.getWorkerNumber()],e);
	    throw e
	  }	
	}
  }
	
  _destroy (err,callback)  {
	  
    // this.LOGGER.trace([this.constructor.name,this.displayName,this.dbi.getWorkerNumber(),this.readableLength,this.writableLength],`YadamuOutputManager._destroy(${err ? err.message : 'Normal'})`)
    this.doDestroy(err).then(() => { callback(err) }).catch((e) => { callback(e) })
  }
}

export { YadamuOutputManager as default}

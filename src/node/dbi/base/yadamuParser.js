
import { 
  performance
}                        from 'perf_hooks';

import {
  setTimeout 
}                        from 'timers/promises'

import { 
  Transform 
}                        from 'stream';

import DBIConstants      from './dbiConstants.js';

import {
  YadamuError
}                        from '../../core/yadamuException.js'

class YadamuParser extends Transform {

  get LOGGER()               { return this._LOGGER }
  set LOGGER(v)              { this._LOGGER = v }
  get DEBUGGER()             { return this._DEBUGGER }
  set DEBUGGER(v)            { this._DEBUGGER = v }
  
  get PIPELINE_STATE()       { return this._PIPELINE_STATE }
  set PIPELINE_STATE(v)      { this._PIPELINE_STATE =  v }

  get STREAM_STATE()         { return this.PIPELINE_STATE[DBIConstants.PARSER_STREAM_ID] }
  set STREAM_STATE(v)        { this.PIPELINE_STATE[DBIConstants.PARSER_STREAM_ID] = v }
	  
  generateTransformations(queryInfo) {
	
	return queryInfo.DATA_TYPE_ARRAY.map((dataTypes,idx) => {
	  return null
	})

  }
  
  setTransformations(queryInfo) {

	this.transformations = this.generateTransformations(queryInfo)

	// Use a dummy rowTransformation function if there are no transformations required.

    this.rowTransformation = this.transformations.every((currentValue) => { return currentValue === null}) ? (row) => {} : (row) => {
    this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }
  }

  constructor(dbi,queryInfo,pipelineState,yadamuLogger) {
    super({objectMode: true });  
	this.dbi = dbi
    this.queryInfo = queryInfo;
	this.setTransformations(queryInfo)
	this.PIPELINE_STATE = pipelineState
	this.STREAM_STATE = { vendor : dbi.DATABASE_VENDOR }
	this.LOGGER   = yadamuLogger || this.dbi.LOGGER
	this.DEBUGGER = this.dbi.DEBUGGER
  }
    
  sendTableMessage() {
	
	// Push a Table Object or a Partition Object. If the this.queryInfo has a PARTITION_NUMBER property assume this is a partition level operation, rather than a table level operation
	
	// Push the table name into the stream before sending the data.

    if (this.queryInfo.hasOwnProperty('PARTITION_NUMBER')) {
      // console.log('YadamuParser()','PUSH','PARTITION',this.queryInfo.MAPPED_TABLE_NAME,this.queryInfo.PARTITION_NUMBER,this.queryInfo.PARTITION_NAME)
      const padSize = this.queryInfo.PARTITION_COUNT.toString().length
	  const partitionInfo =  {
	    tableName          : this.queryInfo.MAPPED_TABLE_NAME
      , displayName        : `${this.queryInfo.MAPPED_TABLE_NAME}(${this.queryInfo.PARTITION_NAME || `#${this.queryInfo.PARTITION_NUMBER.toString().padStart(padSize,"0")}`})`
	  , partitionCount     : this.queryInfo.PARTITION_COUNT
	  , partitionNumber    : this.queryInfo.PARTITION_NUMBER
      , partitionName      : this.queryInfo.PARTITION_NAME || ''
  	  }
		   
	  this.push({partition: partitionInfo})	
	  // this.timings.push(['PARTITION',performance.now()])
    }
	else {
  	  // console.log('YadamuParser()','PUSH','TABLE',this.queryInfo.MAPPED_TABLE_NAME)
      this.push({table: this.queryInfo.MAPPED_TABLE_NAME})
      // this.timings.push(['TABLE',performance.now()])
    }
	  
  }
  	
  async doConstruct() {
	this.STREAM_STATE.startTime = performance.now()
	this.sendTableMessage()
  }

  _construct(callback) {
	
	this.doConstruct().then(() => { 
	  callback() 
	}).catch((e) => { 
   	  this.STREAM_STATE.error = e
	  callback(e) 
	})
  }
  
  async doTransform(data) {
    this.rowTransformation(data)
	return data 
  }

  _transform(data,enc,callback) {

    this.PIPELINE_STATE.parsed++

    this.doTransform(data).then((row) => {
	  this.push({data:row})
      // this.timings.push(['DATA',performance.now()])
	  callback() 
	  // if (this.timings.length === 3) {console.log( this.timings )}
    }).catch((e) => { 
   	  this.STREAM_STATE.error = e
	  callback(e) 
	})
  
  }

   _final(callback) {
	// this.LOGGER.trace([this.constructor.name,this.queryInfo.TABLE_NAME],'_final()');
	callback()
  } 

  async doDestroy(err) {
	      
    this.STREAM_STATE.endTime = performance.now()
	this.STREAM_STATE.readableLength = this.readableLength || 0
    this.STREAM_STATE.writableLength = this.writableLength || 0
	this.PIPELINE_STATE.insertMode = this.tableInfo ? this.tableInfo.insertMode : this.dbi.insertMode

    if (err) {
      this.PIPELINE_STATE.failed = true
      this.PIPELINE_STATE.errorSource = this.PIPELINE_STATE.errorSource || DBIConstants.PARSER_STREAM_ID
	  this.STREAM_STATE.error = this.STREAM_STATE.error || (this.PIPELINE_STATE.errorSource === DBIConstants.PARSER_STREAM_ID) ? err : this.STREAM_STATE.error
	  err.pipelineComponents = [...err.pipelineComponents || [], this.constructor.name]
	  err.pipelineIgnoreErrors = true
	  err.pipelineState = YadamuError.clonePipelineState(this.PIPELINE_STATE)
    }  
	
  }

   _destroy(err,callback) {

	// this.LOGGER.trace([this.constructor.name,this.queryInfo.TABLE_NAME,this.readableLength,this.writableLength],`YadamuParser._destroy(${err ? err.message : 'Normal'})`)
    
	this.doDestroy(err).then(() => { 
	  callback(err) 
	}).catch((e) => { 
	  e.rootCause = err; 
	  callback(e) 
    })
   }
  
}

export { YadamuParser as default}
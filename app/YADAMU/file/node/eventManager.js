"use strict" 

const util = require('util')
const Transform = require('stream').Transform;
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

class EventManager extends Transform {
    
  /*
  **
  ** Controls the flow of messages when processing a sequential event source, such as a file or HTTP Stream.
  ** 
  ** Registers for ddlComplete events from the dbWriter
  ** Pauses the flow of messages by unpiping the event source when a 'metadata' message is sent down the pipe.
  ** Switches the flow of messages from the DBWriter to the worker once the 'ddlComplete' notification is received.
  ** Provide an exportComplete() method to send the 'exportComplete' method to the DBWriter when the export is complete.
  **
  */
	
  constructor(yadamuLogger) {
    super({objectMode: true});
	this.yadamuLogger = yadamuLogger
    this.dbWriterDetached = false;
	
    this.pipeStatistics = {
 	  rowsRead       : 0
    , pipeStartTime  : undefined
    , readerEndTime  : undefined
    , parserEndTime  : undefined
    , copyFailed     : false
    , tableNotFound  : false
    }
  
    this.copyOperations = []
	
  }	
   
  
  pipe(outputStream,options) {
	// Cache the target outputStream
	this.outputStream = outputStream
	if (this.dbWriter === undefined) {
	  this.dbWriter = outputStream;
	  this.ddlComplete = new Promise((resolve,reject) => {
	    this.dbWriter.once('ddlComplete',() => {
 	      // this.yadamuLogger.trace([this.constructor.name],`DDL Complete`)
		  resolve(true);
		})
	  })
	}
	return super.pipe(outputStream,options);
  } 

  async createWorker(tableName) {
    const worker = this.dbWriter.dbi.getOutputStream(tableName);
	await worker.initialize()
	this.copyOperation = new Promise((resolve,reject) => {
	  worker.once('allDataReceived',async () => {
        // this.yadamuLogger.trace([this.constructor.name,worker.constructor.name,worker.tableName],`All Data Received`)
  	    this.pipeStatistics.parserEndTime = performance.now()    
	    this.unpipe(this.outputStream);
		const currentStats = Object.assign({},this.pipeStatistics)
		const reportComplete = new Promise((resolve,reject) => {
		  worker.end(null,null,() => {
		    const timings = worker.reportPerformance(currentStats);
		    worker.removeAllListeners('end');
	        this.dbWriter.recordTimings(timings);
		    resolve(worker.tableName)
	     })
	   })
	   resolve(reportComplete)
	 })
   })
   // Copy error events from dbWriter to worker
   // this.dbWriter.eventNames().forEach((e) => {e !== 'allDataReceived' ? this.dbWriter.listeners(e).forEach((f) => {console.log(e);worker.on(e,f)}) : null});
   this.dbWriter.listeners('error').forEach((f) => {worker.on('error',f)});
   return worker
  }
    
  async _transform (data,encoding,callback)  {
	const messageType = Object.keys(data)[0]
    // this.yadamuLogger.trace([this.constructor.name,`_transform()`,messageType],``)
	switch (messageType) {
	  case 'data':
	    this.pipeStatistics.rowsRead++
        this.push(data);
		break;
      case 'metadata' :
        this.push(data)
        this.push({pause:true})
 	    await this.ddlComplete
		this.unpipe(this.dbWriter)
		this.dbWriterDetached = true;
	    break;
      case 'table':
		// Switch Workers - Couldnot get this work with 'drain' for some reason
	    this.pipeStatistics.pipeStartTime =	performance.now()    
		this.pipeStatistics.readerStartTime = performance.now()    
	    this.pipeStatistics.rowsRead = 0;
		const worker = await this.createWorker(data.table)
		this.pipe(worker) 
	    break;
      case 'eod':
	    this.pipeStatistics.readerEndTime =	performance.now()    
        this.push(data);
		const result = await this.copyOperation
		this.copyOperations.push(result)
	    break;
	  case 'eof':
	    await this.copyOperations
		if (this.dbWriterDetached) {
	      this.pipe(this.dbWriter); 
		  this.dbWriter.deferredCallback()
		}
	    this.push(data);
		break;
	  default:
        this.push(data);
	}
    callback();
  }  
  
}

module.exports = EventManager
"use strict" 

const util = require('util')
const Transform = require('stream').Transform;
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

const YadamuLibrary = require('../../common/yadamuLibrary.js');

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
	
  constructor(yadamu) {
    super({objectMode: true});
	this.yadamu = yadamu
	this.yadamuLogger = yadamu.LOGGER;
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
	}
	return super.pipe(outputStream,options);
  } 


  async createTransformations(tableName) {
	  
	const tableMetadata = this.metadata[tableName]
	this.transformations = tableMetadata.dataTypes.map((targetDataType,idx) => {

      const dataType = YadamuLibrary.decomposeDataType(targetDataType);

	  if (YadamuLibrary.isBinaryDataType(dataType.type)) {
        return (row,idx) =>  {
  		  row[idx] = Buffer.from(row[idx],'hex')
		}
      }

	  switch (dataType.type.toUpperCase()) {
        case "GEOMETRY":
        case "GEOGRAPHY":
        case '"MDSYS"."SDO_GEOMETRY"':
          if (this.spatialFormat.endsWith('WKB')) {
            return (row,idx)  => {
  		      row[idx] = Buffer.from(row[idx],'hex')
			}
          }
		  return null;
		 default:
		   return null
      }
    }) 
	
	// Use a dummy rowTransformation function if there are no transformations required.

	this.rowTransformation = this.transformations.every((currentValue) => { currentValue === null}) ? (row) => {} : (row) => {
      this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }
	  
	  
  }
  
  async createWorker(tableName) {
    const worker = this.dbWriter.dbi.getOutputStream(tableName,this.dbWriter.ddlComplete)
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
    this.dbWriter.listeners('error').forEach((f) => {worker.on('error',f)});
    return worker
  }
    
	
  async _transform (data,encoding,callback)  {
	const messageType = Object.keys(data)[0]
    // this.yadamuLogger.trace([this.constructor.name,`_transform()`,messageType],``)
	switch (messageType) {
	  case 'data':
	    this.pipeStatistics.rowsRead++
		this.rowTransformation(data.data)
        this.push(data);
		break;
      case 'systemInformation' :
        this.push(data)
		this.spatialFormat = data.systemInformation.spatialFormat
		this.yadamu.REJECTION_MANAGER.setSystemInformation(data.systemInformation)
		this.yadamu.WARNING_MANAGER.setSystemInformation(data.systemInformation)
	    break;
      case 'metadata' :
	    this.metadata = data.metadata
        this.push(data)
		this.yadamu.REJECTION_MANAGER.setMetadata(data.metadata)
		this.yadamu.WARNING_MANAGER.setMetadata(data.metadata)
		// Wait for DDL Complete and then release the DBWriter by invoking the deferredCallback
		await this.dbWriter.ddlComplete
		this.dbWriter.deferredCallback()
	    break;
      case 'table':
		// Switch Workers - Couldnot get this work with 'drain' for some reason
		this.createTransformations(data.table)
	    this.pipeStatistics.pipeStartTime =	performance.now()    
		this.pipeStatistics.readerStartTime = performance.now()    
	    this.pipeStatistics.rowsRead = 0;
		const worker = await this.createWorker(data.table)
		this.pipe(worker) 
		this.dbWriterDetached = true;
		this.push(data)
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
"use strict" 

const util = require('util')
const Transform = require('stream').Transform;

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
	
  constructor(yadamuLogger,errorCallback) {
    super({objectMode: true});
	this.yadamuLogger = yadamuLogger
	this.errorCallback = errorCallback
	this.dbWriter = undefined
	this.worker = undefined;
	this.switched = false
  }	
  
  createWorker(dbWriter) {
	this.dbWriter = dbWriter
    const worker = dbWriter.dbi.getOutputStream(dbWriter)
    dbWriter.listeners('error').forEach((f) => {worker.on('error',f)})
    dbWriter.on('ddlComplete',() => {
      // this.yadamuLogger.trace([this.constructor.name,`onddlComplete()`],`Attaching pipe to ${this.worker.constructor.name}`)
	  this.pipe(worker)
      this.switched = true;
	});
	return worker
  }
  
  pipe(target,options) {
    const result = super.pipe(target,options);
	this.worker = this.worker === undefined ? this.createWorker(result) : this.worker
	return result;
  } 
 
  async _transform (data,encoding,callback)  {
	const messageType = Object.keys(data)[0]
    // this.yadamuLogger.trace([this.constructor.name,`_transform()`,messageType],``)
	if (messageType === 'table') {
	  // Need to register the Error callback for each Table to be compatible with a database backed reader.
	  this.worker.registerErrorCallback(data.table,this.errorCallback)
    } 
    this.push(data);
	if (messageType === 'metadata') {
	  // this.yadamuLogger.trace([this.constructor.name,`_transform()`],`Detaching pipe from ${this.dbWriter.constructor.name}`)
	  this.unpipe(this.dbWriter)
	}
	callback()
  }  
  
  exportComplete() {
    // this.yadamuLogger.trace([this.constructor.name],`exportComplete()`)
	this.dbWriter.write({exportComplete:true})
	this.destroy()
  } 
}

module.exports = EventManager
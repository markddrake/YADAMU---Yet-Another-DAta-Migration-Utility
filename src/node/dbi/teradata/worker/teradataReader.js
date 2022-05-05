
import {
  Readable
}                      from 'stream';

import { 
  TeradataError 
}                                     from '../teradataException.js'


class TeradataReader extends Readable {
    
    constructor(worker,connectionProperties,sqlStatement) {
      super({objectMode:true}) 
      this.worker = worker
      this.vendorProperties = connectionProperties
      this.sqlStatement = sqlStatement
      
      this.stagingArea = []
      this.highWaterMark = 10240
      this.lowWaterMark = 7680
      
      this.readPending = false;
      this.streamPaused = false;
      this.streamFailed = false;
      this.streamComplete = false;
      this.fetchInProgress = false;         
      this.recordsRead = 0
	  this.idleTime = 0;
        
    }
    
    async enqueueTask(task) {
      
    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`deligateTask(${task.action})`)
    
    // Use the Worker to perform the task
      
    const taskComplete = new Promise((resolve,reject) => {
      this.worker.once('message',(response) => {
        if (response.success) {
          resolve(response)
        }
        else {
          reject(response.name === 'TeradataError' ? TeradataError.recreateTeradataError(response.cause) : new TeradataError(this.DRIVER_ID,response.cause,task.sql))
        }
      })
    })
    this.worker.postMessage(task)
    return taskComplete
    
  } 
  
  async initialize() {
      
    const result = await this.enqueueTask({action : "connect", connectionProperties : this.vendorProperties})
    const response = await this.enqueueTask({action : "query", sql: this.sqlStatement, batchSize: this.highWaterMark})
    this.stagingArea.push(...response.rows)
	this.recordsRead+=response.rows.length
  }
    
  async doRead() {
    if (this.stagingArea.length > 0) {
      this.push(this.stagingArea.shift())
	  if ((this.stagingArea.length < this.lowWaterMark) && (!this.streamComplete) && (!this.fetchInProgress)){
		this.fetchInProgress = true
        const response = await this.enqueueTask({action : "fetchMore", batchSize: this.lowWaterMark})
		this.fetchInProgress = false;          
        if (response.rows.length > 0) {
          this.stagingArea.push(...response.rows)
	      this.recordsRead+=response.rows.length
        }
        else {
          this.streamComplete = true
        }
        if (this.readPending) {
          this.push(this.stagingArea.length > 0 ? this.stagingArea.shift() : null)
	      const readIdleTime = performance.now() - this.startIdleTime
  		  this.idleTime += readIdleTime;
        }
      }
    }
    else {
      if (!this.fetchInProgress) { 
        this.push(null)
      }
      else {
		this.startIdleTime = performance.now()
        this.readPending = true;
      }
    }
  }
    
  _read() {
    this.doRead().then(() => {}).catch((e) => {this.destroy(e)})
  }
  
  async doDestroy(e) {
    /// console.log('Read','Destroy',this.recordsRead,this.idleTime);
	const result = await this.enqueueTask({action : "disconnect"})
  }

  _destroy(e,callback) {
	this.doDestroy().then(() => {callback(e)}).catch((err) => {err.cause = e; callback(err)})
  }
}

export { TeradataReader as default }

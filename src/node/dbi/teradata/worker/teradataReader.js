
import {
  Readable
}                      from 'stream';

import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

import { 
  TeradataError 
}                                     from '../teradataException.js'


class TeradataReader extends Readable {
    
    constructor(worker,connectionProperties,sqlStatement,tableName,fetchSize) {
      super({objectMode:true}) 
      this.worker = worker
      this.CONNECTION_PROPERTIES = connectionProperties
      this.sqlStatement = sqlStatement
      this.tableName = tableName
      this.readPending = false;
      this.recordsRead = 0
	  this.idleTime = 0;
	  this.fetchSize = fetchSize;
	  this.totalIdleTime = 0
	  
	  this.stagingArea = []
        
    }
       
    async enqueueTask(task) {
      
    // console.log(`deligateTask(${task.action})`)
    
    // Use the Worker to perform the task
      
    const taskComplete = new Promise((resolve,reject) => {
      const stack = new Error().stack
      this.worker.once('message',(response) => {
		if (response.success) {
          resolve(response)
        }
        else {
          reject(response.name === 'TeradataError' ? TeradataError.recreateTeradataError(response.cause) : new TeradataError(this.DRIVER_ID,response.cause,stack,`${response.action}(${this.sqlStatement})`))
        }
      })
    })
    this.worker.postMessage(task)
    return taskComplete
    
  } 
 
  async doConstruct() {
      
    // const result = await this.enqueueTask({action : "connect", connectionProperties : this.CONNECTION_PROPERTIES})
    const response = await this.enqueueTask({action : "query", sql: this.sqlStatement, batchSize: this.fetchSize})
    this.stagingArea.push(...response.rows)
	this.recordsRead+=response.rows.length
	if (response.streamComplete) {
      this.stagingArea.push(null)
	}
	else {
      const stack = new Error().stack
  	  this.worker.on('message', (data) => {
		if (data.success) {
		  const recordsRead = data.rows.length 
		  this.stagingArea.push(...data.rows)
  		  data.rows.length = 0;
	      this.recordsRead+= recordsRead
          if (data.streamComplete) {
		    this.worker.removeAllListeners('message')
            this.stagingArea.push(null)
		  }
          if (this.readPending) {
			const idleTime = performance.now() - this.startIdleTime
			this.totalIdleTime+= idleTime
    	    // console.log(this.tableName,recordsRead,data.elapsedTime,idleTime,(recordsRead/data.elapsedTime)*1000)
			this.push(this.stagingArea.shift())
		  } 
        }
        else {
          throw data.name === 'TeradataError' ? TeradataError.recreateTeradataError(data.cause) : new TeradataError(this.DRIVER_ID,data.cause,stack,`${data.action}(${this.sqlStatement})`)
        }
	  })
	}
  }
    
  async doRead() {
    if (this.stagingArea.length > 0) {
      this.push(this.stagingArea.shift())
	}
	else {
      this.readPending = true;
	  this.startIdleTime = performance.now();
    }
  }

  _construct(callback) {
    this.doConstruct().then(() => {callback()}).catch((e) => {callback(e)})
  }
    
  _read() {
    this.doRead().then(YadamuLibrary.NOOP).catch((e) => {this.destroy(e)})
  }
  
  async doDestroy(e) {
    /// console.log('Read','Destroy',this.recordsRead,this.idleTime);
    this.worker.removeAllListeners('message');
  }

  _destroy(e,callback) {
	this.doDestroy().then(() => {callback(e)}).catch((err) => {err.cause = e; callback(err)})
  }
}

export { TeradataReader as default }

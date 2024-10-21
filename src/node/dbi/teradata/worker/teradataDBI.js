
import path                           from 'path'
import {                               
  performance                          
}                                     from 'perf_hooks';
import {                               
  fileURLToPath                        
}                                     from 'url';
							           
import {                               
  Worker,                              
  isMainThread,                        
  parentPort,                          
  workerData,
  MessageChannel  
}                                     from 'worker_threads';

/* 
**
** from  Database Vendors API 
**
*/

// import TeradataConnection from "teradata-nodejs-driver/teradata-connection";
// import TeradataExceptions from "teradata-nodejs-driver/teradata-exceptions";

/*
**
** Teradata Implementation Notes
**
**    Spatial Data Types : Use JSON
**    Interval Data Types: Use VARCHAR
**    LOB Support: VARCHAR and BINARY are supported to 16Mb
**
*/

/* Yadamu Core */                                    

import YadamuLibrary                  from '../../../lib/yadamuLibrary.js'

/* Yadamu DBI */                                    
							          							          
/* Vendor Specific DBI Implimentation */                                   
						          
import _TeradataDBI                   from '../teradataDBI.js'
import TeradataConstants              from '../teradataConstants.js'
import StatementLibrary               from '../teradataStatementLibrary.js'

import TeradataReader                 from './teradataReader.js'

import { 
  TeradataError 
}                                     from '../teradataException.js'

const  __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

class TeradataDBI extends _TeradataDBI {
	
  /*
  **
  ** All operations are peformed by a seperate worker thread.
  **
  */
    
  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)
  }
  
  async enqueueTask(task) {
	  
	
	// Deligate the task to the worker
	
	const taskComplete = new Promise((resolve,reject) => {
	  this.worker.once('message',(response) => {
	    if (response.success) {
		  resolve(response)
		}
        else {
		  reject(this.trackExceptions( response.name === 'TeradataError' ? TeradataError.recreateTeradataError(response.cause) : this.createDatabaseError(this.DRIVER_ID,response.cause,task.sql)))
	    }
      })
	})
	this.worker.postMessage(task)
	return taskComplete
  }	

  async getTeradataWorker() {
	const worker = new Worker(path.resolve(path.join(__dirname, 'teradataWorker.js')),{workerData: {}})
	
	/*
	**
	** Set up a back channel for console logging
	** 
	
	this.consoleChannel = new MessageChannel();
	this.consoleChannel.port2.on('message',(message) => { console.log("WORKER",message)})
	worker.postMessage({action: 'console', payload: this.consoleChannel.port1},[this.consoleChannel.port1])
	
	**
	*/
	
    return worker
  }

  async getConnectionFromPool() {

    // this.LOGGER.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`getConnectionFromPool()`)
    
    //  Do not Configure Connection here. 

	this.worker = await this.getTeradataWorker()
    this.SQL_TRACE.comment(`Enqueue: connect()`)

	const result = await this.enqueueTask({action : "connect", connectionProperties : this.CONNECTION_PROPERTIES})
	return result
  }
    
  async closeConnection(options) {
	  
    this.SQL_TRACE.comment( `enqueueTask :disconnect()`)
	
	if (this.worker) {
	  // this.LOGGER.trace([`${this.constructor.name}.closeConnection()`,this.ROLE,this.getWorkerNumber()],'Terminating Worker')
	  
	  /*
	  **
	  ** Tear down the back channel for console messages
	  **

      this.consoleChannel.port2.removeAllListeners('message')
      this.consoleChannel.port1.close()
      this.consoleChannel.port1.unref()
      this.consoleChannel.port2.unref()
	  
	  this.worker.removeAllListeners('message')
 	  
	  **
	  */
	  await this.enqueueTask({action : "disconnect" })
      this.worker.unref()
      this.worker = undefined

      // this.LOGGER.trace([`${this.constructor.name}.closeConnection()`,this.ROLE,this.getWorkerNumber()],'Terminated')	  
	}
  }
    
  async closePool(options) { /* Teradata-SDK does not support connection pooling */ }
   
  	  
  async executeSQL(sqlStatement, args) {
         	 
    this.SQL_TRACE.comment(`enqueueTask: ${sqlStatement} Args: ${(Array.isArray(args) && (args.length > 0)) ? `${args.length} Rows` : JSON.stringify(args)}`)
	
    const result = await this.enqueueTask({action : "executeSQL", sql: sqlStatement, args: YadamuLibrary.isEmpty(args) ? null : args})
	return result.rows
  }   

  setTransactionCursor() { /* OVERRRIDE */ } 
      
  resetTransactionCursor() { /* OVERRRIDE */ }
  
  async _getInputStream(tableInfo) {

    // this.LOGGER.trace([`${this.constructor.name}.getInputStream()`,this.getWorkerNumber()],tableInfo.TABLE_NAME)

    // this.LOGGER.trace([`${this.constructor.name}.getInputStream()`,this.getWorkerNumber()],queryInfo.TABLE_NAME)

	let attemptReconnect = this.ATTEMPT_RECONNECTION;
    
    while (true) {
      // Exit with result or exception.  
	  let stack
      try {
        this.SQL_TRACE.traceSQL(queryInfo.SQL_STATEMENT)
		stack = new Error().stack
        const sqlStartTime = performance.now()
		const is = new TeradataReader(this.worker,this.CONNECTION_PROPERTIES,tableInfo.SQL_STATEMENT,tableInfo.TABLE_NAME,this.FETCH_SIZE)
	    this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		return is;
      } catch (e) {
		const cause = this.getDatabaseException(this.DRIVER_ID,e,stack,sqlStatement)
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'SQL')
          continue;
        }
        throw cause
      }      
    } 	
  }  
  
  async setWorkerConnection() {    
    this.CONNECTION_PROPERTIES = this.manager.CONNECTION_PROPERTIES
	await this.getConnectionFromPool()
  }

  classFactory(yadamu) {
	return new TeradataDBI(yadamu,this,this.connectionParameters,this.parameters)
  } 
  
}
 
export { TeradataDBI as default }


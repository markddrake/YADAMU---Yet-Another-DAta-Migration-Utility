
import path                           from 'path'
import {
  createRequire
}                                     from 'module'
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

let TASK_ID = 0

/*
**
** teradatasql is not one of YADAMU's dependencies: it is installed separately (see docs/Teradata.md). The worker's workaround for the driver's
** row buffer leak (freeFetchedRowBuffers in teradataWorker.js) depends on the driver's internals, and has only been tested with this version.
**
*/

const TERADATASQL_VERSION = '20.0.72'
let TERADATASQL_CHECKED = false

/*
**
** A worker must not be terminated while it is inside a teradatasql (koffi) call: Node aborts with
** "FATAL ERROR: Error::ThrowAsJavaScriptException napi_throw". If the process exits with workers still running, ask each one to stop
** and block until it has finished its current call and closed its cursor and connection. 'exit' listeners must be synchronous, hence Atomics.wait().
**
*/

const ACTIVE_WORKERS = new Map()
const SHUTDOWN_TIMEOUT = 30000

process.on('exit',() => {
  ACTIVE_WORKERS.forEach((state,worker) => {
	Atomics.store(state,TeradataConstants.WORKER_STOP,1)
	// Wakes an idle worker, which then closes its connection. A busy worker stops once its current call returns.
	worker.postMessage({action: 'disconnect'})
  })
  const deadline = performance.now() + SHUTDOWN_TIMEOUT
  ACTIVE_WORKERS.forEach((state) => {
	Atomics.wait(state,TeradataConstants.WORKER_CLOSED,0,Math.max(deadline - performance.now(),0))
  })
})

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
	  
	// Deligate the task to the worker. Responses are matched to tasks by id, so a response that nobody is waiting for is ignored.
	
	const stack = new Error().stack
	if (this.worker === undefined) {
	  throw this.getDatabaseException(new Error('Teradata worker is not connected.'),stack,task.sql)
	}
	task.id = ++TASK_ID
	const taskComplete = new Promise((resolve,reject) => {
	  this.pendingTasks.set(task.id,{resolve, reject, stack, sql: task.sql})
	})
	this.worker.postMessage(task)
	return taskComplete
  }	

  checkDriver() {
	if (TERADATASQL_CHECKED) return
	let version
	try {
	  version = createRequire(import.meta.url)('teradatasql/package.json').version
	} catch (e) {
	  throw new Error(`The Teradata driver is not installed. Install it with "npm install --no-save teradatasql@${TERADATASQL_VERSION}" in the directory that holds YADAMU's node_modules. See docs/Teradata.md.`)
	}
	if (version !== TERADATASQL_VERSION) {
	  this.LOGGER.warning([this.DATABASE_VENDOR,'DRIVER'],`teradatasql ${version} is installed. YADAMU is tested with teradatasql ${TERADATASQL_VERSION}, and its workaround for the driver's row buffer leak depends on that version.`)
	}
	TERADATASQL_CHECKED = true
  }

  async getTeradataWorker() {
	this.checkDriver()
	const state = new Int32Array(new SharedArrayBuffer(2 * Int32Array.BYTES_PER_ELEMENT))
	const worker = new Worker(path.resolve(path.join(__dirname, 'teradataWorker.js')),{workerData: {state: state.buffer}})
	const pendingTasks = new Map()
	this.pendingTasks = pendingTasks
	ACTIVE_WORKERS.set(worker,state)
	
	worker.on('message',(response) => {
	  const task = pendingTasks.get(response.id)
	  if (task) {
		pendingTasks.delete(response.id)
	    if (response.success) {
		  task.resolve(response)
		}
        else {
		  const cause = Object.assign(new Error(response.cause.message),response.cause)
		  task.reject(this.getDatabaseException(cause,task.stack,task.sql))
	    }
	  }
	})
	
	const rejectPendingTasks = (cause) => {
	  ACTIVE_WORKERS.delete(worker)
	  pendingTasks.forEach((task) => { task.reject(this.getDatabaseException(cause,task.stack,task.sql)) })
	  pendingTasks.clear()
	}
	worker.on('error',(e) => { rejectPendingTasks(e) })
	worker.on('exit',(exitCode) => { rejectPendingTasks(new Error(`Teradata worker exited (${exitCode}).`)) })
	
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
	  
	  **
	  */
	  
	  // The worker processes messages in order, so the response to disconnect means it has finished any call in progress and closed its 
	  // cursor and connection. Only then is it safe to terminate it.
	  
	  const worker = this.worker
	  try {
	    await this.enqueueTask({action : "disconnect" })
	  } finally {
        this.worker = undefined
		ACTIVE_WORKERS.delete(worker)
	    await worker.terminate()
	  }

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
  
  async _getInputStream(queryInfo) {

    // this.LOGGER.trace([`${this.constructor.name}.getInputStream()`,this.getWorkerNumber()],queryInfo.TABLE_NAME)

	let attemptReconnect = this.ATTEMPT_RECONNECTION;
    
    while (true) {
      // Exit with result or exception.  
	  let stack
      try {
        this.SQL_TRACE.traceSQL(queryInfo.SQL_STATEMENT)
		stack = new Error().stack
        const sqlStartTime = performance.now()
		const is = new TeradataReader(this,queryInfo.SQL_STATEMENT,queryInfo.TABLE_NAME,this.FETCH_SIZE)
	    this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		return is;
      } catch (e) {
		const cause = this.getDatabaseException(e,stack,queryInfo.SQL_STATEMENT)
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
    // Each worker gets its own worker thread and connection. cloneSettings() has already copied the manager's CONNECTION_PROPERTIES.
	this.connection = await this.getConnectionFromPool()
  }

  classFactory(yadamu) {
	return new TeradataDBI(yadamu,this,this.connectionParameters,this.parameters)
  } 
  
}
 
export { TeradataDBI as default }


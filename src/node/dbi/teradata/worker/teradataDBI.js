
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
  workerData                           
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
  
  async enqueueTask(task,psuedoSQL) {
	  
    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`deligateTask(${task.action})`)
	
	// Use the Worker to perform the task
      
	const taskComplete = new Promise((resolve,reject) => {
	  this.worker.once('message',(response) => {
	    if (response.success) {
		  resolve(response)
		}
        else {
		  reject(this.trackExceptions( response.name === 'TeradataError' ? TeradataError.recreateTeradataError(response.cause) : new TeradataError(this.DRIVER_ID,response.cause,task.sql)))
	    }
      })
	})
	// console.log(`enqueueTask(${task.action})`,task.sql)
    this.worker.postMessage(task)
    return taskComplete
	
  }	

  getTeradataWorker() {
	return new Worker(path.resolve(path.join(__dirname, 'teradataWorker.js')),{workerData: {}})
  }
 
  async getConnectionFromPool() {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`getConnectionFromPool()`)
    
    //  Do not Configure Connection here. 

	this.worker = this.getTeradataWorker()
    this.SQL_TRACE.comment(`Enqueue: connect()`)

	const result = await this.enqueueTask({action : "connect", connectionProperties : this.vendorProperties})
	return result
  }
    
  async closeConnection(options) {
	  
    this.SQL_TRACE.comment( `enqueueTask :disconnect()`)
	
    const result =  (this.worker) ? await this.enqueueTask({action : "disconnect" }) : undefined
	
	if (this.worker) {
	  /*
	  **
	  ** Causes Node Crash
	  ** kpedbg_dmp_stack()+396<-kpeDbgCrash()+129<-kpeDbgSignalHandler()+125<-skgesig_Win_UnhandledExceptionFilter()+158<-0x00007FFAD63D2991<-0x00007FFAD8B6A76C<-0x00007FFAD8B537D6<-0x00007FFAD8B686EF<-0x00007FFAD8AF5AEA<-0x00007FFAD8B676FE<-0x00007FFA977C2520
	
	  this.yadamuLogger.trace([`${this.constructor.name}.closeConnection()`,this.ROLE,this.getWorkerNumber()],'Terminating Worker')
      await this.worker.terminate()
      this.yadamuLogger.trace([`${this.constructor.name}.closeConnection()`,this.ROLE,this.getWorkerNumber()],'Terminated')

	  **
  	  */
	  
      this.worker = undefined
	}
	return result
  }
    
  async closePool(options) { /* Teradata-SDK does not support connection pooling */ }
   
  	  
  async executeSQL(sqlStatement, args) {
         	 
    this.SQL_TRACE.comment(`enqueueTask: ${sqlStatement} Args: ${(Array.isArray(args) && (args.length > 0)) ? `${args.length} Rows` : JSON.stringify(args)}`)
	
    const result = await this.enqueueTask({action : "executeSQL", sql: sqlStatement, args: YadamuLibrary.isEmpty(args) ? null : args})
	return result.rows
  }   

  setTransactionCursor() { /* OVERRRIDE */ } 
      
  resetTransactionCursor() { /* OVERRRIDE */ }

  inputStreamError(e,sqlStatement) {
    return this.trackExceptions(new TeradataError(this.DRIVER_ID,e,sqlStatement))
  }
  
  async getInputStream(tableInfo) {
    // this.yadamuLogger.trace([`${this.constructor.name}.getInputStream()`,this.getWorkerNumber()],tableInfo.TABLE_NAME)
    this.streamingStackTrace = new Error().stack;
	try {
	  const worker = this.getTeradataWorker()
      const is = new TeradataReader(worker,this.vendorProperties,tableInfo.SQL_STATEMENT)
	  await is.initialize()
	  return is
	} catch (e) {
	  throw this.trackExceptions(new TeradataError(this.DRIVER_ID,e,tableInfo.SQL_STATEMENT))
	}
    
  }  
  
  async setWorkerConnection() {    
    this.vendorProperties = this.manager.vendorProperties
	await this.getConnectionFromPool()
  }

  classFactory(yadamu) {
	return new TeradataDBI(yadamu,this,this.connectionParameters,this.parameters)
  } 
  
  async destroy() {
	await super.destroy()
  }
  
}
 
export { TeradataDBI as default }


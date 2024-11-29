
import { 
  performance 
}                                     from 'perf_hooks';

import { 
  Worker, 
  isMainThread, 
  parentPort, 
  workerData 
}                                     from 'worker_threads';

import TeradataConnection             from "teradata-nodejs-driver/teradata-connection.js";
import TeradataExceptions             from "teradata-nodejs-driver/teradata-exceptions.js";

import TeradataConstatns              from '../teradataConstants.js'
import { TeradataError }              from '../teradataException.js'

class TeradataWorker {

  get DATABASE_KEY()              { return TeradataConstants.DATABASE_KEY};
  get DATABASE_VENDOR()           { return TeradataConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()           { return TeradataConstants.SOFTWARE_VENDOR};
  get CONNECTION_NAME()           { return 'TeradataWorker' }
  
  get DRIVER_ID()  { return this._DRIVER_ID }
  set DRIVER_ID(v) { this._DRIVER_ID = v }

  constructor() {
    this.DRIVER_ID = performance.now()
	this.configureActions()
  }
    
  configureActions() {
    parentPort.on('message', (message) => {
      const action = message.action
      // console.log(message.action)
      switch (action) {
        case 'console' :
          this.consoleChannel = message.payload
		  // this.consoleChannel.postMessage('Console Channel Active');
		  break;
        case 'executeSQL':
          try {
            const results = this._executeSQL(message.sql, message.args)
            parentPort.postMessage({action:action, sql: message.sql, success: true, rows: results})
          } catch(e) {
            parentPort.postMessage({action: action, sql: message.sql, success: false, cause: {...e},  name : e.constructor.name})
          }  
          break;
        case 'connect': 
          try {
           this._connect(message.connectionProperties);
           parentPort.postMessage({action: action, success: true})
          } catch(e) {
            parentPort.postMessage({action: action, success: false, cause: e})
          }
          break;
        case 'disconnect': 
          try {
            this._disconnect();
            parentPort.postMessage({action: action, success: true})
          } catch(e) {
            parentPort.postMessage({action: action, success: false, cause: e})
          }  
          break;
        case 'query': 
          try {
      	    const startTime = performance.now()
            const results = this._query(message.sql, message.batchSize)
            parentPort.postMessage({action: action, success: true, streamComplete: results.length < message.batchSize, rows: results, elapsedTime: performance.now() - startTime})
            if (results.length === message.batchSize) {
              this._pushRemainingRows(message.batchSize)
            }
			else {
			  this.cursor.close()
			}
          } catch(e) {
            parentPort.postMessage({action: action, success: false, cause: e})
          }  
          break;
        case 'cancelQuery':
          try {
             const results = this._cancelQuery()
             parentPort.postMessage({action:action, success: true, rows: results})
           } catch(e) {
             parentPort.postMessage({action: action, success: false, cause: e})
           }  
           break;
         default:
      }
    })
  }  

  _connect(connectionProperties) {
    let stack 
    let operation 
    try {
      operation = `TeradataConnection.TeradataConnection()`
      stack = new Error().stack
      this.teradataConnection = new TeradataConnection.TeradataConnection();
      operation = `TeradataConnection.teradataConnection.connect()`
 	  stack = new Error().stack
      this.teradataConnection.connect(connectionProperties);
    } catch (e) {
      throw new TeradataError(this,e,stack,operation)
    }
  }
       
  _pushRemainingRows(batchSize) {
     // this.consoleChannel.postMessage('RRR');
     let streamComplete = false
	 
     const operation =`cursor.next()`
     const stack = new Error().stack
	 
     let startTime = undefined
     const rowCache = [] 
     try {
	   while (!streamComplete) {
		 rowCache.length = 0
		 startTime = performance.now()
	     while (rowCache.length < batchSize) {
           rowCache.push(this.cursor.next())
	     }
         parentPort.postMessage({action: "data", success: true, streamComplete: streamComplete, rows: rowCache, elapsedTime: performance.now() - startTime})
		 rowCache.length = 0
	   }
     } catch (e) {
       streamComplete = true
	   if (e.message === "StopIteration") {
         parentPort.postMessage({action: "data", success: true, streamComplete: streamComplete, rows: rowCache, elapsedTime: performance.now() - startTime})
	   }
	   else	{
   	     parentPort.postMessage({action: "data", success: false, streamComplete: streamComplete, cause: new TeradataError(this,e,stack,operation)})
	   }
	 }
	 this.cursor.close()
  }	   
       
  _query(sqlStatement,batchSize) {
              
	  let stack
      let operation
	  
      try {
        operation = `TeradataConnection.teradataConnection.cursor()`
        this.cursor = this.teradataConnection.cursor()
        operation = `cursor.execute(${sqlStatement})`
		stack = new Error().stack
        this.cursor.execute(sqlStatement);
        operation = `cursor.fetchmany(${batchSize})`
		stack = new Error().stack
        const firstRows = this.cursor.fetchmany(batchSize)
        return firstRows;
      } catch (e) {
        throw new TeradataError(this,e,stack,operation)
      }
	  
  }
 
  _executeSQL(sqlStatement,args) {
	  
	  let operation
	  const stack = new Error().stack
	  
      try {
        operation = `TeradataConnection.teradataConnection.cursor()`
        this.cursor = this.teradataConnection.cursor()
        operation = sqlStatement
        this.cursor.execute(sqlStatement,args)
        const results = this.cursor.fetchall()
   	    this.cursor.close()
		return results
      } catch (e) {
        const cause = new TeradataError(this,e,stack,operation)
        throw cause
      }
	  
    }  

  _disconnect() {
	  
    const stack = new Error().stack
    const operation = `TeradataConnection.close()`
	
    try {  
	  if (this.teradataConnection) {
        this.teradataConnection.close();
      }
      this.teradataConnection = undefined
    } catch (e) {
      throw new TeradataError(this,e,stack,operation)
    }
	
  }
}

const teradataWorker = new TeradataWorker()


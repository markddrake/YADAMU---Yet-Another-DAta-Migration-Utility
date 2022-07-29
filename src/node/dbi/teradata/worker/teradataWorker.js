
import { performance }       from 'perf_hooks';
import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';

import TeradataConnection from "teradata-nodejs-driver/teradata-connection.js";
import TeradataExceptions from "teradata-nodejs-driver/teradata-exceptions.js";

import { TeradataError } from '../teradataException.js'

class TeradataWorker {

  get DRIVER_ID()  { return this._DRIVER_ID }
  set DRIVER_ID(v) { this._DRIVER_ID = v }

  constructor() {
	this.activeQuery = false;
	this.DRIVER_ID = performance.now()
  }  

  _connect(connectionProperties) {
    let operation
    try {
      operation = `TeradataConnection.TeradataConnection()`
      this.teradataConnection = new TeradataConnection.TeradataConnection();
      operation = `TeradataConnection.teradataConnection.connect()`
      this.teradataConnection.connect(connectionProperties);
      operation = `TeradataConnection.teradataConnection.cursor()`
      this.cursor = this.teradataConnection.cursor()
    } catch (e) {
      throw new TeradataError(this.DRIVER_ID,e,operation)
    }
  }
  
  
  _query(sqlStatement,batchSize) {
              
      if (this.activeQuery === true) {
        // ### Do we need to clean-up or throw error  
      }
      
      let operation
      try {
        operation = `cursor.execute(${sqlStatement})`
        this.cursor.execute(sqlStatement);
        this.activeQuery = true
        operation = `cursor.fetchmany(${batchSize})`
        return this.cursor.fetchmany(batchSize)
      } catch (e) {
        this.activeQuery = false
        throw new TeradataError(this.DRIVER_ID,e,operation)
      }
  }
  
  _fetchMore(batchSize) {
      
      let operation
      try {
        if (this.activeQuery === false) {
          return []
        }
        operation = `cursor.fetchmany(${batchSize})`
		// const startTime = performance.now()
        const results = this.cursor.fetchmany(batchSize)
		// console.log('fetchMore',performance.now() - startTime)
        if (results.length === 0) {
          this.activeQuery = false;
        }
        return results
      } catch (e) {
		this.activeQuery = false
        throw new TeradataError(this.DRIVER_ID,e,operation)
      }
  }
  
  _executeSQL(sqlStatement,args) {
      const operation =`cursor.execute(${sqlStatement})`
      try {
        this.cursor.execute(sqlStatement,args)
		return this.cursor.fetchall()
      } catch (e) {
		const cause = new TeradataError(this.DRIVER_ID,e,operation)
        throw cause
      }
    }  

  _disconnect() {
    let operation
    try {
      operation = `TeradataConnection.close()`
      this.teradataConnection.close();
    } catch (e) {
      throw new TeradataError(this.DRIVER_ID,e,operation)
    }
  }
}

const teradataWorker = new TeradataWorker()
  parentPort.on('message', (message) => {
   const action = message.action
   switch (action) {
	 case 'executeSQL':
	   try {
		 const results = teradataWorker._executeSQL(message.sql, message.args)
		 parentPort.postMessage({action:action, sql: message.sql, success: true, rows: results})
       } catch(e) {
		 parentPort.postMessage({action: action, sql: message.sql, success: false, cause: Object.assign({},e),  name : e.constructor.name})
       }  
       break;
     case 'connect': 
       try {
         teradataWorker._connect(message.connectionProperties);
         parentPort.postMessage({action: action, success: true})
       } catch(e) {
         parentPort.postMessage({action: action, success: false, cause: e})
       }
       break;
     case 'disconnect': 
       try {
         teradataWorker._disconnect();
         parentPort.postMessage({action: action, success: true})
       } catch(e) {
         parentPort.postMessage({action: action, success: false, cause: e})
       }
       break;
     case 'query': 
       try {
         const results = teradataWorker._query(message.sql, message.batchSize)
         parentPort.postMessage({action:action, success: true, rows: results})
       } catch(e) {
         parentPort.postMessage({action: action, success: false, cause: e})
       }  
       break;
    case  'fetchMore':
      try {
         const results = teradataWorker._fetchMore(message.batchSize)
         parentPort.postMessage({action:action, success: true, rows: results})
       } catch(e) {
         parentPort.postMessage({action: action, success: false, cause: e})
       }  
       break;
    case 'cancelQuery':
      try {
         const results = teradataWorker._cancelQuery()
         parentPort.postMessage({action:action, success: true, rows: results})
       } catch(e) {
         parentPort.postMessage({action: action, success: false, cause: e})
       }  
       break;
	/* 
	**
	** VIA EXECUTE SQL
	**
    case 'writeMany':
      try {
         const results = teradataWorkerthis._writeMany(message.batch)
         parentPort.postMessage({action:action, success: true, rows: results})
       } catch(e) {
         parentPort.postMessage({action: action, success: false, cause: e})
       }  
       break;
    case 'beginTransction':
      try {
         const results = teradataWorker._beginTransction()
         parentPort.postMessage({action:action, success: true, rows: results})
       } catch(e) {
         parentPort.postMessage({action: action, success: false, cause: e})
       }  
       break;
    case 'commitTransaction':
      try {
         const results = teradataWorker._commitTransaction()
         parentPort.postMessage({action:action, success: true, rows: results})
       } catch(e) {
         parentPort.postMessage({action: action, success: false, cause: e})
       }  
       break;
    case 'rollbackTransaction':
      try {
         const results = teradataWorker._rollbackTransaction()
         parentPort.postMessage({action:action, success: true, rows: results})
       } catch(e) {
         parentPort.postMessage({action: action, success: false, cause: e})
       }  
       break;
	*/
    default:
  }
})


"use strict";

const { performance } = require('perf_hooks');

const Yadamu = require('../common/yadamu.js')
const YadamuLibrary = require('../common/yadamuLibrary.js')
const DBReader = require('../common/dbReader.js');									 

class DBReaderMaster extends DBReader {  

  constructor(dbi,mode,status,yadamuLogger,options) {
    super(dbi,mode,status,yadamuLogger,options); 
    this.dbi.sqlTraceTag = `/* Master */`;	
  }
    
  async processTables() {
	  
	/*
	**
	** Create a Pool of dbWriterSlaves that will execute operations table copy operations.
	** The number of writers is determined by the parameter PARALLEL
	** Each Writer will be assinged a table to process. 
	** When a copy is complete the Writer will be assigned a new table to process.
	** When all tables have been processed the Writer will free it's database connection and return  value.
	**
    ** Each Writer is allocated a connection from the Reader connection pool.
	** The same connection will be used to read all the tables processed by the Writer.
	**
	** The Writer requests a task from the taskList.
    ** It creates a reader slave for the table and processes all the rows in the table.
	** It then requests another task from the tasklist.
	**
	** When the task list is empty The Reader connection is returned to the pool.
	**
	*/
	
	const maxSlaveCount = parseInt(this.dbi.parameters.PARALLEL)
	const slaveCount = this.schemaInfo.length < maxSlaveCount ? this.schemaInfo.length : maxSlaveCount
    const tasks = [...this.schemaInfo]
    const parallelTasks = Array(slaveCount).fill(0)
    const dbWriterSlavePool = await Promise.all(parallelTasks.map(async function(dummy,idx) {
      const dbWriterSlave = await this.outputStream.newSlave(idx)		 
      const slaveReaderDBI = await this.dbi.slaveDBI(idx);
	  while (tasks.length > 0) {
		const task = tasks.shift();
		dbWriterSlave.write({table: task.TABLE_NAME})
        const copyOperation = this.createCopyOperation(slaveReaderDBI,task,dbWriterSlave)
        const startTime = performance.now()
        let stats = {}
	    try {
          // this.yadamuLogger.trace([`${this.constructor.name}`,`${task.TABLE_NAME}`,`START`],``)
          stats = await copyOperation
		  
          const elapsedTime = performance.now() - startTime
          // this.yadamuLogger.trace([`${this.constructor.name}`,`${task.TABLE_NAME}`,`SUCCESS`],`Rows read: ${stats.rowsRead}. Elaspsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s. Throughput: ${Math.round((stats.rowsRead/elapsedTime) * 1000)} rows/s.`)
       	} catch(e) {
		  // Slave raised unrecoverable error. Slave cannot process any more tables.
          // this.yadamuLogger.trace([`${this.constructor.name}`,`${task.TABLE_NAME}`,`FAILED`],`Rows read: ${stats.rowsRead}.`)
          this.yadamuLogger.handleException([`${slaveReaderDBI.DATABASE_VENDOR}`,`Reader`,`${task.TABLE_NAME}`,`COPY`],e)
		  this.outputStream.setSlaveException(e);
		  if (['FLUSH','SKIP'].indexOf(this.dbi.parameters.READ_ON_ERROR) === -1) {
            tasks.length = 0;
			/*
			dbWriterSlavePool.forEach(function (dbWriterSlave) {
			  dbWriterSlave.abortTable()
			},this)
			*/
		  }
		  break;
        }
 		stats.tableName = task.TABLE_NAME
        dbWriterSlave.write({eod: stats})
	  }
	  await slaveReaderDBI.releaseSlaveConnection()
	  dbWriterSlave.end()
	  return dbWriterSlave 
	},this))
	
	// All slaves terminated and been instructed to close their connections. 
	// Wait for all Writer slaves to complete final inserts and close connections before invoking master's final() method.
	
  }
      
  async _read() {
    // console.log(new Date().toISOString(),`${this.constructor.name}.read`,this.nextPhase); 
    try {
       switch (this.nextPhase) {
         case 'systemInformation' :
           const systemInformation = await this.getSystemInformation();
		   // Needed in case we have to generate DDL from the system information and metadata.
           this.dbi.setSystemInformation(systemInformation);
           this.push({systemInformation : systemInformation});
           if (this.mode === 'DATA_ONLY') {
             this.nextPhase = 'metadata';
           }
           else { 
             this.nextPhase = 'ddl';
           }
           break;
         case 'ddl' :
           let ddl = await this.getDDLOperations();
           if (ddl === undefined) {
             // Database does not provide mechansim to retrieve DDL statements used to create a schema (eg Oracle's DBMS_METADATA package)
             // Reverse Engineer DDL from metadata.
             const metadata = await this.getMetadata();
             await this.generateStatementCache(metadata)
             ddl = Object.keys(this.dbi.statementCache).map(function(table) {
               return this.dbi.statementCache[table].ddl
             },this)
           } 
           this.push({ddl: ddl});
		   this.nextPhase = this.mode === 'DDL_ONLY' ? 'finished' : 'metadata';
           break;
         case 'metadata' :
           const metadata = await this.getMetadata();
		   const self = this

           // ### Defined here so self is the Reader not the Writer in these event handlers
		   
		   this.outputStream.on('ReadyForData',function() {
			 if (self.schemaInfolength === 0) {
	           self.yadamuLogger.info([`${self.dbi.DATABASE_VENDOR}`,'Master','Reader'],`No tables found.`);
			 }
			 else {
  			   self.processTables()
			 }
		   });
		   
           this.outputStream.on('AllDataWritten',async function(err) {
			 // If AllDataWritten is raised with an Error destory the reader with the error emitted by the DBWriter.
			 // otherwise push a NULL indicating that the reader has finished reading.
			 if (err instanceof Error) {
			   try {
   	             await self.dbi.closeConnection();
			   } catch (e) {
                 self.yadamuLogger.logException([`${self.constructor.name}.onAllDataWritten()`],e);
			   }
               self.destroy(err)
			 }
			 else {
               await self.dbi.finalizeExport();
			   await self.dbi.releaseMasterConnection();
	           self.push(null);
			 }
           });
		   
           this.push({metadata: metadata});
		   this.nextPhase = this.schemaInfo.length === 0 ? 'finished' : 'data';
		   break;
		 case 'finished':
		   // 'finsihed' occurs in DDL_ONLY mode and when copying a schema that contains no tables.
		   // 'finished' will not occur when copying a schema that contains tables in DATA_ONLY or DDL_AND_DATA modes. In this case finished processing takles place when the the writer emits 'AllDataWritten'
		   await this.dbi.finalizeExport();
		   await this.dbi.releaseMasterConnection();
	       this.push(null);
         default:
      }
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._read()`],e);
	  await this.dbi.releaseMasterConnection();
      this.destroy(e)
    }
  }
}

module.exports = DBReaderMaster;
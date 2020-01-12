"use strict";

const { performance } = require('perf_hooks');

const Yadamu = require('../common/yadamu.js')
const YadamuLibrary = require('../common/yadamuLibrary.js')
const DBReader = require('../common/dbReader.js');									 

class DBReaderMaster extends DBReader {  

  constructor(dbi,mode,status,yadamuLogger,options) {
    super(dbi,mode,status,yadamuLogger,options); 
    this.dbi.sqlTraceTag = `/* Master */`;	
    const self = this
  }
    
  async createTableReader(dbi,tableMetadata,outputStream) {
    const tableInfo = this.dbi.generateSelectStatement(tableMetadata)
    const parser = dbi.createParser(tableInfo,outputStream.objectMode())
    const inputStream = await dbi.getInputStream(tableInfo,parser)

    const self = this
    
    const tableReader = new Promise(function(resolve,reject) {  

      const outputStreamError = function(err){
        // Named OnError Listener
		self.yadamuLogger.logException([`${self.constructor.name}.createTableReader()`,`${tableMetadata.TABLE_NAME}`,`${this.constructor.name}.onError()`],err)
	    reject(err)
      }       

      outputStream.on('error',
	    outputStreamError
	  );

      parser.on('end',
	    function() {
		  outputStream.removeListener('error',outputStreamError)
	      resolve(parser.getCounter())
      })
	  
	  parser.on('error',
	    function(err){
		  reject(err)
      });

	  const stack = new Error().stack;
	  inputStream.on('error',
	    function(err) { 
		  if (err.yadamuHandled === true) {
	        self.yadamuLogger.info([`${self.constructor.name}.createTableReader()`,`${tableMetadata.TABLE_NAME}`],`Rows read: ${parser.getCounter()}. Read Pipe Closed`)
	      } 
		  reject(self.dbi.processStreamingError(err,stack,tableInfo))
	    }
      )		

	  try {
        inputStream.pipe(parser).pipe(outputStream,{end: false })
	  } catch (e) {
		self.yadamuLogger.logException([`${self.constructor.name}.createTableReader()`,`${tableMetadata.TABLE_NAME}`,`PIPE`],e)
		throw e
	  }
    })
    
	return tableReader
  }
    
  async processTables() {
	  
	// Create a Set of TableWriters that will execute operations table copy operations.
	// The number of writers is determined by the parameter PARALLEL
	// Each Writer will be assinged a table to process. 
	// When a table copy is complete ti will be assigned a new table to process.
	// When all tables have been processed the Writer will free it's database connection and return  value.
	  
	const maxSlaveCount = parseInt(this.dbi.parameters.PARALLEL)
	const slaveCount = this.schemaInfo.length < maxSlaveCount ? this.schemaInfo.length : maxSlaveCount
	if (slaveCount > 0) {
      const tasks = [...this.schemaInfo]
	  const parallelTasks = Array(slaveCount).fill(0)
      const tableWriters = await Promise.all(parallelTasks.map(async function(dummy,idx) {
    	 const tableWriter = await this.outputStream.newSlave(idx)		 
         const tableReaderDBI = await this.dbi.newSlaveInterface(idx);
		 while (tasks.length > 0) {
		   const task = tasks.shift();
		   tableWriter.write({table: task.TABLE_NAME})
           const tableReader = this.createTableReader(tableReaderDBI,task,tableWriter)
           const startTime = performance.now()
	       try {
             const rows = await tableReader
             const elapsedTime = performance.now() - startTime
             this.yadamuLogger.info([`${this.constructor.name}`,`${task.TABLE_NAME}`],`Rows read: ${rows}. Elaspsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.`)
       	   } catch(e) {
			 // Slave raised unrecoverable error. Slave cannot process any more tables.
             this.yadamuLogger.logException([`${this.constructor.name}`,`${task.TABLE_NAME}`,`COPY`],e)
			 this.outputStream.setSlaveException(e);
			 break;
           }
           tableWriter.write({eod: task.TABLE_NAME})
		 }
		 try {
		   await tableReaderDBI.releaseConnection();
		 } catch (e) {
           this.yadamuLogger.logException([`${this.constructor.name}`,`${tableReaderDBI.constructor.name}.releaseConnection()`],e)
		 }
		 return tableWriter 
	  },this))
	  
      await Promise.all(tableWriters.map(function(tableWriter) {
		const self = this;
        return new Promise(function(resolve,reject) {
		  tableWriter.write({releaseSlave: null},undefined,function(err) {
            self.outputStream.write({slaveReleased: null})
	        resolve()
		  })
		})
	  },this));
	}
	else {
      this.yadamuLogger.info([`${this.constructor.name}`],`No tables found.`);
	}
	// All slaves terminated and been instructed to close their connections. 
	// Need to wait for all Writer slaves to complete final inserts and close connections before invoking master's final() method.
	// Push a null to the master writer to force the final method to be invoked.
  }
      
  async _read() {
    // console.log(new Date().toISOString(),`${this.constructor.name}.read`,this.nextPhase); 
    try {
       switch (this.nextPhase) {
         case 'systemInformation' :
           const systemInformation = await this.getSystemInformation(Yadamu.EXPORT_VERSION);
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
		   this.outputStream.on('ReadyForData',
		     function() {
			   self.processTables()
		     }
		   );
           this.outputStream.on('AllDataWritten',
		     async function(err) {
			   // If AllDataWritten supplies an Error  destory the reader with the error sent by the Master DBWriter.
			   try {
                 await self.dbi.finalizeExport();
	             if (err instanceof Error) {
                   self.destroy(err);				   
				 }
			     else {
				   self.push(null)
				 }
			   } catch (e) {
                 self.yadamuLogger.logException([`${this.constructor.name}.onAllDataWritten()`],e);
				 e = (err instanceof Error) ? err : e
                 self.destroy(e)
			   }
		     }
		   );
           this.push({metadata: metadata});
		   this.nextPhase = this.schemaInfo.length === 0 ? 'finished' : 'data';
		   break;
		 case 'data':
			this.nextPhase = 'finished'
			break;
		 case 'finished':
            await this.dbi.finalizeExport();
	        this.push(null);
            break;
         default:
      }
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._read()`],e);
      this.destroy(e)
    }
  }
}

module.exports = DBReaderMaster;
"use strict";
const Readable = require('stream').Readable;
const Yadamu = require('./yadamu.js')
const YadamuLibrary = require('./yadamuLibrary.js')
const { performance } = require('perf_hooks');

class DBReader extends Readable {  

  constructor(dbi,mode,status,yadamuLogger,options) {

    super({objectMode: true });  
 
    this.dbi = dbi;
    this.mode = mode;
    this.status = status;
    this.yadamuLogger = yadamuLogger;
    this.yadamuLogger.info([`${this.constructor.name}`,`${dbi.DATABASE_VENDOR}`],`Ready. Mode: ${this.mode}.`)
       
    this.schemaInfo = [];
    
    this.nextPhase = 'systemInformation'
    this.ddlCompleted = false;
    this.outputStream = undefined;
  }
 
  isDatabase() {
    return true;
  }
    
  pipe(target,options) {
    this.outputStream = super.pipe(target,options);
	return this.outputStream;
  } 
  
  async initialize() {
	await this.dbi.initializeExport() 
  }
  
  async getSystemInformation(version) {
    return this.dbi.getSystemInformation(version)
  }
  
  async getDDLOperations() {
	const startTime = performance.now();
    const ddl = await this.dbi.getDDLOperations()
	if (ddl !== undefined) {
      this.yadamuLogger.info([`${this.dbi.constructor.name}.getDDLOperations()`],`Generated ${ddl.length} DDL statements. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
	}
	return ddl
  }
  
  async getMetadata() {
      
     const startTime = performance.now();
     this.schemaInfo = await this.dbi.getSchemaInfo('FROM_USER')
     this.yadamuLogger.ddl([`${this.constructor.name}.getMetadata()`],`Generated metadata for ${this.schemaInfo.length} tables. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
     return this.dbi.generateMetadata(this.schemaInfo)
  }
      
  async createCopyOperation(dbi,tableMetadata,outputStream) {
	  
    /* 
	**
	** Input Stream has failed. 3 options
    **   ABORT : Skip buffered records. Terminate YADAMU.
	**   SKIP  : Skip buffered records. If connection was lost attempt to get a new connection. Continue Processing. 
	**   FLUSH : Write buffered Records. If connection was lost attempt to get a new connection. Continue processing.
	**
	** Some Databases seem to raise multiple events for a given error. Only process the first event for a given record.
	**   MSSQL : Will raise Connect Lost twice 
	** 
	*/ 
	
	/*
	**
	** The writer does not 'end' until all tables have been processed.
	** The Output Stream Error handler needs to be set for each table processed, so that it gets the table specific 'reject' function.
	** Once the table has been processed the error handler needs to be removed.
	**
	*/
 
    const self = this
    const stack = new Error().stack;
    // const startTime = performance.now();
	let cause = undefined
    let copyFailed = false;
	let tableMissing = false;
 
    const tableInfo = dbi.generateSelectStatement(tableMetadata)
    const parser = dbi.createParser(tableInfo,outputStream.objectMode())

    // ### TESTING ONLY: Uncomment folllowing line to force Table Not Found condition
    // tableInfo.SQL_STATEMENT = tableInfo.SQL_STATEMENT.replace(tableInfo.TABLE_NAME,tableInfo.TABLE_NAME + "1")

    const inputStream = await dbi.getInputStream(tableInfo,parser)
 
    const copyOperation = new Promise(function(resolve,reject) {  
	
	  let pipeStartTime;
	  let readerEndTime;
	  let parserEndTime;
	  
      const outputStreamError = function(err){
        // Named OnError Listener
		self.yadamuLogger.logException([`${self.constructor.name}.copyOperation()`,`${this.constructor.name}.onError()`,`${tableMetadata.TABLE_NAME}`],err)
		reject(err)
	  }       
    
	  outputStream.on('error',
	    outputStreamError
	  );
	  
	  /* 
	  **
	  ** The listener needs to be removed when the writer completes processing the 'eod' message since the writer's end() event is supressed to allow for writing multiple tables to the same stream.
	  **
	  */
	  
	  outputStream.setErrorHandler(outputStreamError)

      parser.on('end',
	    async function(){

   	      // self.yadamuLogger.trace([`${self.constructor.name}.copyOperation()`,`${parser.constructor.name}.onEnd()}`,`${tableMetadata.TABLE_NAME}`,dbi.parameters.READ_ON_ERROR],`${copyFailed ? 'FAILED' : 'SUCCSESS'}. Stream open ${YadamuLibrary.stringifyDuration(performance.now() - pipeStartTime)}.`);

    	  parserEndTime = performance.now()
		  const pipeStatistics = {rowsRead: parser.getCounter(), pipeStartTime: pipeStartTime, readerEndTime: readerEndTime, parserEndTime: parserEndTime, copyFailed: copyFailed, tableNotFound: tableMissing}
          
		  
		  /*
		  **
		  ** OracleDBI overides freeInputStream() to prevent non-standard close() event from firing and raising an NJS-018 exception since the input stream's connection has been closed
		  **
		  */
  	      await dbi.freeInputStream(tableMetadata,inputStream);
		  
		  if (copyFailed) {
            switch (dbi.parameters.READ_ON_ERROR) {
	          case undefined:
		      case 'ABORT':
			    const rows = pipeStatistics.rowCount + 1;
				// self.yadamuLogger.warning([`${this.constructor.name}`,`${tableMetadata.TABLE_NAME}`],`Reader failed at row: ${rows}.`)
				outputStream.reportTableComplete(pipeStatistics);
    		    reject(cause);
                break;
	          case 'SKIP':
              case 'FLUSH':
		        /*
		        **
		        ** InputStream's database connection may not be open at this point if end() fires after inputStream raised an error() event due to back end disconnecting
		        **
		        */
		        if (cause && cause.lostConnection()) {
	              // self.yadamuLogger.trace([`${self.constructor.name}.copyOperation()`,`${inputStream.constructor.name}.onError()`,`${tableInfo.TABLE_NAME}`,`${cause.code}`],`Lost Connection: ${cause.lostConnection()}`); 
		          await dbi.reconnect(cause)
		        }
    		    break;
		    }
  		  }
		  resolve(pipeStatistics)    		  
	    }
	  )

	  parser.on('error',
	    function(err) { 	 
		  // Only report and process first error
		  if (!copyFailed) {
  	        switch (dbi.parameters.READ_ON_ERROR) {
		      case undefined:
			  case 'ABORT':
	          case 'SKIP':
			    outputStream.abortTable();
              case 'FLUSH':
  			    break;
  		    }     		   
			copyFailed = true;
            cause = dbi.streamingError(err,stack,tableMetadata)
   		    self.yadamuLogger.logException([`${self.constructor.name}.copyOperation()`,`${parser.constructor.name}.onError()}`,`${tableMetadata.TABLE_NAME}`,dbi.parameters.READ_ON_ERROR],cause);
		  } 
        }
	  );

      inputStream.on('end',
	    function() {
			// self.yadamuLogger.trace([`${self.constructor.name}.copyOperation()`,`${inputStream.constructor.name}.onEnd()}`,`${tableMetadata.TABLE_NAME}`,dbi.parameters.READ_ON_ERROR],`${copyFailed ? 'FAILED' : 'SUCCSESS'}`);
			readerEndTime = performance.now()
	    }
	  )

	  inputStream.on('error',
	    function(err) { 	 
		  // Only report and process first error
          // self.yadamuLogger.trace([`${self.constructor.name}.copyOperation()`,`${inputStream.constructor.name}.onError()`,`${tableInfo.TABLE_NAME}`,`${err.code}`],`Stream open ${YadamuLibrary.stringifyDuration(performance.now() - pipeStartTime)}.`); 
		  if (!copyFailed) {
  	        switch (dbi.parameters.READ_ON_ERROR) {
		      case undefined:
			  case 'ABORT':
	          case 'SKIP':
			    outputStream.abortTable();
              case 'FLUSH':
  			    break;
  		    }     		   
			copyFailed = true;
            cause = dbi.streamingError(err,stack,tableMetadata)
			if (cause.missingTable()) {
			  tableMissing = true;
			}
			else {
              self.yadamuLogger.logException([`${self.constructor.name}.copyOperation()`,`${inputStream.constructor.name}.onError()}`,`${tableMetadata.TABLE_NAME}`,dbi.parameters.READ_ON_ERROR],cause);
		    }
			
			/*
			**
			** It appears that some implementations do not always raise end() after error(). 
			** Explicitly push a NULL seems to force end() 
			**
			** Oracle does not raise end() following a lost connection error
			**
			*/
			
			if (dbi.forceEndOnInputStreamError(cause)) {
		      parser.push(null);
			}
		  } 
		}
      );
	  
	  try {
		pipeStartTime = performance.now();
        inputStream.pipe(parser).pipe(outputStream,{end: false })
	  } catch (e) {
		self.yadamuLogger.logException([`${self.constructor.name}.copyOperation()`,`${tableMetadata.TABLE_NAME}`,`PIPE`],e)
		reject(e)
	  }
    })
    
	return copyOperation
  }
  
  async copyContent(tableMetadata,outputStream) {
	 
    const copyOperation = this.createCopyOperation(this.dbi,tableMetadata,outputStream);
    const startTime = performance.now()
	try {
      const readerStatistics = await copyOperation;
      const elapsedTime = performance.now() - startTime
      // this.yadamuLogger.trace([`${this.constructor.name}`,`${tableMetadata.TABLE_NAME}`],`Rows read: ${rows}. Elaspsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.`)
	  await this.dbi.finalizeRead(tableMetadata);
      return readerStatistics
	} catch(e) {
	  /*
	  **
	  ** The copyOperation had failed...
	  **
	  */      
	  this.yadamuLogger.logException([`${this.constructor.name}.copyContent()`,`${tableMetadata.TABLE_NAME}`,`COPY`],e);
	  throw e;
    }
      
  }
  
  async generateStatementCache(metadata) {
    if (Object.keys(metadata).length > 0) {   
      // ### if the import already processed a DDL object do not execute DDL when generating statements.
      Object.keys(metadata).forEach(function(table) {
         metadata[table].vendor = this.dbi.systemInformation.vendor;
      },this)
    }
    this.dbi.setMetadata(metadata)      
    await this.dbi.generateStatementCache('%%SCHEMA%%',false)
  }
  
  getInputStream() {
    if (this.dbi.isDatabase()) {
      // dbReader.js provides the ordered event stream for random (database based) readers.
      return this
    }
    else {
      // For File based readers the event stream is generated by the order of elements in the document beging parsed
      return this.dbi.getInputStream()
    }
  }
  
  async _read() {
    // Error().stack(new Date().toISOString(),`${this.constructor.name}.read`,this.nextPhase); 
    try {
       const self = this;
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
           this.push({metadata: metadata});
		   this.nextPhase = this.schemaInfo.length === 0 ? 'finished' : 'data';
           break;   
         case 'data' :
		   const task = this.schemaInfo.shift();
		   // Only push once when finished
		   await new Promise(async function(resolve,reject) {
		     self.outputStream.write({table: task.TABLE_NAME},undefined,
			   function(err) {
                resolve()
		       })
		   })
           const stats = await this.copyContent(task,this.outputStream)
		   stats.tableName = task.TABLE_NAME
           this.push({eod: stats})
	       this.nextPhase = this.schemaInfo.length === 0 ? 'finished' : 'data';
		   break;
		 case 'finished':
           await this.dbi.finalizeExport();
		   this.push(null);
           break;
         default:
      }
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._read()`],e);
      this.destroy(e);
    }
  }
}

module.exports = DBReader;
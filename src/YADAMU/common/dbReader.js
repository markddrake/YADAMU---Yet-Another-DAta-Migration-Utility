"use strict";

const { performance } = require('perf_hooks');
const util = require('util')
const Readable = require('stream').Readable;
const stream = require('stream')
const PassThrough = stream.PassThrough
//const pipeline = util.promisify(stream.pipeline);
const { pipeline } = require('stream/promises');
const finished = stream.finished

const Yadamu = require('./yadamu.js')
const YadamuLibrary = require('./yadamuLibrary.js')
const YadamuWriter = require('./yadamuWriter.js')
const {YadamuError, ExportError, DatabaseError, IterativeInsertError, InputStreamError} = require('./yadamuException.js')

const YadamuConstants = require('./yadamuConstants.js')

class DBReader extends Readable {  

  /* 
  **
  ** The DBReader is responsibe for replicating the source data source to the target source.
  **
  ** The DBReader starts by sending "systemInformation", "ddl" "metadata" and "pause" messages directly to the DBWriter.
  ** Then it waits for the DBWriter to raise 'ddlCompelete' before sending the contents of the tables.
  **
  ** A seperate set of table level readers and writers are used to send the table contents.
  ** If the target is a database, the table level operations can execute sequentially or in parallel.
  ** If the target is a file the table level operations must operate sequentially.
  ** 
  ** When the DBWriter recieves a pause message it caches the associated callback instead of invoking it. This has the effect of pausing the DBWriter. 
  ** When the DBReader has finished processing all the tables it resumes the DBWriter by invoking the cached callback.
  **
  */

  constructor(dbi,yadamuLogger,options) {

    super({objectMode: true });  
 
    this.dbi = dbi;
    this.status = dbi.yadamu.STATUS
    this.yadamuLogger = yadamuLogger;
    this.yadamuLogger.info([`Reader`,dbi.DATABASE_VENDOR,dbi.DB_VERSION,this.dbi.MODE,this.dbi.getWorkerNumber()],`Ready.`)
  
    this.metadata = undefined
    this.schemaInfo = [];
  
    this.nextPhase = 'systemInformation'
    this.dbWriter = undefined;		
  }

  isDatabase() {
    return this.dbi.isDatabase()
  }
    
  pipe(outputStream,options) {
	this.dbWriter = outputStream
	// If the target does not support Parallel operation overide the setting of PARALLEL
	if (!this.dbWriter.dbi.PARALLEL_WRITE_OPERATIONS) {
	  this.dbi.yadamu.parameters.PARALLEL = 0
	}
	return super.pipe(outputStream,options);
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
      this.yadamuLogger.ddl([this.dbi.DATABASE_VENDOR],`Generated ${ddl.length} DDL statements. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
	}
	return ddl
  }
  
  async getMetadata() {
      
     const startTime = performance.now();
     this.schemaInfo = this.dbi.applyTableFilter(await this.dbi.getSchemaInfo('FROM_USER'))
     this.yadamuLogger.ddl([this.dbi.DATABASE_VENDOR],`Generated metadata for ${this.schemaInfo.length} tables. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
     return this.dbi.generateMetadata(this.schemaInfo)
  }
  
  traceStreamEvents(streams,tableName) {

    // Add event tracing to the streams
	  
	streams[0].once('readable',() => {
	  console.log(streams[0].constructor.name,tableName,'readable')
	})
	
	/*
	  .on('read',() => {
	  console.log(streams[0].constructor.name,tableName,'read')
	}).on('data',() => {
	  console.log(streams[0].constructor.name,tableName,'data')
	})
	*/
	
    streams.forEach((s,idx) => {
	  s.once('end',() => {
	     console.log(s.constructor.name,tableName,'end')
	  }).once('finish', (err) => {
	    console.log(s.constructor.name,tableName,'finish')
	  }).once('close', (err) => {
        console.log(s.constructor.name,tableName,'close')
	  }).once('error', (err) => {
	    console.log(s.constructor.name,tableName,'error',err.message)
	  })
	}) 
  }
  
  async pipelineTable(task,readerDBI,writerDBI) {
	 
    let tableInfo
	let tableOutputStream

	const yadamuPipeline = []
	let streamEnded
	let streamFailed
   
    try {
      tableInfo = readerDBI.generateQueryInformation(task)
	  tableInfo.TARGET_DATA_TYPES = writerDBI.metadata?.[tableInfo.TABLE_NAME]?.dataTypes ?? []
	  
	  // ### TODO: Pass partitioning information to getOutputStreams() ???
	  
	  const inputStreams = await readerDBI.getInputStreams(tableInfo)
      yadamuPipeline.push(...inputStreams)
	  const outputStreams = await writerDBI.getOutputStreams(tableInfo.MAPPED_TABLE_NAME,this.dbWriter.ddlComplete)
	  yadamuPipeline.push(...outputStreams)

      tableOutputStream = outputStreams[0]
      tableOutputStream.setReaderMetrics(readerDBI.INPUT_METRICS)

    } catch (e) {
      this.yadamuLogger.handleException(['PIPELINE','STREAM INITIALIZATION',task.TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],e)
      throw (e)
    }

    // this.traceStreamEvents(yadamuPipeline,task.TABLE_NAME)
	// yadamuPipeline.forEach((s) => { console.log(task.TABLE_NAME,s.constructor.name, s.eventNames().map((e) => {return `"${e}(${s.listenerCount(e)})"`}).join(','))})
	const streamsCompleted = yadamuPipeline.map((s) => { 
	  return new Promise((resolve,reject) => {
		finished(s,(err) => {
          resolve()
		  // if (err) {reject(err)} else {resolve()}
		})
      })
    })
	
	try {
	  this.activeWorkers.add(yadamuPipeline[0])
	  // this.yadamuLogger.trace([this.constructor.name,'PIPELINE',tableInfo.MAPPED_TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`${yadamuPipeline.map((proc) => { return proc.constructor.name }).join(' => ')}`)
      readerDBI.INPUT_METRICS.pipeStartTime = performance.now();
	  await pipeline(...yadamuPipeline)
	  readerDBI.INPUT_METRICS.pipeEndTime = performance.now();
	  // this.yadamuLogger.trace([this.constructor.name,'PIPELINE',tableInfo.MAPPED_TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`${yadamuPipeline.map((proc) => { return `${proc.constructor.name}:${proc.destroyed}` }).join(' => ')}`)
      this.activeWorkers.delete(yadamuPipeline[0])  
	  // console.log(task.TABLE_NAME,streamsCompleted)
	  await Promise.allSettled(streamsCompleted)
	  // console.log(task.TABLE_NAME,streamsCompleted)
	} catch (err) {
	  // this.yadamuLogger.trace([this.constructor.name,'PIPELINE',tableInfo.MAPPED_TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.yadamu.ON_ERROR,'FAILED'],`${err.constructor.name},${err.message}`)
	 
	  // Wait for DDL operations to complete. Catch, Report and Throw DDL errors. If DDL is successful this becomes a no-op.
	  try {
		await this.dbWriter.ddlComplete
	  } catch (err) {
	    this.yadamuLogger.handleException(['PIPELINE','DDL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.ON_ERROR,readerDBI.getWorkerNumber()],err);
		throw err
      }

	  // When an the pipleline throws an exception not all componants of the pipeline have been finished processing. Wait all the streams to complete
      await Promise.allSettled(streamsCompleted)
	  this.activeWorkers.delete(yadamuPipeline[0])  
	  
	  // this.yadamuLogger.trace([this.constructor.name,'PIPELINE',tableInfo.MAPPED_TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`${yadamuPipeline.map((proc) => { return `${proc.constructor.name}:${proc.destroyed}` }).join(' => ')}`)
	    
	  const cause = readerDBI.INPUT_METRICS.readerError || readerDBI.INPUT_METRICS.parserError || yadamuPipeline.find((s) => {return s.underlyingError instanceof Error}).underlyingError || err
      if (readerDBI.ON_ERROR === 'ABORT') {
  	    // Throw the underlying cause if ON_ERROR handling is ABORT
        throw cause;
      }

      if (YadamuError.lostConnection(readerDBI.INPUT_METRICS.readerError) || YadamuError.lostConnection(readerDBI.INPUT_METRICS.parserError)) {
        // If the reader or parser failed with a lost connection error re-establish the input stream connection 
  	    await readerDBI.reconnect(cause,'READER')
      }
	  /*
	  else {
        this.yadamuLogger.handleException(['PIPELINE',tableInfo.MAPPED_TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],cause)
		tableOutputStream.destroy(cause);
	  }
	  */
	  
      this.dbi.resetExceptionTracking()
    }
  }
  
  async pipelineTables(taskList,readerDBI,writerDBI) {
	 
	this.activeWorkers = new Set();
    this.yadamuLogger.info(['PIPELINE','SEQUENTIAL',this.dbi.DATABASE_VENDOR,this.dbWriter.dbi.DATABASE_VENDOR],`Processing ${taskList.length} Tables`);
	  
	while (taskList.length > 0) {
	  const task = taskList.shift()
      // this.yadamuLogger.trace(['PIPELINE','SEQUENTIAL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,task.TABLE_NAME],`Processing Table`);
      try {
	    await this.pipelineTable(task,readerDBI,writerDBI)
	  } catch (cause) {
	    // this.yadamuLogger.trace(['PIPELINE','SEQUENTIAL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,task.TABLE_NAME],cause)
		this.yadamuLogger.handleException(['PIPELINE','SEQUENTIAL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,task.TABLE_NAME,readerDBI.ON_ERROR],cause)
		if (taskList.length > 0) {
		  this.yadamuLogger.error(['PIPELINE','SEQUENTIAL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.ON_ERROR],`Operation failed: Skipping ${taskList.length} Tables`);
	    }
		else {
		  this.yadamuLogger.warning(['PIPELINE','SEQUENTIAL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.ON_ERROR],`Operation failed.`);
		}				
	    // Throwing here raises 'ERR_STREAM_PREMATURE_CLOSE' on the Writer. Cache the cause 
		this.underlyingError = cause;
        throw cause
	  }
    }
  }
  
  async pipelineTableToFile(readerDBI,writerDBI,task) {
      
     // this.yadamuLogger.trace(['PIPELINE','SERIAL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,task.TABLE_NAME],`Processing Table`);
	 
	 const tableInfo = readerDBI.generateQueryInformation(task)
	 
	 // Get the Table Readers 
	 const sourcePipeline = await readerDBI.getInputStreams(tableInfo)
     const targetPipeline = writerDBI.getOutputStreams(tableInfo.MAPPED_TABLE_NAME)

     // targetPipeline.forEach((s) => { console.log( task.TABLE_NAME,s.constructor.name, s.eventNames().map((e) => {return `"${e}(${s.listenerCount(e)})"`}).join(','))})

	 targetPipeline[0].setReaderMetrics(readerDBI.INPUT_METRICS)
	 const tableSwitcher = targetPipeline[1]
	 const yadamuPipeline = new Array(...sourcePipeline,...targetPipeline)

     // this.traceStreamEvents(yadamuPipeline,task.TABLE_NAME)

   	 // console.log(yadamuPipeline.map((s) => { return s.constructor.name }).join(' ==> '))
	
	 const stack = new Error().stack
	 const tableComplete = new Promise((resolve,reject) => {
	   finished(tableSwitcher,(cause) => {
	     // Manually clean up the previous pipeline since it never completely ended. Prevents excessive memory usage and dangling listeners
		 // Remove unpipe listeners on targets, unpipe all target streams
	     targetPipeline.forEach((s,i) => { 
           s.removeAllListeners('unpipe')
		   if (i < targetPipeline.length-1) {
		     s.unpipe(targetPipeline[i+1])
		   }
		   s.removeAllListeners(); 
		 })
	   
	     // Destroy the source streams
	     sourcePipeline.forEach((s) => { s.destroy() })
		 if (cause instanceof Error) reject(new ExportError(task.TABLE_NAME,cause,stack))
         resolve()
  	   })
     })
   
     // targetPipeline.forEach((s) => { console.log(task.TABLE_NAME,s.constructor.name, s.eventNames().map((e) => {return `"${e}(${s.listenerCount(e)})"`}).join(','))})
     // this.traceStreamEvents(yadamuPipeline,task.TABLE_NAME)
	  
     // this.yadamuLogger.trace([this.constructor.name,'PIPELINE',tableInfo.TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`${yadamuPipeline.map((proc) => { return proc.constructor.name }).join(' => ')}`)
     pipeline(...yadamuPipeline).catch((cause) => {
	   // console.log('EMIT','ExportFailed')
	 })
	 
     await tableComplete
	 	 
  }

  async pipelineTablesToFile(taskList,readerDBI,writerDBI) {
	
  	  await this.dbWriter.ddlComplete
      this.yadamuLogger.info(['PIPELINE','SERIAL',this.dbi.DATABASE_VENDOR,this.dbWriter.dbi.DATABASE_VENDOR],`Processing ${taskList.length} Tables`);
	  
	  while (taskList.length > 0) {
	    const task = taskList.shift()
		try {
  	      await this.pipelineTableToFile(readerDBI,writerDBI,task)
		} catch (cause) {
		  this.yadamuLogger.handleException(['PIPELINE','SERIAL',task.TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],cause)
		  taskList.length = 0
		}
	  }
  }

  async generateStatementCache(metadata) {
    if (!YadamuLibrary.isEmpty(metadata)) {   
      // ### if the import already processed a DDL object do not execute DDL when generating statements.
      Object.keys(metadata).forEach((table) => {
         metadata[table].vendor = this.dbi.systemInformation.vendor;
      })
    }
    this.dbi.setMetadata(metadata)      
    await this.dbi.generateStatementCache('%%SCHEMA%%',false)
  }
  
  getInputStreams() {
	  
	/*
	**
	** For a database the DBReader class is repsonsible for generating the events in the required order. This is handled via the 'nextPhase' setting in the _read method.
	** Thus the DBReader becomes the event stream.
	**
	** For a File the events are generated according to the contents of the file.
	** The file parser becomes the event stream.
	*/
	
    if (this.dbi.isDatabase()){
      return [this]
    }
    else {
	  return this.dbi.getInputStreams()
    }

  }
 
  async _read() {
    try {
 	  // this.yadamuLogger.trace([this.constructor.name,`_READ()`,this.dbi.DATABASE_VENDOR],this.nextPhase)
      switch (this.nextPhase) {
         case 'systemInformation' :
           const systemInformation = await this.getSystemInformation();
		   // Needed in case we have to generate DDL from the system information and metadata.
           this.dbi.setSystemInformation(systemInformation);
		   this.dbi.yadamu.REJECTION_MANAGER.setSystemInformation(systemInformation)
		   this.dbi.yadamu.WARNING_MANAGER.setSystemInformation(systemInformation)
           this.push({systemInformation : systemInformation});
           if (this.dbi.MODE === 'DATA_ONLY') {
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
             this.metadata = await this.getMetadata();
             await this.generateStatementCache(this.metadata)
             ddl = Object.values(this.dbi.statementCache).map((table) => {
               return table.ddl
             })
           } 
           this.push({ddl: ddl});
		   this.nextPhase = 'metadata';
           break;
         case 'metadata' :
           this.metadata = this.metadata ? this.metadata : await this.getMetadata();
           this.push({metadata: this.dbi.transformMetadata(this.metadata,this.dbi.inverseTableMappings)});
		   this.dbi.yadamu.REJECTION_MANAGER.setMetadata(this.metadata)
		   this.dbi.yadamu.WARNING_MANAGER.setMetadata(this.metadata)
		   this.nextPhase = ((this.dbi.MODE === 'DDL_ONLY') || (this.schemaInfo.length === 0)) ? 'exportComplete' : 'copyData';
		   break;
		 case 'copyData':	   
           if (this.dbWriter.dbi.isDatabase()) {
		     await this.pipelineTables(this.schemaInfo,this.dbi,this.dbWriter.dbi);
	       }
	       else {
		     await this.pipelineTablesToFile(this.schemaInfo,this.dbi,this.dbWriter.dbi);
           }
		   // No 'break' - fall through to 'exportComplete'.
		 case 'exportComplete':
	       // this.yadamuLogger.trace([this.constructor.name,'DLL_COMPLETE',this.dbi.getWorkerNumber()],'DDL COMPLETE: WAITING')
	       await this.dbWriter.ddlComplete
           // this.yadamuLogger.trace([this.constructor.name,'DLL_COMPLETE',this.dbi.getWorkerNumber()],'DDL COMPLETE: PROCESSING')
		   this.dbWriter.callDeferredCallback()
		   this.push(null);
       	   break;
	    default:
      }
    } catch (cause) {
  	  this.yadamuLogger.handleException([`READER`,`READ`,this.nextPhase,this.dbi.DATABASE_VENDOR,this.dbi.yadamu.ON_ERROR],cause);
	  this.underlyingError = cause;
	  await this.dbi.releasePrimaryConnection();
      this.destroy(cause)
    }
  }  
   
  async _destroy(cause,callback) {
    // this.yadamuLogger.trace([this.constructor.name,this.dbi.isDatabase()],'_destroy()')
    try {
	  await this.dbi.finalizeExport();
	  await this.dbi.releasePrimaryConnection();
	  callback()
	} catch (e) {
      if (YadamuError.lostConnection(cause)) {
        callback(cause)
	  }
	  else {
        this.yadamuLogger.handleException([`READER`,`DESTROY`,this.dbi.DATABASE_VENDOR,this.dbi.yadamu.ON_ERROR],e);		
        callback(e)
      }
    }
  }
  
}

module.exports = DBReader;
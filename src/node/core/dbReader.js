"use strict";

import { performance }            from 'perf_hooks';
import { Readable, PassThrough  } from 'stream'
import { pipeline, finished }     from 'stream/promises';

import YadamuLibrary              from '../lib/yadamuLibrary.js'
import YadamuConstants            from '../lib/yadamuConstants.js'
import YadamuWriter               from '../dbi/base/yadamuWriter.js'

import Yadamu                     from './yadamu.js'
import {YadamuError, ExportError, DatabaseError, IterativeInsertError, InputStreamError} from './yadamuException.js'


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
    this.yadamuLogger.info([YadamuConstants.READER_ROLE,dbi.DATABASE_VENDOR,dbi.DATABASE_VERSION,this.dbi.getWorkerNumber()],`Ready.`)
  
    this.metadata = undefined
    this.schemaMetadata = [];
  
    this.nextPhase = 'systemInformation'
    this.dbWriter = undefined;		
	this.activeReaders = new Set()
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
  
  async getSourceMetadata() {
      
     const startTime = performance.now();
     this.schemaMetadata = this.dbi.applyTableFilter(await this.dbi.getSchemaMetadata())
     this.yadamuLogger.ddl([this.dbi.DATABASE_VENDOR],`Loaded metadata for ${this.schemaMetadata.length} tables. Elapsed time: ${YadamuLibrary.stringifyDuration(performance.now() - startTime)}s.`);
     return this.dbi.generateMetadata(this.schemaMetadata)
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
  
  setReader(pipeline) {
	const writer = pipeline.find((s) => { return s instanceof YadamuWriter})
	if (writer) {
      writer.setReader(pipeline[0])
	}
  }
  
  async pipelineTable(task,readerDBI,writerDBI) {
	  
    let queryInfo

	const yadamuPipeline = []
    const activeStreams = []
    let pipelineMetrics
	
    try {
      queryInfo = readerDBI.generateSQLQuery(task)
	  queryInfo.TARGET_DATA_TYPES = writerDBI.metadata?.[queryInfo.TABLE_NAME]?.dataTypes ?? []  
	  const inputStreams = await readerDBI.getInputStreams(queryInfo,writerDBI.PARSE_DELAY)
	  pipelineMetrics = inputStreams[0].COPY_METRICS
      yadamuPipeline.push(...inputStreams)
	  const outputStreams = await writerDBI.getOutputStreams(queryInfo.MAPPED_TABLE_NAME,pipelineMetrics)
	  yadamuPipeline.push(...outputStreams)
      activeStreams.push(...yadamuPipeline.map((s) => { return finished(s) }))
    } catch (e) {
      this.yadamuLogger.handleException(['PIPELINE','STREAM INITIALIZATION',task.TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],e)
      throw (e)
    }
 	
	try {
	  // Track active read streams.. Used to when aborting parallel operations
	  const activeReader = yadamuPipeline[0]
	  activeReader.once('close',() => {
		  
		 this.activeReaders.delete(activeReader)
	  })
	  this.activeReaders.add(activeReader)
	  // Pass the Reader to the YadamuWriter instance so it can calculate lost rows correctly in the event of an error
	  this.setReader(yadamuPipeline)
	  // this.yadamuLogger.trace([this.constructor.name,'PIPELINE',queryInfo.MAPPED_TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`${yadamuPipeline.map((s) => { return s.constructor.name }).join(' => ')}`)
      pipelineMetrics.pipeStartTime = performance.now();
	  await pipeline(...yadamuPipeline)
	  pipelineMetrics.pipeEndTime = performance.now();
	  // this.yadamuLogger.trace([this.constructor.name,'PIPELINE',queryInfo.MAPPED_TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`${yadamuPipeline.map((s) => { return `${s.constructor.name}:${s.destroyed}` }).join(' => ')}`)
	  
	  
	} catch (err) {
	  // this.yadamuLogger.trace([this.constructor.name,'PIPELINE',queryInfo.MAPPED_TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.yadamu.ON_ERROR,'FAILED'],`${err.constructor.name},${err.message}`)
	  
	  // Wait for any outstanding DDL operations to complete. Throw DDL errors. If the DDL phase was successful this becomes a no-op.
	  
	  try {
        // this.yadamuLogger.trace([this.constructor.name,'PIPELINE','FAILED','DDL_COMPLETE',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.ON_ERROR,readerDBI.getWorkerNumber(),task.TABLE_NAME],'WAITING')
		await this.dbWriter.dbi.ddlComplete
    	// this.yadamuLogger.trace([this.constructor.name,'PIPELINE','FAILED','DDL_COMPLETE',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.ON_ERROR,readerDBI.getWorkerNumber(),task.TABLE_NAME],'PROCESSING')
	  } catch (err) {
		throw err
      }
	  
	  // Wait for all components of the pipeline to finish before closing connections
	  
	  // this.yadamuLogger.trace([this.constructor.name,'PIPELINE','FAILED','STREAMS_COMPLETE',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.ON_ERROR,readerDBI.getWorkerNumber(),task.TABLE_NAME,`${yadamuPipeline.map((s) => { return `${s.constructor.name}`}).join(' => ')}`],'WAITING')
	  await Promise.allSettled(activeStreams)
      // this.yadamuLogger.trace([this.constructor.name,'PIPELINE','FAILED','STREAMS_COMPLETE',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.ON_ERROR,readerDBI.getWorkerNumber(),task.TABLE_NAME,`${yadamuPipeline.map((s) => { return `${s.constructor.name}`}).join(' => ')}`],'COMPLETED')
      
      // console.log(pipelineMetrics)
	  // this.yadamuLogger.trace([this.constructor.name,'PIPELINE',queryInfo.MAPPED_TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`${yadamuPipeline.map((s) => { return `${s.constructor.name}:[${s.readableLength},${s.writableLength}]` }).join(',')}`)
	  
	  // Determine the underlying cause of the error.
	  const cause = pipelineMetrics.readerError || pipelineMetrics.parserError || yadamuPipeline.find((s) => {return s.underlyingError instanceof Error})?.underlyingError || err

	  // Verify all components of the pipeline have been destroyed. 
	  // this.yadamuLogger.trace([this.constructor.name,'PIPELINE',queryInfo.MAPPED_TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`${yadamuPipeline.map((s) => { return `${s.constructor.name}:${s.destroyed}` }).join(' => ')}`)
      // yadamuPipeline.map((s) => { if (!s.destroyed){ s.destroy(cause)})	    

      if (readerDBI.ON_ERROR === 'ABORT') {
  	    // Throw the underlying cause if ON_ERROR handling is ABORT
        throw cause;
      }

      if (YadamuError.lostConnection(pipelineMetrics.readerError) || YadamuError.lostConnection(pipelineMetrics.parserError)) {
        // If the reader or parser failed with a lost connection error re-establish the input stream connection 
  	    await readerDBI.reconnect(cause,'READER')
      }
	  else {
        this.yadamuLogger.handleException(['PIPELINE',queryInfo.MAPPED_TABLE_NAME,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],cause)
	  }
	  
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
	** Thus the DBReader becomes the source of the events for the operation
	**
	** For a File the events are generated according to the contents of the file.
	** Thus File Parser becomes the source of the events for the operation
	**
	*/
	
    if (this.dbi.isDatabase()){
      return [this]
    }
    else {
	  return this.dbi.getInputStreams()
    }

  }
 
  async doRead() {
 
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
           this.metadata = await this.getSourceMetadata();
           await this.generateStatementCache(this.metadata)
           ddl = Object.values(this.dbi.statementCache).map((table) => {
             return table.ddl
           })
         } 
         this.push({ddl: ddl});
		 this.nextPhase = 'metadata';
         break;
       case 'metadata' :
         this.metadata = this.metadata ? this.metadata : await this.getSourceMetadata();
		 this.push({metadata: this.dbi.transformMetadata(this.metadata,this.dbi.inverseTableMappings)});
		 this.dbi.yadamu.REJECTION_MANAGER.setMetadata(this.metadata)
		 this.dbi.yadamu.WARNING_MANAGER.setMetadata(this.metadata)
		 this.nextPhase = ((this.dbi.MODE === 'DDL_ONLY') || (this.schemaMetadata.length === 0)) ? 'exportComplete' : 'copyData';
	     break;
       case 'copyData':	   
    	 await this.pipelineTables(this.schemaMetadata,this.dbi,this.dbWriter.dbi);
		 // No 'break' - fall through to 'exportComplete'.
       case 'exportComplete':
	     // this.yadamuLogger.trace([this.constructor.name,this.nextPhase],'DDL COMPLETE: WAITING')
	     await this.dbWriter.dbi.ddlComplete
         // this.yadamuLogger.trace([this.constructor.name,this.nextPhase],'DDL COMPLETE: PROCESSING')
		 // All data has been processed. The DBReader has beem reattched to the DBWriter. Release the DBWriter by invoking the pending callback.
		 // this.yadamuLogger.trace([this.constructor.name,this.nextPhase],'CALLBACK COMPLETE')
		 this.push(null);
       	 break;
	   default:
    }
  }  
   
  _read() {
    // this.yadamuLogger.trace([this.constructor.name,`READ`,this.dbi.DATABASE_VENDOR],this.nextPhase)
	this.doRead().then(() => {}).catch((cause) => {
	  this.yadamuLogger.handleException([`READER`,`READ`,this.nextPhase,this.dbi.DATABASE_VENDOR,this.dbi.yadamu.ON_ERROR],cause);
	  this.underlyingError = cause;
      this.destroy(cause)
    })
  }
  
  async doDestroy(err) {
    // this.yadamuLogger.trace([this.constructor.name,`destroy`,this.dbi.DATABASE_VENDOR],``)
	try {
	  // Try to clean up the DBI gracefully
      await this.dbi.finalizeExport();
	  await this.dbi.final()
	}
	catch (e) {
	  err = err || e
	}
	finally {
      // Forced clean-up of the DBI
	  await this.dbi.destroy(err)
	}
  }
   
  _destroy(err,callback) {
	  
	this.doDestroy(err).then(() => {
	  callback(err)
	}).catch((cause) => {
      if (YadamuError.lostConnection(cause)) {
        callback(cause)
	  }
	  else {
        this.yadamuLogger.handleException([`READER`,`FINAL`,this.dbi.DATABASE_VENDOR,this.dbi.yadamu.ON_ERROR],cause);		
        callback(err)
      }
    })
  }  
}

export { DBReader as default}
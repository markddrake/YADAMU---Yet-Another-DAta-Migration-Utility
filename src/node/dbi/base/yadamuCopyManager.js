
import YadamuLibrary       from '../../lib/yadamuLibrary.js'

import DBIConstants        from './dbiConstants.js';

class YadamuCopyManager {
 
  get LOGGER()             { return this._LOGGER }
  set LOGGER(v)            { this._LOGGER = v }
  get DEBUGGER()           { return this._DEBUGGER }
  set DEBUGGER(v)          { this._DEBUGGER = v }

  constructor(dbi,credentials,yadamuLogger) {
    this.dbi = dbi
	this.credentials = credentials

	this.LOGGER   = yadamuLogger || this.dbi.LOGGER
	this.DEBUGGER = this.dbi.DEBUGGER
  } 
  
  async reportCopyResults(tableName,copyState) {

    const elapsedTime = copyState.endTime - copyState.startTime
	const throughput = Math.round((copyState.read/elapsedTime) * 1000)
    const writerTimings = `Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s. Throughput: ${throughput} rows/s.`
   
    let rowCountSummary
    switch (copyState.skipped) {
	  case 0:
	    rowCountSummary = `Rows ${copyState.read}.`
        this.LOGGER.info([`${tableName}`,`Copy`],`${rowCountSummary} ${writerTimings}`)  
        break
      default:
	    rowCountSummary = `Read ${copyState.read}. Written ${copyState.read - copyState.skipped}.`
        this.LOGGER.error([`${tableName}`,`Copy`],`${rowCountSummary} ${writerTimings}`)  
        await this.dbi.reportCopyErrors(tableName,copyState)
	}
	
	/*
	if (copyStatements.hasOwnProperty('partitionCount')) {
	  this.dbi.yadamu.recordPartitioncopyState(tableName,copyState);  
	}   
	else {
	}
	*/
	
	this.dbi.yadamu.recordMetrics(tableName,copyState);  
  }
  
  async copyOperation(taskList,worker,mode,sourceVendor) {

    let result = undefined
	let operationAborted = false;
	await worker.workerReady

    try {
      while (taskList.length > 0) {
	    const task = taskList.shift();
		const copyState = DBIConstants.PIPELINE_STATE
	    copyState.insertMode = 'COPY'
	    const results = await worker.copyOperation(task.TABLE_NAME,task.copyOperation,copyState)
		this.reportCopyResults(task.TABLE_NAME,copyState)
      }
	} catch (cause) {
	  result = cause
	  // this.LOGGER.trace(['PIPELINE','COPY',sourceVendor,worker.DATABASE_VENDOR,this.dbi.getWorkerNumber()],cause)
	  this.LOGGER.handleException(['PIPELINE','COPY',sourceVendor,worker.DATABASE_VENDOR,this.dbi.getWorkerNumber()],cause)
	  if ((this.dbi.ON_ERROR === 'ABORT') && !operationAborted) {
        operationAborted = true
		if (taskList.length > 0) {
	      this.LOGGER.error(['PIPELINE','COPY',concurrency,sourceVendor,worker.DATABASE_VENDOR,worker.ON_ERROR],`Operation failed: Skipping ${taskList.length} Tables`);
		}
		else {
	      this.LOGGER.warning(['PIPELINE','COPY',concurrency,sourceVendor,worker.DATABASE_VENDOR,worker.ON_ERROR],`Operation failed.`);
		}		
        taskList.length = 0;
      }
	}
    if (!worker.isManager()) {
	  await worker.destroyWorker() 
	}
	return result
  }

  async copyOperations(taskList,sourceVendor) {
	 
    this.dbi.activeWorkers = new Set()
    const taskCount = taskList.length
    const maxWorkerCount = parseInt(this.dbi.yadamu.PARALLEL)
    const workerCount = taskList.length < maxWorkerCount ? taskList.length : maxWorkerCount
    
	await this.dbi.dbConnected
    const workers = workerCount === 0 ? [this] : new Array(workerCount).fill(0).map((x,idx) => { return this.dbi.workerDBI(idx) })
	const concurrency = workerCount > 0 ? `PARALLEL (${workerCount})` : 'SEQUENTIAL'
	
	const copyOperations = workers.map((worker,idx) => { 
	  return this.copyOperation(taskList,worker)
    })
	  
    this.LOGGER.info(['PIPELINE','COPY',concurrency,sourceVendor,this.dbi.DATABASE_VENDOR],`Processing ${taskCount} Tables`);

    // this.LOGGER.trace([this.constructor.name,'COPY',concurrency,sourceVendor,this.dbi.DATABASE_VENDOR,'COPY_OPERATIONS',copyOperations.length],'WAITING')
	const results = await Promise.allSettled(copyOperations)
	// this.LOGGER.trace([this.constructor.name,,concurrency,sourceVendor,this.dbi.DATABASE_VENDOR,workerCount,'COPY_OPERATIONS',copyOperations.length],'PROCESSING')
	const fatalError = results.find((result) => { return result.value instanceof Error })?.value
	if (fatalError) throw fatalError
	return results
  }  
  
  async copyStagedData(vendor,controlFile,metadata) {

    // this.LOGGER.trace([this.constructor.name,'COPY',this.dbi.DATABASE_VENDOR],'copyStagedData()')

    this.dbi.verifyStagingSource(vendor)
	
	this.dbi.setSystemInformation(controlFile.systemInformation)
	await this.dbi.initializeImport()

    const startTime = performance.now()	
	const statementCache = await this.dbi.generateCopyStatements(metadata,this.credentials);
	const ddlStatements = this.dbi.analyzeStatementCache(statementCache,startTime)
	let results = await this.dbi.executeDDL(ddlStatements)

	await this.dbi.initializeCopy(controlFile)
    
	if (this.dbi.MODE !== 'DDL_ONLY') {
	  const taskList = Object.keys(statementCache).flatMap((table) => {
		if (Array.isArray(statementCache[table].copy)) {
	      return statementCache[table].copy.map((copyOperation) => { 
		     return { 
	           TABLE_NAME    : table
	         , copyOperation : copyOperation
		     }
		  })
		}  
		else {
		  return {
  	        TABLE_NAME    : table
	      , copyOperation : statementCache[table].copy
          }
		}
	  })
      results = await this.copyOperations(taskList,vendor)
	  await this.dbi.finalizeCopy()	  
	}
	await this.dbi.finalizeImport()
    return results
  }
  
}

export { YadamuCopyManager as default}
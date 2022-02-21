import DBIConstants from './dbiConstants.js';
import YadamuLibrary from './yadamuLibrary.js'

class YadamuCopyManager {
 
  constructor(dbi,yadamuLogger) {
    this.dbi = dbi
	this.yadamuLogger = yadamuLogger
  } 
  
  async generateCopyStatements(metadata) {
    const startTime = performance.now()
    await this.dbi.setMetadata(metadata)   
    const statementCache = await this.dbi.generateStatementCache(this.dbi.CURRENT_SCHEMA)
	return statementCache
  }     
  
  async reportCopyErrors(tableName,stack,sqlStatement,failed) {
  }
  
  async reportCopyResults(tableName,metrics) {

    const elapsedTime = metrics.writerEndTime - metrics.writerStartTime
	const throughput = Math.round((metrics.read/elapsedTime) * 1000)
    const writerTimings = `Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s. Throughput: ${throughput} rows/s.`
   
    let rowCountSummary
    switch (metrics.skipped) {
	  case 0:
	    rowCountSummary = `Rows ${metrics.read}.`
        this.yadamuLogger.info([`${tableName}`,`Copy`],`${rowCountSummary} ${writerTimings}`)  
        break
      default:
	    rowCountSummary = `Read ${metrics.read}. Written ${metrics.read - metrics.skipped}.`
        this.yadamuLogger.error([`${tableName}`,`Copy`],`${rowCountSummary} ${writerTimings}`)  
        await this.dbi.reportCopyErrors(tableName,stack,copyStatements.dml,failed)
	}
	
	await this.dbi.reportCopyErrors(tableName,metrics) 

	/*
	if (copyStatements.hasOwnProperty('partitionCount')) {
	  this.dbi.yadamu.recordPartitionMetrics(tableName,metrics);  
	}   
	else {
	*/
	  metrics.pipeStartTime = metrics.writerStartTime
	  this.dbi.yadamu.recordMetrics(tableName,metrics);  
	// }
  }
  
  async copyOperation(taskList,worker,mode,sourceVendor) {

    let result = undefined
	let operationAborted = false;
	await worker.workerReady

    try {
      while (taskList.length > 0) {
	    const task = taskList.shift();
		const metrics = DBIConstants.NEW_COPY_METRICS
	    metrics.insertMode = 'COPY'
	    const results = await worker.copyOperation(task.TABLE_NAME,task.copyOperation,metrics)
		this.reportCopyResults(task.TABLE_NAME,metrics)
      }
	} catch (cause) {
	  result = cause
	  // this.yadamuLogger.trace(['PIPELINE','COPY',sourceVendor,worker.DATABASE_VENDOR,this.dbi.getWorkerNumber()],cause)
	  this.yadamuLogger.handleException(['PIPELINE','COPY',sourceVendor,worker.DATABASE_VENDOR,this.dbi.getWorkerNumber()],cause)
	  if ((this.dbi.ON_ERROR === 'ABORT') && !operationAborted) {
        operationAborted = true
		if (taskList.length > 0) {
	      this.yadamuLogger.error(['PIPELINE','COPY',concurrency,sourceVendor,worker.DATABASE_VENDOR,worker.ON_ERROR],`Operation failed: Skipping ${taskList.length} Tables`);
		}
		else {
	      this.yadamuLogger.warning(['PIPELINE','COPY',concurrency,sourceVendor,worker.DATABASE_VENDOR,worker.ON_ERROR],`Operation failed.`);
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
	  
    this.yadamuLogger.info(['PIPELINE','COPY',concurrency,sourceVendor,this.dbi.DATABASE_VENDOR],`Processing ${taskCount} Tables`);

    // this.yadamuLogger.trace([this.constructor.name,'COPY',concurrency,sourceVendor,this.dbi.DATABASE_VENDOR,'COPY_OPERATIONS',copyOperations.length],'WAITING')
	const results = await Promise.allSettled(copyOperations)
	// this.yadamuLogger.trace([this.constructor.name,,concurrency,sourceVendor,this.dbi.DATABASE_VENDOR,workerCount,'COPY_OPERATIONS',copyOperations.length],'PROCESSING')
	const fatalError = results.find((result) => { return result.value instanceof Error })?.value
	if (fatalError) throw fatalError
	return results
  }  
  
  async copyStagedData(vendor,controlFile,metadata,credentials) {

	this.dbi.setSystemInformation(controlFile.systemInformation)

    const startTime = performance.now()	
	await this.dbi.initializeCopy(controlFile)

	const statementCache = await this.generateCopyStatements(metadata,credentials);
	const ddlStatements = this.dbi.analyzeStatementCache(statementCache,startTime)
	let results = await this.dbi.executeDDL(ddlStatements)
    
	if (this.dbi.MODE != 'DDL_ONLY') {
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
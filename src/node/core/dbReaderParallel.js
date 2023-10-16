
import { 
  performance 
}                                 from 'perf_hooks';

import YadamuLibrary              from '../lib/yadamuLibrary.js'
import YadamuConstants            from '../lib/yadamuConstants.js'
import YadamuWriter               from '../dbi/base/yadamuWriter.js'

import Yadamu                     from './yadamu.js'

import {
  YadamuError, 
  CopyOperationAborted
}                                 from './yadamuException.js'

import DBReader                   from './dbReader.js';									 

class DBReaderParallel extends DBReader {  

  constructor(dbi,yadamuLogger,options) {
    super(dbi,yadamuLogger,options); 
  }
  
  async doCopyOperations(taskList,worker,idx) {

	let operationAborted = false;
	
    const writerDBI = worker.writerDBI
    const readerDBI = worker.readerDBI
	
	// this.yadamuLogger.trace([this.constructor.name,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,idx,readerDBI.getWorkerNumber(),'WORKERS READY'],'WAITING')
    await Promise.all([readerDBI.workerReady,writerDBI.workerReady])
	// this.yadamuLogger.trace([this.constructor.name,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,idx,readerDBI.getWorkerNumber(),'WORKERS READY'],'PROCESSING')
	
	let status = undefined
	let task
    try {
      while (taskList.length > 0) {
	    task = taskList.shift();
		// this.yadamuLogger.trace(['PIPELINE','PARALLEL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,idx,readerDBI.getWorkerNumber(),task.TABLE_NAME],'Allocated')
		await this.pipelineTable(task,readerDBI,writerDBI,true)
		// this.yadamuLogger.trace(['PIPELINE','PARALLEL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,idx,readerDBI.getWorkerNumber(),task.TABLE_NAME],'Completed')
      }
	} catch (cause) {
	  // this.yadamuLogger.trace(['PIPELINE','PARALLEL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,idx,readerDBI.getWorkerNumber(),task.TABLE_NAME],'Failed')
      if (!(cause instanceof CopyOperationAborted)) {
   	    status = cause
	    this.yadamuLogger.handleException(['PIPELINE','PARALLEL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,idx,readerDBI.getWorkerNumber(),task.TABLE_NAME],cause)
	    if ((this.dbi.ON_ERROR === 'ABORT') && !operationAborted) {
          operationAborted = true
	  	  if (taskList.length > 0) {
	        this.yadamuLogger.error(['PIPELINE','PARALLEL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.ON_ERROR],`Operation failed: Skipping ${taskList.length} Tables`);
		  }
		  else {
	        this.yadamuLogger.warning(['PIPELINE','PARALLEL',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.ON_ERROR],`Operation failed.`);
		  }		
          taskList.length = 0;
		  // Destroy any active readers.
		  this.activeReaders.forEach((reader) => {
		    try {
		      reader.destroy(new CopyOperationAborted())
		    } catch(e) { 
			  if (!(cause instanceof CopyOperationAborted)) {
			    this.yadamuLogger.handleWarning(['PIPELINE','PARALLEL','ABORT READER',readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.ON_ERROR,],e) 			
			  }
			}
		  })
		}
      }
      else {
        // this.yadamuLogger.trace([this.constructor.name,'COPY_OPERATIONS',idx],'Sibling Aborted')
	  }
	}	
    readerDBI.destroyWorker()
	writerDBI.destroyWorker()
	return status
  }
  
  async pipelineTables(taskList,readerDBI,writerDBI) {

    this.activeWorkers = new Set()
	
    if (readerDBI.PARTITION_LEVEL_OPERATIONS) {	
	  taskList = readerDBI.expandTaskList(taskList) 
	}
	
    const maxWorkerCount = parseInt(this.dbi.yadamu.PARALLEL)
    const workerCount = taskList.length < maxWorkerCount ? taskList.length : maxWorkerCount

	// this.yadamuLogger.trace([this.constructor.name,workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,'MANAGER READY'],'WAITING')
    await Promise.all([readerDBI.dbConnected,writerDBI.cacheLoaded])
	// this.yadamuLogger.trace([this.constructor.name,workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,'MANAGER READY'],'PROCESSING')
	
	const workers = new Array(workerCount).fill(0).map((x,idx) => { 
	  const worker = {
		readerDBI : readerDBI.workerDBI(idx)
      , writerDBI : writerDBI.workerDBI(idx)			
	  }
      return worker
	})
	   
	const copyOperations = workers.map((worker,idx) => { 
	  return this.doCopyOperations(taskList,worker,idx)
    })
	
    this.yadamuLogger.info(['PIPELINE','PARALLEL',workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`Processing ${taskList.length} Tables`);
    // this.yadamuLogger.trace([this.constructor.name,workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,'COPY_OPERATIONS',copyOperations.length],'WAITING')
	const results = await Promise.allSettled(copyOperations)
	// this.yadamuLogger.trace([this.constructor.name,workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,'COPY_OPERATIONS',copyOperations.length],'PROCESSING')
	const fatalError = results.find((result) => { return result.value instanceof Error })?.value
	if (fatalError) throw fatalError
	// this.yadamuLogger.trace(['PIPELINE','PARALLEL',workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,taskList.length],`Processing Complete`);
  }       
}

export { DBReaderParallel as default}
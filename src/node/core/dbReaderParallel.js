
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

  get PIPELINE_MODE()               { return 'PARALLEL' }

  constructor(dbi,yadamuLogger,options) {
    super(dbi,yadamuLogger,options); 
  }
    
  async pipelineTables(taskList,readerDBI,writerDBI) {

    // Treat each partition as aseperat task in parallel mode.

	if (readerDBI.PARTITION_LEVEL_OPERATIONS) {	
	  taskList = readerDBI.expandTaskList(taskList) 
	}
    const tasks = taskList.entries()
	
    const maxWorkerCount = parseInt(this.dbi.yadamu.PARALLEL) || 1
    const workerCount = taskList.length < maxWorkerCount ? taskList.length : maxWorkerCount

    // this.yadamuLogger.trace(['PIPELINE',this.PIPELINE_MODE,this.PIPELINE_MODE,workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,'MANAGER READY'],'WAITING')
    await Promise.all([readerDBI.dbConnected,writerDBI.cacheLoaded])
	// this.yadamuLogger.trace(['PIPELINE',this.PIPELINE_MODE,this.PIPELINE_MODE,workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,'MANAGER READY'],'PROCESSING')

	this.yadamuLogger.info(['PIPELINE',this.PIPELINE_MODE,workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`Processing ${taskList.length} Tables`);
	
	let operationAborted = false;
	const results = []
	
	const workerPool = Array(workerCount).fill(tasks).map(async (tasks,idx) => {
	  try {
	    const reader = readerDBI.workerDBI(idx)
        const writer = writerDBI.workerDBI(idx)			
	  
	    // this.yadamuLogger.trace(['PIPELINE',this.PIPELINE_MODE,reader.DATABASE_VENDOR,writer.DATABASE_VENDOR,idx,reader.getWorkerNumber(),'WORKERS READY'],'WAITING')
        await Promise.all([reader.workerReady,writer.workerReady])
	    // this.yadamuLogger.trace(['PIPELINE',this.PIPELINE_MODE,reader.DATABASE_VENDOR,writer.DATABASE_VENDOR,idx,reader.getWorkerNumber(),'WORKERS READY'],'PROCESSING')
	
	    for (let [tidx, task] of tasks) {
		  if (operationAborted) break;
		  try {
  	        results.push(await this.pipelineTable(task,reader,writer,true))
	   	  } catch (cause) {
	        results.push(cause)
		    // this.yadamuLogger.trace(['PIPELINE',this.PIPELINE_MODE,workerCount,reader.DATABASE_VENDOR,writer.DATABASE_VENDOR,idx,reader.getWorkerNumber(),task.TABLE_NAME],'Failed')
            if (!(cause instanceof CopyOperationAborted)) {
              this.yadamuLogger.handleException(['PIPELINE',this.PIPELINE_MODE,workerCount,reader.DATABASE_VENDOR,writer.DATABASE_VENDOR,idx,reader.getWorkerNumber(),task.TABLE_NAME],cause)
	          if ((this.dbi.ON_ERROR === 'ABORT') && !operationAborted) {
                operationAborted = true
				const remainingTasks = Array.from(tasks)
	  	        this.yadamuLogger.error(['PIPELINE',this.PIPELINE_MODE,workerCount,reader.DATABASE_VENDOR,writer.DATABASE_VENDOR,reader.ON_ERROR],`Operation failed${remainingTasks.length > 0 ?`: Skipping ${remainingTasks.length} Tables` : `.`}`);
		        // Destroy any active readers.
		        this.activeReaders.forEach((reader) => {
		          try {
		            reader.destroy(new CopyOperationAborted())
		          } catch(e) { 
			        if (!(cause instanceof CopyOperationAborted)) {
			          this.yadamuLogger.handleWarning(['PIPELINE',this.PIPELINE_MODE,workerCount,'ABORT READER',reader.DATABASE_VENDOR,writer.DATABASE_VENDOR,reader.ON_ERROR,],e) 			
			        }
			      }
		        })
		      }
            }
	      }
	    }
	    reader.destroyWorker();
  	    writer.destroyWorker();
	    return operationAborted
	  } catch (e) { 
        this.yadamuLogger.handleException(['PIPELINE',this.PIPELINE_MODE,workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,readerDBI.ON_ERROR],e);
		results.push(e)
	    return e
	  }
	})

    // this.yadamuLogger.trace(['PIPELINE',this.PIPELINE_MODE,workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,'COPY_OPERATIONS',workerPool.length],'WAITING')
	await Promise.allSettled(workerPool)
    // this.yadamuLogger.trace(['PIPELINE',this.PIPELINE_MODE,workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,'COPY_OPERATIONS',workerPool.length],'PROCESSING')
	
	const fatalError = results.find((result) => { return result instanceof Error })
	if (fatalError) throw fatalError
	// this.yadamuLogger.trace(['PIPELINE',this.PIPELINE_MODE,workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR,taskList.length],`Processing Complete`);
  }       
  
}

export { DBReaderParallel as default}
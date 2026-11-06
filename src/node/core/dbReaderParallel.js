
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
import ParallelWorkerHost         from './parallelWorkerHost.js';

class DBReaderParallel extends DBReader {  

  get PIPELINE_MODE()               { return 'PARALLEL' }

  constructor(dbi,yadamuLogger,options) {
    super(dbi,yadamuLogger,options); 
  }
  
  // Run the workers on worker threads when both drivers support it. On by default (cfg/yadamuDefaults.json); THREADED=false keeps them in-process.
  get THREADED() {
	// The environment variable YADAMU_THREADED (true or false) overrides the parameter, so a whole regression suite can be run either way without changing its configuration.
	const threaded = process.env.YADAMU_THREADED?.trim().toLowerCase()
	return ((threaded === 'true') || (threaded === 'false')) ? (threaded === 'true') : (this.dbi.yadamu.parameters.THREADED === true)
  }

  threadsSupported(dbi) {
	// The class itself must name its module (WORKER_MODULE_URL), otherwise a thread would load its parent class, e.g. the product class for a QA subclass.
	return dbi.THREADED_PARALLEL_SUPPORTED && Object.hasOwn(Object.getPrototypeOf(dbi),'WORKER_MODULE_URL')
  }
  
  async pipelineTablesThreaded(taskList,readerDBI,writerDBI,workerCount) {
    const host = new ParallelWorkerHost(this,readerDBI,writerDBI,workerCount)
	const results = await host.pipelineTables(taskList)
	const fatalError = results.find((result) => { return result instanceof Error })
	if (fatalError) throw fatalError
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

	// The pipeline mode is part of the tag (THREADED or PARALLEL), so threaded and in-process runs log the same number of lines.
	const threaded = this.THREADED && this.threadsSupported(readerDBI) && this.threadsSupported(writerDBI)
	const unsupported = this.THREADED && !threaded ? `. Worker threads are not supported by ${[readerDBI,writerDBI].filter((dbi) => { return !this.threadsSupported(dbi) }).map((dbi) => { return dbi.constructor.name }).join(' and ')}.` : ''
	this.yadamuLogger.info(['PIPELINE',threaded ? 'THREADED' : this.PIPELINE_MODE,workerCount,readerDBI.DATABASE_VENDOR,writerDBI.DATABASE_VENDOR],`Processing ${taskList.length} Tables${unsupported}`);
	
	if (threaded) {
	  return await this.pipelineTablesThreaded(taskList,readerDBI,writerDBI,workerCount)
	}
	
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
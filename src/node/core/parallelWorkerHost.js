
import {
  Worker
}                                 from 'worker_threads'

import {
  performance
}                                 from 'perf_hooks';

import {
  setTimeout
}                                 from 'timers/promises'

import NullWriter                 from '../util/nullWriter.js'

import WorkerThreadSupport        from './workerThreadSupport.js'

import {
  CopyOperationAborted
}                                 from './yadamuException.js'

/*
**
** Runs DBReaderParallel's workers on worker threads (THREADED=true). The main thread keeps the managers, hands out tables, applies ON_ERROR=ABORT,
** and performs the requests a thread's shadow managers forward to the real managers. See parallelWorker.js for the thread side.
**
*/

class ParallelWorkerHost {

  get PIPELINE_MODE()             { return 'THREADED' }

  // Requests a worker thread may make of the reject managers. Requests to a DBI manager are limited to that DBI's THREAD_RPC_METHODS.
  static get REJECT_MANAGER_METHODS() { return ['rejectRow','rejectRows'] }

  constructor(dbReader,readerDBI,writerDBI,workerCount) {
	this.dbReader = dbReader
	this.yadamu = dbReader.dbi.yadamu
	this.LOGGER = dbReader.yadamuLogger
	this.readerDBI = readerDBI
	this.writerDBI = writerDBI
	this.workerCount = workerCount
	this.workers = []
	this.results = []
	this.operationAborted = false
  }

  broadcast(message) {
	this.workers.forEach((worker) => {
	  worker.postMessage(message)
	})
  }

  failed(worker,idx,tableName,cause) {
	this.results.push(cause)
	if (!(cause instanceof CopyOperationAborted)) {
	  // The thread logged the error. Only the ABORT decision is made here, because only the main thread knows what remains to be done.
	  if ((this.readerDBI.ON_ERROR === 'ABORT') && !this.operationAborted) {
        this.operationAborted = true
		const remainingTasks = Array.from(this.tasks)
	    this.LOGGER.error(['PIPELINE',this.PIPELINE_MODE,this.workerCount,this.readerDBI.DATABASE_VENDOR,this.writerDBI.DATABASE_VENDOR,this.readerDBI.ON_ERROR],`Operation failed${remainingTasks.length > 0 ?`: Skipping ${remainingTasks.length} Tables` : `.`}`);
		this.broadcast({type: 'abort'})
	  }
	}
  }

  getRPCTarget(target,method) {
	switch (target) {
	  case 'READER_MANAGER':
	    return this.readerDBI.THREAD_RPC_METHODS.includes(method) ? this.readerDBI : undefined
	  case 'WRITER_MANAGER':
	    return this.writerDBI.THREAD_RPC_METHODS.includes(method) ? this.writerDBI : undefined
	  case 'REJECTION_MANAGER':
	  case 'WARNING_MANAGER':
	    return ParallelWorkerHost.REJECT_MANAGER_METHODS.includes(method) ? this.yadamu[target] : undefined
	}
  }

  async processRPC(worker,message) {
	try {
	  const target = this.getRPCTarget(message.target,message.method)
	  if (target === undefined) {
		throw new Error(`Unsupported worker thread request: ${message.target}.${message.method}().`)
	  }
	  const result = await target[message.method](...message.args)
	  try {
	    worker.postMessage({type: 'rpcResult', id: message.id, result: result})
	  } catch (e) {
		// A result that cannot be cloned (e.g. a driver's result set) is not needed by the thread.
	    worker.postMessage({type: 'rpcResult', id: message.id})
	  }
	} catch (e) {
	  worker.postMessage({type: 'rpcResult', id: message.id, error: WorkerThreadSupport.serializeError(e)})
	}
  }

  processMessage(worker,idx,message,state) {
	switch (message.type) {
	  case 'write':
	    this.LOGGER.writeDirect(message.msg)
		break
	  case 'count':
	    this.LOGGER.metrics[message.kind]++
		if (message.kind === 'errors') {
		  this.LOGGER.state.errorRaised = true
		}
		else {
		  this.LOGGER.state.warningRaised = true
		}
		break
	  case 'sql':
	    this.yadamu.STATUS.sqlLogger.write(message.msg)
		break
	  case 'nextTask':
	    const next = this.operationAborted ? { done : true } : this.tasks.next()
		worker.postMessage({type: 'task', task: next.done ? undefined : next.value[1]})
		break
	  case 'result':
	    if (message.error) {
		  const cause = WorkerThreadSupport.rehydrateError(message.error)
		  this.failed(worker,idx,message.tableName,cause)
		}
		else {
		  this.results.push(message.tableName)
		}
		break
	  case 'metrics':
	    this.yadamu.recordMetrics(message.tableName,message.tableState)
		break
	  case 'partitionState':
	    this.writerDBI.trackThreadedPartitionState(message.state)
		break
	  case 'rpc':
	    this.processRPC(worker,message)
		break
	  case 'threadResults':
	    this.readerDBI.mergeThreadResults(message.reader)
	    this.writerDBI.mergeThreadResults(message.writer)
		break
	  case 'fatal':
	    this.failed(worker,idx,undefined,WorkerThreadSupport.rehydrateError(message.error))
	    state.finished = true
		break
	  case 'finished':
	    state.finished = true
	    break
	  case 'ready':
	    break
	  default:
	}
  }

  startWorker(idx) {
	const worker = new Worker(new URL('./parallelWorker.js',import.meta.url),{
	  workerData : {
	    idx                      : idx
	  , workerCount              : this.workerCount
	  , mainTimeOrigin           : performance.timeOrigin
	  , operation                : this.yadamu.STATUS.operation
	  , yadamuModuleURL          : this.yadamu.MODULE_URL
	  , yadamuQA                 : this.yadamu.YADAMU_QA === true
	  , loggerModuleURL          : this.LOGGER.MODULE_URL
	  , yadamuParameters         : this.yadamu.parameters
	  , terminationConfiguration : this.yadamu.TERMINATION_CONFIGURATION
	  , macros                   : this.yadamu.MACROS
	  , homogeneousOperation     : this.yadamu.HOMOGENEOUS_OPERATION
	  , parallelEnabled          : this.yadamu.PARALLEL_ENABLED
	  , partitionLevelReads      : this.yadamu.PARTITION_LEVEL_READS
	  , exceptionFolder          : this.LOGGER.EXCEPTION_FOLDER
	  , exceptionFilePrefix      : this.LOGGER.EXCEPTION_FILE_PREFIX
	  , sqlTrace                 : !(this.yadamu.STATUS.sqlLogger instanceof NullWriter)
	  , reader                   : this.readerState
	  , writer                   : this.writerState
	  }
	})
	this.workers.push(worker)
	const state = { finished : false }
	return new Promise((resolve) => {
	  worker.on('message',(message) => { this.processMessage(worker,idx,message,state) })
	  worker.on('error',(e) => {
		// An uncaught exception in the thread. The thread exits.
		this.LOGGER.handleException(['PIPELINE',this.PIPELINE_MODE,this.workerCount,this.readerDBI.DATABASE_VENDOR,this.writerDBI.DATABASE_VENDOR,'THREAD',idx],e)
		this.failed(worker,idx,undefined,e)
		state.finished = true
	  })
	  worker.on('exit',async (code) => {
	    // Make sure the thread's last messages (results, metrics, counts) have been processed before the operation completes.
	    for (let wait = 0; (!state.finished) && (wait < 50); wait++) {
		  await setTimeout(20)
		}
		if (!state.finished) {
		  this.LOGGER.error(['PIPELINE',this.PIPELINE_MODE,this.workerCount,this.readerDBI.DATABASE_VENDOR,this.writerDBI.DATABASE_VENDOR,'THREAD',idx],`Worker thread exited without finishing.`)
		  this.failed(worker,idx,undefined,new Error(`Worker thread ${idx} exited without finishing (exit code ${code}).`))
		}
	    resolve(code)
	  })
	})
  }

  async pipelineTables(taskList) {

	this.tasks = taskList.entries()
	this.readerState = this.readerDBI.getWorkerState()
	this.writerState = this.writerDBI.getWorkerState()

	const threads = Array(this.workerCount).fill(0).map((v,idx) => { return this.startWorker(idx) })

	// Workers wait for the target's DDL, and for the target to be ready for data (e.g. Oracle's constraints disabled by initializeData()), before they write.
	// A thread's shadow manager has no readyForData of its own. Messages are queued until the thread is listening.
	Promise.all([this.writerDBI.ddlComplete,this.dbReader.dbWriter?.dataInitialized]).then(() => {
	  return this.writerDBI.isReadyForData()
	}).then(() => {
	  this.broadcast({type: 'ddlComplete', state: this.writerDBI.getPostDDLWorkerState()})
	}).catch((e) => {
	  this.broadcast({type: 'ddlComplete', error: WorkerThreadSupport.serializeError(e)})
	})

	const exitCodes = await Promise.all(threads)
	exitCodes.forEach((code,idx) => {
	  if (code !== 0) {
		this.LOGGER.error(['PIPELINE',this.PIPELINE_MODE,this.workerCount,this.readerDBI.DATABASE_VENDOR,this.writerDBI.DATABASE_VENDOR,'THREAD',idx],`Worker thread exited with code ${code}.`)
	  }
	})

	return this.results
  }

}

export { ParallelWorkerHost as default }

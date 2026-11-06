
import {
  parentPort,
  workerData
}                                 from 'worker_threads'

import {
  performance
}                                 from 'perf_hooks';

import YadamuLibrary              from '../lib/yadamuLibrary.js'
import NullWriter                 from '../util/nullWriter.js'

import NodeYadamu                 from './yadamu.js'
import DBReader                   from './dbReader.js'
import WorkerThreadSupport        from './workerThreadSupport.js'

import {
  CopyOperationAborted
}                                 from './yadamuException.js'

/*
**
** Entry point of a worker thread used by DBReaderParallel when THREADED=true.
**
** The thread copies tables using its own reader and writer workers, created from shadow managers rebuilt from the state posted by the main thread.
** Row data stays in the thread. The main thread hands out tables (nextTask/task), and receives log output, error and warning counts, metrics,
** partition states, results and the requests a shadow manager forwards to the real manager (rpc). It broadcasts ddlComplete and abort.
**
*/

// The thread uses the same Yadamu and logger classes as the main thread (the QA harness has its own), loaded by module URL.

const BaseYadamu = (await import(workerData.yadamuModuleURL)).default
const BaseLogger = (await import(workerData.loggerModuleURL)).default

class ForwardingWriter {

  constructor(type) {
	this.type = type
  }

  write(msg) {
	parentPort.postMessage({type: this.type, msg: msg})
	return true
  }

  close() {
  }

}

class ForwardingLogger extends BaseLogger {

  // The main thread's logger counts errors and warnings and sets the operation's status. Forward whatever this logger counted.

  forwardCounts(operation) {
	const before = { ...this.metrics }
	const result = operation()
	for (const kind of ['errors','warnings']) {
	  for (let count = before[kind]; count < this.metrics[kind]; count++) {
	    parentPort.postMessage({type: 'count', kind: kind})
	  }
	}
	return result
  }

  warning(args,msg)   { return this.forwardCounts(() => { return super.warning(args,msg) }) }
  error(args,msg)     { return this.forwardCounts(() => { return super.error(args,msg) }) }
  qaWarning(args,msg) { return this.forwardCounts(() => { return super.qaWarning(args,msg) }) }
  qaError(args,msg)   { return this.forwardCounts(() => { return super.qaError(args,msg) }) }

}

let rpcId = 0
const pendingRPC = new Map()

function rpc(target,method,args) {
  return new Promise((resolve,reject) => {
	const id = ++rpcId
	pendingRPC.set(id,{resolve,reject})
	parentPort.postMessage({type: 'rpc', id: id, target: target, method: method, args: args})
  })
}

class RejectManagerProxy {

  constructor(target) {
	this.target = target
  }

  // Rejected rows are written to the main thread's file.
  async rejectRow(tableName,data) {
	return await rpc(this.target,'rejectRow',[tableName,data])
  }

  async rejectRows(tableName,data) {
	return await rpc(this.target,'rejectRows',[tableName,data])
  }

  setSystemInformation() {}
  setMetadata() {}
  async close() {}

}

class ThreadYadamu extends BaseYadamu {

  constructor() {
	// The QA Yadamu takes only the configuration parameters.
	if (workerData.yadamuQA) {
	  super(workerData.yadamuParameters)
	}
	else {
	  super(workerData.operation,workerData.yadamuParameters,{})
	}
	if (workerData.terminationConfiguration) {
	  this.TERMINATION_CONFIGURATION = workerData.terminationConfiguration
	}
	this.HOMOGENEOUS_OPERATION = workerData.homogeneousOperation
	this._PARALLEL_ENABLED = workerData.parallelEnabled
	this._PARTITION_LEVEL_READS = workerData.partitionLevelReads
  }

  // Parameters come from the main thread. Never read the thread's own process.argv.
  get COMMAND_LINE_PARAMETERS()       { return {} }

  // Macros (e.g. timestamps in bucket names) must resolve as they do on the main thread.
  get MACROS()                        { return workerData.macros }
  set MACROS(v)                       { }

  get LOGGER() {
    this._LOGGER = this._LOGGER || new ForwardingLogger(new ForwardingWriter('write'),this.STATUS,workerData.exceptionFolder,workerData.exceptionFilePrefix)
    return this._LOGGER
  }

  set LOGGER(v) {
	this._LOGGER = v
  }

  get REJECTION_MANAGER() {
    this._REJECTION_MANAGER = this._REJECTION_MANAGER || new RejectManagerProxy('REJECTION_MANAGER')
	return this._REJECTION_MANAGER
  }

  get WARNING_MANAGER() {
    this._WARNING_MANAGER = this._WARNING_MANAGER || new RejectManagerProxy('WARNING_MANAGER')
	return this._WARNING_MANAGER
  }

  initializeSQLTrace() {
	// SQL trace output is forwarded to the main thread, which owns the trace file.
    this.STATUS.sqlLogger = workerData.sqlTrace ? new ForwardingWriter('sql') : NullWriter.NULL_WRITER
  }

  recordMetrics(tableName,tableState) {
	// The main thread records the metrics, and runs any checks its Yadamu adds (e.g. the QA consistency check). Only elapsed times are used, so performance.now() values from this thread need no conversion.
	parentPort.postMessage({type: 'metrics', tableName: tableName, tableState: WorkerThreadSupport.summarizeTableState(tableState)})
	return NodeYadamu.prototype.recordMetrics.call(this,tableName,tableState)
  }

}

class ParallelWorker {

  constructor() {
    this.yadamu = new ThreadYadamu()
	this.LOGGER = this.yadamu.LOGGER
	this.aborted = false
	this.taskResolver = YadamuLibrary.NOOP
	this.ddlComplete = new Promise((resolve,reject) => {
	  this.resolveDDLComplete = resolve
	  this.rejectDDLComplete = reject
	})
	// The rejection is reported by the main thread and by the pipelines that wait on it.
	this.ddlComplete.catch(YadamuLibrary.NOOP)
	parentPort.on('message',(message) => { this.processMessage(message) })
  }

  processMessage(message) {
	switch (message.type) {
	  case 'task':
	    this.taskResolver(message.task)
		break
	  case 'ddlComplete':
	    if (message.error) {
		  const cause = WorkerThreadSupport.rehydrateError(message.error)
		  cause.ignoreUnhandledRejection = true
		  this.rejectDDLComplete(cause)
		}
		else {
		  // State the writer's manager sets once the DDL is complete (e.g. partition metadata, the loader's control file). Workers may have been created before then.
		  this.postDDLState = message.state
		  this.setPostDDLState()
		  this.resolveDDLComplete(true)
		}
		break
	  case 'abort':
	    this.abort()
		break
	  case 'rpcResult':
	    const pending = pendingRPC.get(message.id)
		pendingRPC.delete(message.id)
		if (message.error) {
		  pending.reject(WorkerThreadSupport.rehydrateError(message.error))
		}
		else {
		  pending.resolve(message.result)
		}
		break
	}
  }

  abort() {
	this.aborted = true
	// Destroy any active readers, as DBReaderParallel does for in-process workers.
	this.pipelineContext?.activeReaders.forEach((reader) => {
	  try {
	    reader.destroy(new CopyOperationAborted())
	  } catch(e) {
	    this.LOGGER.handleWarning(['PIPELINE','THREADED','THREAD',workerData.idx,'ABORT READER'],e)
	  }
	})
  }

  setPostDDLState() {
	if (this.postDDLState) {
	  [this.writerManager,this.writer].forEach((dbi) => {
	    dbi?.hydratePostDDLWorkerState(this.postDDLState)
	  })
	}
  }

  async createManager(state,role) {
	const DBIClass = (await import(state.moduleURL)).default
	if (DBIClass.name !== state.className) {
	  throw new Error(`Worker thread loaded class "${DBIClass.name}" from "${state.moduleURL}". Expected "${state.className}".`)
	}
	const manager = new DBIClass(this.yadamu,undefined,state.connectionSettings,{})
	manager.THREAD_SHADOW = true
	manager.hydrateWorkerState(state)
	// The shadow manager has no connection of its own. Requests that must be served by the real manager (e.g. statements on its connection, QA kills) are forwarded to the main thread.
	manager.THREAD_RPC_METHODS.forEach((method) => {
	  manager[method] = async (...args) => { return await rpc(role,method,args) }
	})
	// The main thread's manager connected and loaded the statement cache before the threads were started.
	manager.dbConnected = Promise.resolve(true)
	manager.cacheLoaded = Promise.resolve(true)
	manager.ddlComplete = this.ddlComplete
	await manager.createConnectionPool()
	// Version dependent classes are selected once connected, as YadamuDBI.initialize() does.
	manager.setLibraries()
	return manager
  }

  nextTask() {
	return new Promise((resolve) => {
	  this.taskResolver = resolve
	  parentPort.postMessage({type: 'nextTask'})
	})
  }

  async run() {

	const idx = workerData.idx
	this.readerManager = await this.createManager(workerData.reader,'READER_MANAGER')
	this.writerManager = await this.createManager(workerData.writer,'WRITER_MANAGER')

	const reader = this.readerManager.workerDBI(idx)
	const writer = this.writerManager.workerDBI(idx)
	this.writer = writer
	this.setPostDDLState()

	// Partitions of one table may be copied by different threads, so the main thread tracks the table. Times are converted to the main thread's clock.
	const clockOffset = performance.timeOrigin - workerData.mainTimeOrigin
	writer.trackPartitionedTableState = (partitionState) => {
	  const state = WorkerThreadSupport.summarizeTableState(partitionState)
	  state.startTime += clockOffset
	  state.endTime += clockOffset
	  parentPort.postMessage({type: 'partitionState', state: state})
	}

    await Promise.all([reader.workerReady,writer.workerReady])

	// DBReader.pipelineTable() and its retry logic run unchanged, with the shadow managers in place of the DBReader's DBI and DBWriter.
	this.pipelineContext = Object.create(DBReader.prototype)
	Object.assign(this.pipelineContext,{
	  dbi           : this.readerManager
	, dbWriter      : { dbi : this.writerManager }
	, yadamuLogger  : this.LOGGER
	, status        : this.yadamu.STATUS
	, activeReaders : new Set()
	})

	parentPort.postMessage({type: 'ready'})

	let task
	while ((task = await this.nextTask()) !== undefined) {
	  try {
	    await this.pipelineContext.pipelineTable(task,reader,writer,true)
		parentPort.postMessage({type: 'result', tableName: task.TABLE_NAME})
	  } catch (cause) {
		const aborted = cause instanceof CopyOperationAborted
        if (!aborted) {
          this.LOGGER.handleException(['PIPELINE','THREADED',workerData.workerCount,reader.DATABASE_VENDOR,writer.DATABASE_VENDOR,idx,reader.getWorkerNumber(),task.TABLE_NAME],cause)
		}
		parentPort.postMessage({type: 'result', tableName: task.TABLE_NAME, aborted: aborted, error: WorkerThreadSupport.serializeError(cause)})
	  }
	}

	await Promise.allSettled([reader.destroyWorker(),writer.destroyWorker()])
	parentPort.postMessage({type: 'threadResults', reader: this.readerManager.getThreadResults(), writer: this.writerManager.getThreadResults()})
	await Promise.allSettled([this.readerManager.closePool(),this.writerManager.closePool()])
	parentPort.postMessage({type: 'finished'})
	parentPort.close()
  }

}

const worker = new ParallelWorker()
worker.run().catch((e) => {
  worker.LOGGER.handleException(['PIPELINE','THREADED','THREAD',workerData.idx],e)
  parentPort.postMessage({type: 'fatal', error: WorkerThreadSupport.serializeError(e)})
  parentPort.close()
})

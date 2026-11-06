
import {
  YadamuError,
  CopyOperationAborted
}                                 from './yadamuException.js'

/*
**
** Errors cross between threads as plain objects: structured cloning keeps neither the class nor most of the properties YADAMU relies on.
** The worker thread logs an error before posting it, so the rehydrated error is flagged as already reported.
**
*/

class WorkerThreadError extends YadamuError {

  constructor(summary) {
	super(summary.message)
	this.stack = summary.stack
	this.className = summary.className
	this.code = summary.code
	this.workerLostConnection = summary.lostConnection
	this.yadamuAlreadyReported = summary.yadamuAlreadyReported === true
	if (summary.cause) {
	  this.cause = new WorkerThreadError(summary.cause)
	}
  }

}

class WorkerThreadSupport {

  static serializeError(e,depth = 0) {
	if (!(e instanceof Error)) {
	  return { className : typeof e, message : String(e) }
	}
	return {
	  className             : e.constructor?.name
	, message               : e.message
	, stack                 : e.stack
	, code                  : e.code
	, lostConnection        : YadamuError.lostConnection(e)
	, yadamuAlreadyReported : e.yadamuAlreadyReported === true
	, cause                 : ((e.cause instanceof Error) && (depth < 4)) ? this.serializeError(e.cause,depth+1) : undefined
	}
  }

  static rehydrateError(summary) {
	if (summary.className === 'CopyOperationAborted') {
	  return new CopyOperationAborted()
	}
	return new WorkerThreadError(summary)
  }

  // Row counts and times needed by Yadamu.recordMetrics(), its QA consistency check, and YadamuDBI.trackPartitionedTableState().
  static summarizeTableState(state) {
	return {
	  partitionedTableName : state.partitionedTableName
	, partitionNumber      : state.partitionNumber
	, partitionCount       : state.partitionCount
	, insertMode           : state.insertMode
	, startTime            : state.startTime
	, endTime              : state.endTime
	, read                 : state.read
	, parsed               : state.parsed
	, received             : state.received
	, committed            : state.committed
	, pending              : state.pending
	, written              : state.written
	, cached               : state.cached
	, lost                 : state.lost
	, skipped              : state.skipped
	, sqlTime              : state.sqlTime
	, retryPending         : state.retryPending
	}
  }

  static clonable(value) {
	// Returns value if it can be posted to a worker thread, otherwise its JSON representation (functions and promises are dropped, class instances become plain objects).
	try {
	  structuredClone(value)
	  return value
	} catch (e) {
	  return value === undefined ? value : JSON.parse(JSON.stringify(value))
	}
  }

}

export { WorkerThreadSupport as default, WorkerThreadError }

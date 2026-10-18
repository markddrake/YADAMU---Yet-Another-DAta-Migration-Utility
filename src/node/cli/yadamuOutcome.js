
import {
  performance
}                         from 'perf_hooks';

import {
  AssertionError
}                         from 'assert';

import YadamuLibrary      from '../lib/yadamuLibrary.js';

import {
  CommandLineError,
  ConfigurationFileError
}                         from '../core/yadamuException.js';

/*
**
** How YADAMU reports success or failure to whatever invoked it.
**
** A job FAILED if it threw, or if it logged one or more messages at ERROR level while it ran
** (e.g. a table that failed with ON_ERROR=SKIP, or rejected rows). Warnings do not fail a job.
** A run (CLI invocation or HTTP request) FAILED if any job in it failed, or if it could not start.
**
** CLI exit codes:
**
**   0  SUCCESS          Every job completed without errors (warnings are allowed).
**   1  FAILED           One or more jobs failed, or the operation failed.
**   2  INVALID_REQUEST  The command line or configuration was invalid; no job was run.
**
** Service: the final line of the body of every job, batch, copy, import, export and upload request
** is a single JSON object, the RESULT, with "yadamuResult" : 1 and "outcome" : "SUCCESS" | "FAILED".
** The status code is 200 once log output has started streaming; errors detected before streaming
** starts (unknown job or batch, invalid URL, invalid configuration) return 4xx/5xx with the RESULT as the body.
**
*/

class YadamuOutcome {

  static get SUCCESS()   { return 'SUCCESS' }
  static get FAILED()    { return 'FAILED' }

  static get RESULT_VERSION() { return 1 }

  static #EXIT_CODES = Object.freeze({
    SUCCESS         : 0
  , FAILED          : 1
  , INVALID_REQUEST : 2
  })

  static get EXIT_CODES() { return this.#EXIT_CODES }

  static isInvalidRequest(e) {
	return (e instanceof CommandLineError) || (e instanceof ConfigurationFileError) || (e instanceof AssertionError) || (e instanceof SyntaxError)
  }

  static getExitCode(e) {
	return (e === undefined) ? this.EXIT_CODES.SUCCESS : this.isInvalidRequest(e) ? this.EXIT_CODES.INVALID_REQUEST : this.EXIT_CODES.FAILED
  }

  static getHttpStatus(e) {
	switch (true) {
	  case (e === undefined):
	    return 200
	  case (e.httpStatus !== undefined):
	    return e.httpStatus
	  case (this.isInvalidRequest(e)):
	    return 400
	  default:
	    return 500
	}
  }

  static getErrorMessage(e) {
	return (e instanceof Error) ? e.message : String(e)
  }

  static startJob(name,yadamu) {
	// Snapshot the logger's counters so that each job is judged only on the messages it logged.
	return {
	  name        : name
	, startTime   : performance.now()
	, logger      : yadamu.LOGGER
	, startCounts : yadamu.LOGGER.getMetrics()
	}
  }

  static completeJob(tracker,metrics,e) {

	const counts = tracker.logger.getMetrics()
	const errors = Math.max(0,counts.errors - tracker.startCounts.errors)
	const warnings = Math.max(0,counts.warnings - tracker.startCounts.warnings)

	const tables = Object.values(metrics || {}).filter((tableMetrics) => { return (tableMetrics !== null) && (typeof tableMetrics === 'object') && (tableMetrics.rowCount !== undefined) })

	const summary = {
	  name        : tracker.name
	, outcome     : ((e === undefined) && (errors === 0)) ? this.SUCCESS : this.FAILED
	, errors      : (e !== undefined) && (errors === 0) ? 1 : errors
	, warnings    : warnings
	, tables      : tables.length
	, rows        : tables.reduce((total,tableMetrics) => { return total + Math.max(0,tableMetrics.rowCount) },0)
	, rowsSkipped : tables.reduce((total,tableMetrics) => { return total + (tableMetrics.rowsSkipped || 0) },0)
	, elapsedTime : YadamuLibrary.stringifyDuration(performance.now() - tracker.startTime)
	}
	if (e !== undefined) {
	  summary.error = this.getErrorMessage(e)
	}
	return summary
  }

  static failedJob(name,e) {
	// A job that could not be started, e.g. a batch that references an unknown job.
	return {
	  name        : name
	, outcome     : this.FAILED
	, errors      : 1
	, warnings    : 0
	, tables      : 0
	, rows        : 0
	, rowsSkipped : 0
	, elapsedTime : YadamuLibrary.stringifyDuration(0)
	, error       : this.getErrorMessage(e)
	}
  }

  static createResult(operation,name,startTime,jobs,e) {

	// e is an error that ended the run outside of any job (invalid request, aborted batch, internal error).

	const endTime = performance.now()
	const failedJobs = jobs.filter((job) => { return job.outcome === this.FAILED })
	const result = {
	  yadamuResult : this.RESULT_VERSION
	, outcome      : ((e === undefined) && (failedJobs.length === 0)) ? this.SUCCESS : this.FAILED
	, operation    : operation
	, name         : name
	, jobCount     : jobs.length
	, failedJobs   : failedJobs.length
	, errors       : jobs.reduce((total,job) => { return total + job.errors },(e === undefined) ? 0 : 1)
	, warnings     : jobs.reduce((total,job) => { return total + job.warnings },0)
	, tables       : jobs.reduce((total,job) => { return total + job.tables },0)
	, rows         : jobs.reduce((total,job) => { return total + job.rows },0)
	, elapsedTime  : YadamuLibrary.stringifyDuration(endTime - startTime)
	, jobs         : jobs
	}
	if (e !== undefined) {
	  result.error = this.getErrorMessage(e)
	}
	return result
  }

  static serializeResult(result) {
    return `${JSON.stringify(result)}\n`
  }

}

export { YadamuOutcome as default }

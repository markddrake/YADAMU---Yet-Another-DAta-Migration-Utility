
import fs                from 'fs'
import path              from 'path'
import {
  performance
}                        from 'perf_hooks';

import {
  pipeline
}                        from 'stream/promises'

import assert             from 'assert';

import Yadamu            from '../core/yadamu.js';
import YadamuLogger      from '../core/yadamuLogger.js';

import {
  CommandLineError,
  ConfigurationFileError
}                        from '../core/yadamuException.js';

import YadamuLibrary     from '../lib/yadamuLibrary.js';
import YadamuConstants   from '../lib/yadamuConstants.js';

import YadamuCLI         from '../cli/yadamuCLI.js';
import YadamuOutcome     from '../cli/yadamuOutcome.js';

import FileDBI           from '../dbi/file/fileDBI.js'
import StringWriter      from '../util/stringWriter.js'


import HttpDBI from './httpDBI.js';

/*
**
** Reporting the outcome of a request (see also yadamuOutcome.js)
**
** Requests that run jobs (jobs, batchOperation, copy, import, export, upload) stream their log to the response, so the
** status code and headers are sent with the first log record, before the outcome is known. Every such response therefore
** ends with a RESULT: a single line containing a JSON object with "yadamuResult" : 1 and "outcome" : "SUCCESS" or "FAILED",
** plus per job outcomes, error and warning counts, and table and row counts. The response header "X-Yadamu-Result: final-line"
** announces this. A batch with any failed job is FAILED.
**
** Errors detected before streaming starts (unknown job or batch, invalid URL, unknown connection or schema, invalid configuration)
** return 400 (or 404 for an unknown job or batch, 500 for an internal error) with the RESULT as the (JSON) body.
**
** The download route streams the exported data itself, so it cannot carry a RESULT. If the export fails after data has
** started streaming the response is aborted, so the client sees an incomplete transfer rather than a truncated file
** that appears complete. A download that completes normally succeeded.
**
*/

class Service extends YadamuCLI {

  constructor () {
    super()
    // this.yadamuLogger.switchOutputStream(process.stdout)

	// The configuration is loaded once, at startup, and replaced only by PUT /configuration. Requests do not re-read or re-validate the file named by CONFIG.
    this.CONFIGURATION = this.loadConfigurationFile()
  }

  initialize() {
  }

  getRequestOperation(request) {
	return request.originalUrl.split('/')[2].toUpperCase()
  }

  createRequestYadamu(operation) {
	// Each request gets its own Yadamu, so concurrent requests do not share status, metrics or loggers, and the service's own logger (Routes.LOGGER) is never redirected to a response.
	// It reuses the command line parameters validated when the service started rather than re-parsing process.argv.
	return new Yadamu(operation,undefined,this.yadamu.COMMAND_LINE_PARAMETERS)
  }

  createRequestError(httpStatus,message) {
	const err = new ConfigurationFileError(message)
	err.httpStatus = httpStatus
	return err
  }

  createJobfromRestParameters(request,response) {

     const job = {
       source     : {}
     , target     : {}
     , parameters : {}
     }

     const params = request.params
     const operation = request.originalUrl.split('/')[2]

     switch (operation) {
       case 'download' :
         assert(params.sourceConnection,`Invalid URL - Missing value for parameter "/sourceConnection/" : Valid values are ${this.CONNECTION_NAMES}.`)
         assert(params.sourceSchema,`Invalid URL - Missing value for parameter "/sourceSchema/" : Valid values are ${this.SCHEMA_NAMES}.`)
         job.source.connection = params.sourceConnection
         job.source.schema = params.sourceSchema
         break;
       case 'export' :
         assert(params.sourceConnection,`Invalid URL - Missing value for parameter "/source/" : Valid values are ${this.CONNECTION_NAMES}.`)
         assert(params.sourceSchema,`Invalid URL - Missing value for parameter "/sourceSchema/" : Valid values are ${this.SCHEMA_NAMES}.`)
         assert(params.directory,`Invalid URL - Missing value for parameter "/directory/" : Valid values are ${this.CONNECTION_NAMES}.`)
         assert(params.file,`Invalid URL - Missing value for parameter "/file/"`)
         job.source.connection = params.sourceConnection
         job.source.schema = params.sourceSchema
         job.target.connection = params.directory
         job.parameters.file = params.file
         break
       case 'upload':
         assert(params.targetConnection,`Invalid URL - Missing value for component "/targetConnection/" : Valid values are ${this.CONNECTION_NAMES}.`)
         assert(params.targetSchema,`Invalid URL - Missing value for parameter "/targetSchema/" : Valid values are ${this.SCHEMA_NAMES}.`)
         job.target.connection = params.targetConnection
         job.target.schema = params.targetSchema
         break;
       case 'import':
         assert(params.directory,`Invalid URL - Missing value for parameter "/directory/" : Valid values are ${this.CONNECTION_NAMES}.`)
         assert(params.file,`Invalid URL - Missing value for parameter "/file/"`)
         assert(params.targetConnection,`Invalid URL - Missing value for parameter "/targetConnection/" : Valid values are ${this.CONNECTION_NAMES}.`)
         assert(params.targetSchema,`Invalid URL - Missing value for parameter "/targetSchema/" : Valid values are ${this.SCHEMA_NAMES}.`)
         job.target.connection = params.targetConnection
         job.target.schema = params.targetSchema
         job.source.connection = params.directory
         job.parameters.file = params.file
         break;
       case 'copy':
         assert(params.sourceConnection,`Invalid URL - Missing value for parameter "/sourceConnection/" : Valid values are ${this.CONNECTION_NAMES}.`)
         assert(params.targetConnection,`Invalid URL - Missing value for parameter "/targetConnection/" : Valid values are ${this.CONNECTION_NAMES}.`)
         assert(params.sourceSchema,`Invalid URL - Missing value for parameter "/sourceSchema/" : Valid values are ${this.CONNECTION_NAMES}.`)
         assert(params.targetSchema,`Invalid URL - Missing value for parameter "/targetSchema/" : Valid values are ${this.CONNECTION_NAMES}.`)
         job.source.connection = params.sourceConnection
         job.source.schema = params.sourceSchema
         job.target.connection = params.targetConnection
         job.target.schema = params.targetSchema
         break;
       default:
		 assert(false,`Invalid URL - Invalid value "${operation}" for component "/operation/" : Valid values are ["download","export","upload","import",copy"].`)
	 }
     return job
  }

  validateEndpoint(jobName,role,endpoint) {

	// Check, before any output is streamed, the configuration problems that getSourceConnection() and getTargetConnection() would otherwise report once the job is running.

	assert(endpoint?.connection,new ConfigurationFileError(`Job "${jobName}": ${role} connection not specified.`))
  	assert(this.CONNECTION_NAMES.includes(endpoint.connection),new ConfigurationFileError(`Job "${jobName}": ${role} Connection "${endpoint.connection}" not found. Valid connections: "${this.CONNECTION_NAMES}".`))
	const vendor = YadamuLibrary.getVendorName(this.CONFIGURATION.connections[endpoint.connection])
	assert(this.DATABASE_DRIVERS.hasOwnProperty(vendor),new ConfigurationFileError(`Job "${jobName}": ${role} Connection "${endpoint.connection}": Unsupported database interface "${vendor}".`))
	if (!YadamuConstants.FILE_BASED_DRIVERS.includes(vendor)) {
      assert(this.SCHEMA_NAMES.includes(endpoint.schema),new ConfigurationFileError(`Job "${jobName}": ${role} Schema: "${endpoint.schema}" not found. Valid schemas: "${this.SCHEMA_NAMES}".`))
	}
  }

  validateJob(jobName,job,validateSource=true,validateTarget=true) {
	if ((validateSource) && (job.parser !== 'SQL')) {
	  this.validateEndpoint(jobName,'Source',job.source)
	}
	if (validateTarget) {
	  this.validateEndpoint(jobName,'Target',job.target)
	}
  }

  startStreaming(response,yadamu) {
	response.type('text')
	response.setHeader('X-Yadamu-Result','final-line')
	yadamu.LOGGER = YadamuLogger.streamLogger(response,yadamu.STATUS,yadamu.EXCEPTION_FOLDER,yadamu.EXCEPTION_FILE_PREFIX)
  }

  sendResult(response,result,e) {

	// Once log output has been streamed the status code has already been sent, so the outcome is reported by the final line of the body.
	// Otherwise the status code reports the outcome and the RESULT is the body.

	if (response.writableEnded || response.destroyed) {
	  return
	}
	const body = YadamuOutcome.serializeResult(result)
	if (response.headersSent) {
	  response.end(body)
	}
	else {
	  // Set by streamLogger(). It would conflict with the Content-Length set by send()
	  response.removeHeader('Transfer-Encoding')
	  response.status(result.outcome === YadamuOutcome.SUCCESS ? 200 : YadamuOutcome.getHttpStatus(e))
	  response.type('json')
	  response.send(body)
	}
  }

  sendFailure(request,response,e) {
	// An error that was not handled by the request: either it occurred before streaming started (invalid request), or it is an internal error.
	const operation = this.getRequestOperation(request)
	const result = YadamuOutcome.createResult(operation,request.originalUrl,request.yadamuStartTime || performance.now(),[],e)
	this.sendResult(response,result,e)
  }

  async streamJobs(response,operation,name,jobs) {

	// Run a list of jobs [{jobName, job, execute?}], streaming the log to the response and finishing with the RESULT line.
	// Failed jobs are logged and recorded, and the remaining jobs run, unless the failed job specifies abortOnError.

	const startTime = performance.now()
	const jobOutcomes = []
	let firstError = undefined

    const yadamu = this.createRequestYadamu(operation)
	this.startStreaming(response,yadamu)

	try {
	  for (const entry of jobs) {
		try {
		  await yadamu.reset(entry.job.parameters)
		  if (entry.execute) {
			await entry.execute(yadamu,jobOutcomes)
		  }
		  else {
			// Pass this.CONFIGURATION for each job: executeJob() assigns it to this.CONFIGURATION, which must not undo a concurrent PUT /configuration.
		    await this.executeTrackedJob(yadamu,this.CONFIGURATION,entry.job,entry.jobName,jobOutcomes)
		  }
		} catch (e) {
		  firstError = firstError || e
		  yadamu.LOGGER.handleException(['YADAMU-SVR',operation,name,entry.jobName],e)
		  if (entry.job.abortOnError) {
		    yadamu.LOGGER.error(['YADAMU-SVR',operation,name,entry.jobName],`Job failed and specifies "abortOnError". Remaining jobs skipped.`)
			break
		  }
		}
	  }
	} finally {
	  try {
	    await yadamu.close()
	  } catch (e) {
		this.yadamuLogger.handleWarning(['YADAMU-SVR',operation,name,'CLOSE'],e)
	  }
	}

	this.sendResult(response,YadamuOutcome.createResult(operation,name,startTime,jobOutcomes),firstError)
  }

  async exportStream(request,response) {

	// The response body is the exported data. Log output goes to the service log.

    const operation = this.getRequestOperation(request)
    const job = this.createJobfromRestParameters(request)
	this.validateJob(operation,job,true,false)

    const yadamu = this.createRequestYadamu(operation)
    response.type('json')
	try {
      const sourceDBI = await this.getSourceConnection(yadamu,job)
      const targetDBI = new HttpDBI(yadamu,response)
      await yadamu.pumpData(sourceDBI,targetDBI)
      response.end();
    } catch (e) {
	  if (!response.headersSent) {
		throw e
	  }
	  // Data has already been sent with status 200. Abort the response so the client sees an incomplete transfer rather than a truncated export that appears complete.
	  this.yadamuLogger.handleException(['YADAMU-SVR',operation,'ABORTED'],e)
	  if (!response.destroyed) {
	    response.destroy()
	  }
    } finally {
	  await yadamu.close()
	}
  }

  async importStream(request,response) {

    const operation = this.getRequestOperation(request)
    const job = this.createJobfromRestParameters(request)
	this.validateJob(operation,job,false,true)

	const execute = async (yadamu,jobOutcomes) => {
	  const tracker = YadamuOutcome.startJob(operation,yadamu)
	  try {
        const sourceDBI = new HttpDBI(yadamu,request)
        const targetDBI = await this.getTargetConnection(yadamu,job)
        const metrics = await yadamu.pumpData(sourceDBI,targetDBI)
	    jobOutcomes.push(YadamuOutcome.completeJob(tracker,metrics))
	  } catch (e) {
	    jobOutcomes.push(YadamuOutcome.completeJob(tracker,yadamu.metrics,e))
		throw e
	  }
	}

	await this.streamJobs(response,operation,request.originalUrl,[{jobName: operation, job: job, execute: execute}])
  }

  async executeRestRequest(request,response) {

    const operation = this.getRequestOperation(request)
    const job = this.createJobfromRestParameters(request)
	this.validateJob(operation,job)
	await this.streamJobs(response,operation,request.originalUrl,[{jobName: operation, job: job}])
  }

  async exportFile(request,response) {

	await this.executeRestRequest(request,response)

  }

  async importFile(request,response) {

	await this.executeRestRequest(request,response)

  }

  async copy(request,response) {

	await this.executeRestRequest(request,response)

  }

  async updateConfiguration(request,response) {
     const startTime = performance.now()
     const stringWriter = new StringWriter()
     await pipeline(request,stringWriter);
	 // The CONFIGURATION setter validates the new configuration before replacing the current one.
     this.CONFIGURATION = JSON.parse(stringWriter.toString())
	 this.sendResult(response,YadamuOutcome.createResult('CONFIGURATION','configuration',startTime,[]))
  }

  getJobByNumber(jobNumber) {
	const jobs = Array.isArray(this.CONFIGURATION.jobs) ? this.CONFIGURATION.jobs : Object.values(this.CONFIGURATION.jobs || {})
	const jobNames = Array.isArray(this.CONFIGURATION.jobs) ? jobs.map((job,idx) => { return `JOB#${idx+1}` }) : Object.keys(this.CONFIGURATION.jobs || {})
	const idx = Number(jobNumber) - 1
	if (!Number.isInteger(idx) || (idx < 0) || (idx >= jobs.length)) {
	  throw this.createRequestError(404,`Job number "${jobNumber}" not found. The configuration contains ${jobs.length} jobs.`)
	}
	return { jobName: jobNames[idx], job: jobs[idx] }
  }

  async executeJobs(request,response) {

	const operation = this.getRequestOperation(request)
	let entry
	switch (true) {
	  case Object.hasOwn(request.params,'jobName'):
        if (!this.JOB_NAMES.includes(request.params.jobName)) {
		  throw this.createRequestError(404,`Job "${request.params.jobName}" not found. Valid jobs: "${this.JOB_NAMES}".`)
        }
		entry = { jobName: request.params.jobName, job: this.CONFIGURATION.jobs[request.params.jobName] }
		break
	  case Object.hasOwn(request.params,'jobNumber'):
		entry = this.getJobByNumber(request.params.jobNumber)
		break
	  default:
	    throw this.createRequestError(400,`Specify the job to run: /yadamu/jobs/jobName/:jobName or /yadamu/jobs/jobNumber/:jobNumber. Valid jobs: "${this.JOB_NAMES}".`)
    }

	this.validateJob(entry.jobName,entry.job)
	await this.streamJobs(response,operation,entry.jobName,[entry])
  }

  async executeBatch(request,response) {

	const operation = this.getRequestOperation(request)
    const batchName = request.params.batchName
	if (!this.BATCH_NAMES.includes(batchName)) {
	  throw this.createRequestError(404,`Batch "${batchName}" not found. Valid batches: "${this.BATCH_NAMES}".`)
	}

	// Validate every job in the batch before running any of them.

	const jobs = this.CONFIGURATION.batchOperations[batchName].map((jobName) => {
	  if (!this.JOB_NAMES.includes(jobName)) {
	    throw this.createRequestError(400,`Batch "${batchName}": Job "${jobName}" not found. Valid job names: "${this.JOB_NAMES}".`)
	  }
	  const job = this.CONFIGURATION.jobs[jobName]
	  this.validateJob(jobName,job)
	  return { jobName: jobName, job: job }
	})

	await this.streamJobs(response,operation,batchName,jobs)
  }

  async about(request,response) {
    response.type('text')
    response.write('Yadamu Service v1.0. Copyright Yet Another Bay Area Software Company 2022.');
    response.end();
  }

}

export { Service as default }

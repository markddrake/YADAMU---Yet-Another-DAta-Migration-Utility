
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

import FileDBI           from '../dbi/file/fileDBI.js'
import StringWriter      from '../util/stringWriter.js'


import HttpDBI from './httpDBI.js';

class Service extends YadamuCLI {
  
  constructor () {
    super()
    // this.yadamuLogger.switchOutputStream(process.stdout)
    this.CONFIGURATION = this.loadConfigurationFile()
  }
        
  initialize() {
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
  
  async processRequest(yadamu,sourceDBI,targetDBI,response,resetLogger) {

    await yadamu.pumpData(sourceDBI,targetDBI)
    await yadamu.close()
    response.end();     
 
  }  
          
  async exportStream(request,response) {

	try {
      response.type('json')     
      this.command = request.originalUrl.split('/')[2].toUpperCase()
      this.yadamu = this.createYadamu()
      const job = this.createJobfromRestParameters(request)
      const sourceDBI = await this.getSourceConnection(this.yadamu,job)
      const targetDBI = new HttpDBI(this.yadamu,response)
      await this.processRequest(this.yadamu,sourceDBI,targetDBI,response,false)
    } catch (e) {
      response.status(400).send(e.message)
      throw e 
    }      
  }

  async importStream(request,response) {

	try {
      response.type('text')     
      this.command = request.originalUrl.split('/')[2].toUpperCase()
      this.yadamu = this.createYadamu()
      const responseLogger = YadamuLogger.streamLogger(response,this.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX)
      this.yadamu.LOGGER = responseLogger
      const job = this.createJobfromRestParameters(request)
      const sourceDBI = new HttpDBI(this.yadamu,request)
      const targetDBI = await this.getTargetConnection(this.yadamu,job)
      await this.processRequest(this.yadamu,sourceDBI,targetDBI,response,true)
    } catch (e) {
      response.status(400).send(e.message)
      throw e 
    }      
  }
      
  
  async executeRestRequest(request,response,resetLogger) {

    this.command = request.originalUrl.split('/')[2].toUpperCase()
    this.yadamu = this.createYadamu()
    const responseLogger = YadamuLogger.streamLogger(response,this.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX)
    this.yadamu.LOGGER = responseLogger

	try {
      response.type('text')     
      const job = this.createJobfromRestParameters(request)
      await this.executeJob(this.yadamu,this.CONFIGURATION,job)
      response.end();     
    } catch (e) {
      response.status(400).send(e.message)
      throw e 
	}
  }
  
  async exportFile(request,response) {
	  
	await this.executeRestRequest(request,response,true)

  }

  async importFile(request,response) {  

	await this.executeRestRequest(request,response,true)

  }
      
  async copy(request,response) {

	await this.executeRestRequest(request,response,true)

  }
    
  async updateConfiguration(request,response) {
     const stringWriter = new StringWriter()
     await pipeline(request,stringWriter);
     this.CONFIGURATION = JSON.parse(stringWriter.toString())
     response.end();      
  }
      
  async executeJobs(request,response) {

      if (request.params.hasOwnProperty('jobName')) {
        const jobName = request.params.jobName
        if (!this.JOB_NAMES.includes(jobName)) {
          response.status(400).send(`Job "${jobName}" not found. Valid jobs: "${this.JOB_NAMES}".`)
          return
        }
      }
      /*
      if (request.params.hasOwnProperty('jobNumber') && ((request.params.jobNumber-1) > this.configuration.jobs.length)) {
        response.status(400).send(`Invalid URL: Job Number "${request.params.jobNumber}" not found in configuration file.`)
        return
      }
      */
	  
	  const operation = request.originalUrl.split('/')[2].toUpperCase()
      const yadamu = new Yadamu(operation)
      const responseLogger = YadamuLogger.streamLogger(response,this.yadamu.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX)
      yadamu.LOGGER = responseLogger

      response.type('text')
            
	  try {		
        const job = this.CONFIGURATION.jobs[request.params.jobName]
        await this.executeJob(yadamu,this.CONFIGURATION,job)
      } catch (e) {
        response.status(400).send(e.message)
      }
	  
      response.end();     
  }
    
  async executeBatch(request,response) {

    const batchName = request.params.batchName
	try {
  	  assert(this.BATCH_NAMES.includes(batchName),new ConfigurationFileError(`Batch "${batchName}" not found. Valid connections: "${this.BATCH_NAMES}".`))	
	} catch(e) {
  	  response.status(400).send(e.message)
      return
    }
    
	const batch = this.CONFIGURATION.batchOperations[batchName]

	const operation = request.originalUrl.split('/')[2].toUpperCase()
    const yadamu = new Yadamu(operation)
    const responseLogger = YadamuLogger.streamLogger(response,this.yadamu.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX)
    yadamu.LOGGER = responseLogger

    response.type('text')

    for (let jobName of batch) {          
	  try {
  	    assert(this.JOB_NAMES.includes(jobName),new ConfigurationFileError(`Job "${jobName}" not found. Valid job names: "${this.JOB_NAMES}".`))	
        const job = this.CONFIGURATION.jobs[jobName]
	    await this.executeJob(yadamu,this.CONFIGURATION,job)
      }
	  catch (e) {
	    yadamu.LOGGER.logException(['YADAMU','SERVICE','EXECUTE_BATCH',batchName,jobName],e)
	  }
	}
    response.end();     
  }
    
  async about(request,response) {
    response.type('text')
    response.write('Yadamu Service v1.0. Copyright Yet Another Bay Area Software Company 2022.');
    response.end();
  }
      
}

export { Service as default }
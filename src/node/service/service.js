
import fs                from 'fs'
import path              from 'path'
import { 
  performance 
}                        from 'perf_hooks';

import {
  pipeline
}                        from 'stream/promises'

import Yadamu            from '../core/yadamu.js';
import YadamuLogger      from '../core/yadamuLogger.js';
import YadamuLibrary     from '../lib/yadamuLibrary.js';
import YadamuConstants   from '../lib/yadamuConstants.js';

import YadamuCLI         from '../cli/yadamuCLI.js';

import FileDBI           from '../dbi/file/fileDBI.js'
import StringWriter      from '../util/stringWriter.js'

import HttpDBI from './httpDBI.js';

class Service extends YadamuCLI {

  
  getUser(vendor,schema) {
    
     return vendor === 'mssql' ? schema.owner : (vendor === 'snowflake' ? schema.snowflake.schema : schema.schema)
     
  }
  
  constructor () {
	super()
    this.yadamuLogger.switchOutputStream(process.stdout)
	this.configuration = this.loadConfigurationFile()
  }
		
  initialize() {
  }

  getConnectionList(file) {
	return ` Valid values are "${Object.keys(this.configuration.connections).join('","')}".`
  }	  
  
  getSchemaList() {
	return ` Valid values are "${Object.keys(this.configuration.schemas).join('","')}".`
  }	  
  
  validateRestParameters(params) {
	 
    if (params.hasOwnProperty('sourceConnection') && !this.configuration.connections.hasOwnProperty(params.sourceConnection)) {
	  return `Invalid URL: Source Connection named "${params.sourceConnection}" not found in configuration file.${this.getConnectionList(false)}`
	}
	
    if (params.hasOwnProperty('sourceSchema') && !this.configuration.schemas.hasOwnProperty(params.sourceSchema)) {
      return `Invalid URL: Source Schema named "${params.sourceSchema}" not found in configuration file.${this.getSchemaList()}`
    }
	
    if (params.hasOwnProperty('targetConnection') && !this.configuration.connections.hasOwnProperty(params.targetConnection)) {
	  return `Invalid URL: Target Connection named "${params.targetConnection}" not found in configuration file.${this.getConnectionList(false)}`
	}
	
    if (params.hasOwnProperty('targetSchema') && !this.configuration.schemas.hasOwnProperty(params.targetSchema)) {
      return `Invalid URL: Source Schema named "${params.targetSchema}" not found in configuration file.${this.getSchemaList()}`
    }
	
    if (params.hasOwnProperty('directory') && !this.configuration.connections.hasOwnProperty(params.directory)) {
	  return `Invalid URL: Directory named "${params.directory}" not found in configuration file.${this.getConnectionList(true)}`
    }
	
	return undefined
	
  }
  
  async getSourceConnection(yadamu,params) {
  
    const sourceConnection = this.configuration.connections[params.sourceConnection]
    const sourceSchema = this.configuration.schemas[params.sourceSchema]
    const sourceDatabase =   YadamuLibrary.getVendorName(sourceConnection)
    const sourceParameters = {
	  FROM_USER: this.getUser(sourceDatabase,sourceSchema)
	}
    return await this.getDatabaseInterface(yadamu,sourceDatabase,sourceConnection,sourceParameters);
	
  }

  async getTargetConnection(yadamu,params) {

    const targetConnection = this.configuration.connections[params.targetConnection]
    const targetSchema = this.configuration.schemas[params.targetSchema]
    const targetDatabase = YadamuLibrary.getVendorName(targetConnection);
    const targetParameters = {
	 TO_USER: this.getUser(targetDatabase,targetSchema)
	}
    return await this.getDatabaseInterface(yadamu,targetDatabase,targetConnection,targetParameters);
	
  }
	  
  async getDirectory(yadamu,params) {

    const directory = this.configuration.connections[params.directory]
    const parameters = {
	  FILE: path.join(directory.file.directory,params.file)
	}
    return await this.getDatabaseInterface(yadamu,'file',{},parameters);

  }	  
	  
  async processRequest(yadamu,sourceDBI,targetDBI,response,resetLogger) {

	try {
      await yadamu.doCopy(sourceDBI,targetDBI)
	} finally {
      if (resetLogger) {
		yadamu.LOGGER.switchOutputStream(process.stdout);
	  }
      response.end();	  
	}
  }	 
	 
	  
  async exportStream(request,response) {

    const parameterError = this.validateRestParameters(request.params)
    if (parameterError) {		
	  response.status(400).send(parameterError)
	  return
	} 

    response.type('json')	  
	const yadamu = this.yadamu.clone()
    const sourceDBI = await this.getSourceConnection(yadamu,request.params)
    const targetDBI = new HttpDBI(yadamu,response)
    await this.processRequest(yadamu,sourceDBI,targetDBI,response,false)
	  	  
  }

  async exportFile(request,response) {

    const parameterError = this.validateRestParameters(request.params)
    if (parameterError) {		
	  response.status(400).send(parameterError)
	  return
	} 

    response.type('text')	  
	const yadamu = this.yadamu.clone()
    yadamu.LOGGER.switchOutputStream(response);
    const sourceDBI = await this.getSourceConnection(yadamu,request.params)
    const targetDBI = await this.getDirectory(yadamu,request.params)
	await this.processRequest(yadamu,sourceDBI,targetDBI,response,true)

  }

  async importStream(request,response) {

    const parameterError = this.validateRestParameters(request.params)
    if (parameterError) {		
	  response.status(400).send(parameterError)
	  return
	} 

    response.type('text')	  
	const yadamu = this.yadamu.clone()
    yadamu.LOGGER.switchOutputStream(response);
	const sourceDBI = new HttpDBI(yadamu,request)
    const targetDBI = await this.getTargetConnection(yadamu,request.params)
	await this.processRequest(yadamu,sourceDBI,targetDBI,response,true)
  }
	  
  async importFile(request,response) {  

    const parameterError = this.validateRestParameters(request.params)
    if (parameterError) {		
	  response.status(400).send(parameterError)
	  return
	} 

    response.type('text')	  
	const yadamu = this.yadamu.clone()
    yadamu.LOGGER.switchOutputStream(response);
    const sourceDBI = await this.getDirectory(yadamu,request.params)
    const targetDBI = await this.getTargetConnection(yadamu,request.params)
	await this.processRequest(yadamu,sourceDBI,targetDBI,response,true)

  }
	  
  async copy(request,response) {

    const parameterError = this.validateRestParameters(request.params)
    if (parameterError) {		
	  response.status(400).send(parameterError)
	  return
	} 

    response.type('text')	  
	const yadamu = this.yadamu.clone()
    yadamu.LOGGER.switchOutputStream(response);
    const sourceDBI = await this.getSourceConnection(yadamu,request.params)
    const targetDBI = await this.getTargetConnection(yadamu,request.params)
	await this.processRequest(yadamu,sourceDBI,targetDBI,response,true)

  }
	
  async updateConfiguration(request,response) {
	 const stringWriter = new StringWriter()
	 await pipeline(request,stringWriter);
	 this.configuration = JSON.parse(stringWriter.toString())
     response.end();	  
  }
	  
  async executeJobs(request,response) {
	  
	  if (request.params.hasOwnProperty('jobNumber') && ((request.params.jobNumber-1) > this.configuration.jobs.length)) {
		response.status(400).send(`Invalid URL: Job Number "${request.params.jobNumber}" not found in configuration file.`)
		return
      }
	  
      if (request.params.hasOwnProperty('jobNName') && !this.configuration.jobs.hasOwnProperty(request.params.jobName)) {
		response.status(400).send(`Invalid URL: Job named "${request.params.targetConnection}" not found in configuration file.}`)
		return
      }

	  const yadamu = this.yadamu.clone()
	  
	  response.type('text')
	  yadamu.LOGGER.switchOutputStream(response);
	        
	  let job
      switch (true) {
		 case request.params.hasOwnProperty('jobNumber'):
		   job = this.configuration.jobs[request.params.jobNumber-1]
           await this.executeJob(this.configuration,job)
		   break;
		 case request.params.hasOwnProperty('jobName'):
		   job = this.configuration.jobs[request.params.jobName]
           await this.executeJob(this.configuration,job)
		   break;
		 default:
           for (const job of this.configuration.jobs) {
	        await executeJob(this.configuration,job)
          }
	  }
	  
	  yadamu.LOGGER.switchOutputStream(process.stdout);
	  response.end();	  
  }
	
  async about(request,response) {
	response.type('text')
	response.write('Yadamu Service v1.0. Copyright Yet Another Bay Area Software Company 2022.');
	response.end();
  }
	  
}

export { Service as default }
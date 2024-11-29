
import fs                 from 'fs';
import path               from 'path';

import { 
  performance
}                         from 'perf_hooks';

import assert             from 'assert';

import StringWriter       from '../util/stringWriter.js'

import Yadamu             from '../core/yadamu.js';
import YadamuLogger       from '../core/yadamuLogger.js';
import YadamuConstants    from '../lib/yadamuConstants.js';
import YadamuLibrary      from '../lib/yadamuLibrary.js';
import YadamuCompare      from '../dbi/base/yadamuCompare.js'


import {
  CommandLineError, 
  ConfigurationFileError
}                         from '../core/yadamuException.js';

import {
  FileNotFound
}                         from '../dbi/file/fileException.js';


/*
**
** Instantiated directly Electron app.
** The YadamuCmd, Import, Upload, Export, Copy and Test classes inherit from this class.
** 
** The following command line switches are supported
**
** INIT: Only valid with the Electron app. Loads the configuration file used to pre-populate the fields in the YADAMU UI. 
**
** COPY: Valid with the Electron app in cmdline mode, the YadamuCMD class and the Copy class. Copies data between databases
**
** TEST: Valid with the Electron app in cmdline mode, the YadamuCMD class and the Test class. Used internally for Yadamu regression tests.
**
** IMPORT: Valid with the Electron app in cmdline mode, the YadamuCMD class and the Import class. Imports the specified file into a database
** 
** UPLOAD: Valid with the Electron app in cmdline mode, the YadamuCMD class and the Upload class. Imports the specified file into a database by uploading the file to the database and using the database's native JSON capabiliites to process the file.
**
** EXPORT: Valid with the Electron app in cmdline mode, the YadamuCMD class and the Export class. Generates the specified file from a database
**
** Specifying a switch other than INIT with the Electron app causes it to execute as a command line interface. 
**
** Only one of the supported command line switches can be specified on the command line. 
** The INIT command line switch is only valid with the Elecron app.
** The values supplied using the command line switch is the path to the file to be processed
** Command Line Switches are not valid when using the Import, Export, Upload, Copy and Test clases.
** The Command line argument file must be specified when using the Import, Export, Upload Copy and Test classes.
** 
** IMPORT, UPLOAD, COPY, TEST and INIT operaitons require that the file exists
** EXPORT operations require that the file does not exists unless the command line argument OVERWRITE=YES is specified
**
*/

class YadamuCLI {

  static get REQUIRES_SWITCH() {
    this._REQUIRES_SWITCH = this._REQUIRES_SWITCH || Object.freeze(['YADAMUCLI' , 'YADAMUCMD'])
    return this._REQUIRES_SWITCH
  }

  static get VALID_SWITCHES() {
    this._VALID_SWITCHES = this._VALID_SWITCHES || Object.freeze(['EXPORT','IMPORT','LOAD','UNLOAD','UPLOAD','COPY'])
    return this._VALID_SWITCHES
  }

  static get FILE_MUST_EXIST() {
    this._FILE_MUST_EXIST = this._FILE_MUST_EXIST || Object.freeze(['IMPORT'])
    return this._FILE_MUST_EXIST
  }
  
  static get FILE_MUST_NOT_EXIST() {
    this._FILE_MUST_NOT_EXIST = this._FILE_MUST_NOT_EXIST || Object.freeze(['EXPORT'])
    return this._FILE_MUST_NOT_EXIST
  }
  
  static get CONFIGURATION_REQUIRED() {
    this._CONFIGURATION_REQUIRED = this._CONFIGURATION_REQUIRED || Object.freeze(['COPY','SERVICE','YADAMUAPI','TEST'])
    return this._CONFIGURATION_REQUIRED
  }

  static get ENUMERATED_PARAMETERS() {
    this._ENUMERATED_PARAMETERS = this._ENUMERATED_PARAMETERS || Object.freeze(['DDL_ONLY','DATA_ONLY','DDL_AND_DATA'])
    return this._ENUMERATED_PARAMETERS
  }

  static #SUPPORTED_ARGUMENTS = Object.freeze({
      "YADAMUCMD"    : Object.freeze([])
    , "YADAMUGUI"    : Object.freeze(['EXPORT','IMPORT','LOAD','UNLOAD','COPY','UPLOAD'])
    , "YADAMUCLI"    : Object.freeze(['EXPORT','IMPORT','LOAD','UNLOAD','COPY','UPLOAD'])
    , "EXPORT"       : Object.freeze([])
    , "IMPORT"       : Object.freeze([])
    , "LOAD"         : Object.freeze([])
    , "UNLOAD"       : Object.freeze([])
    , "COPY"         : Object.freeze([])
    , "UPLOAD"       : Object.freeze([])
    , "SERVICE"      : Object.freeze([])
    , "COMPRARE"     : Object.freeze([])
    , "INIT"         : Object.freeze([])
    , "DIRECTLOAD"   : Object.freeze([])
    })
	
  static get SUPPORTED_ARGUMENTS() { return this.#SUPPORTED_ARGUMENTS }

  static #REQUIRED_ARGUMENTS = Object.freeze({
      "YADAMUGUI"      : Object.freeze([])
    , "YADAMUAPI"      : Object.freeze([])
    , "EXPORT"         : Object.freeze(['FROM_USER','FILE'])
    , "IMPORT"         : Object.freeze(['TO_USER','FILE'])
    , "UNLOAD"         : Object.freeze(['FROM_USER','DIRECTORY'])
    , "LOAD"           : Object.freeze(['TO_USER','DIRECTORY'])
    , "UPLOAD"         : Object.freeze(['TO_USER'])
    , "COPY"           : Object.freeze(['CONFIG'])
    , "SERVICE"        : Object.freeze([])
    , "ENCRYPT"        : Object.freeze(['FILE'])
    , "DECRYPT"        : Object.freeze(['FILE'])
    , "COMPARE"        : Object.freeze([])
    , "TEST"           : Object.freeze(['CONFIG'])
    , "YADAMUCMD"      : Object.freeze([])
    , "YADAMUCLI"      : Object.freeze([])
    , "DIRECTLOAD"     : Object.freeze(['FROM_USER','TO_USER','REMOTE_STAGING_AREA'])
    , "CONNECTIONTEST" : Object.freeze(['CONFIG','RDBMS'])
  })

  static get REQUIRED_ARGUMENTS() { return this.#REQUIRED_ARGUMENTS }


  static #OPTIONAL_ARGUMENTS = Object.freeze({
      "YADAMUGUI"      : Object.freeze([])
    , "YADAMUAPI"      : Object.freeze([])
    , "EXPORT"         : Object.freeze([])
    , "IMPORT"         : Object.freeze([])
    , "UNLOAD"         : Object.freeze([])
    , "LOAD"           : Object.freeze([])
    , "UPLOAD"         : Object.freeze([])
    , "COPY"           : Object.freeze([])
    , "SERVICE"        : Object.freeze([])
    , "ENCRYPT"        : Object.freeze([])
    , "DECRYPT"        : Object.freeze([])
    , "COMPARE"        : Object.freeze([])
    , "TEST"           : Object.freeze([])
    , "YADAMUCMD"      : Object.freeze([])
    , "YADAMUCLI"      : Object.freeze([])
    , "DIRECTLOAD"     : Object.freeze([])
    , "CONNECTIONTEST" : Object.freeze([])
  })

  static get OPTIONAL_ARGUMENTS() { return this.#OPTIONAL_ARGUMENTS }

  static #ILLEGAL_ARGUMENTS = Object.freeze({
      "YADAMUCLI" : Object.freeze(['FILE','CONFIG','CONFIGURATION'])
    , "YADAMUCMD" : Object.freeze(['FILE','CONFIG','CONFIGURATION'])
    , "IMPORT"    : Object.freeze(['CONFIG','CONFIGURATION',"FROM_USER","OVERWRITE"])
    , "LOAD"      : Object.freeze(['CONFIG','CONFIGURATION',"FROM_USER","OVERWRITE"])
    , "UPLOAD"    : Object.freeze(['CONFIG','CONFIGURATION',"FROM_USER","OVERWRITE"])
    , "EXPORT"    : Object.freeze(['CONFIG','CONFIGURATION',"TO_USER","IDENTIFIER_TRANSFORMATION"])
    , "UNLOAD"    : Object.freeze(['CONFIG','CONFIGURATION',"TO_USER","IDENTIFIER_TRANSFORMATION"])
    , "COPY"      : Object.freeze(['FILE',"FROM_USER","TO_USER"])
    , "TEST"      : Object.freeze(['FILE',"FROM_USER","TO_USER"])
    , "SERVICE"   : Object.freeze([])
    , "COMPARE"   : Object.freeze([])
  })
  
  static get ILLEGAL_ARGUMENTS() { return this.#ILLEGAL_ARGUMENTS  }
  
  static get ARGUMENT_SYNONYMS() {
    this._ARGUMENT_SYNONYMS = this._ARGUMENT_SYNONYMS || Object.freeze({
      "IMPORT"    : Object.freeze({OWNER: "TO_USER", IMPORT: "FILE"})
    , "LOAD"      : Object.freeze({OWNER: "TO_USER"}, )
    , "UPLOAD"    : Object.freeze({OWNER: "TO_USER", UPLOAD: "FILE"})
    , "EXPORT"    : Object.freeze({OWNER: "FROM_USER", EXPORT: "FILE"})
    , "UNLOAD"    : Object.freeze({OWNER: "FROM_USER"})
	, "COPY"      : Object.freeze({COPY: "CONFIG"})
	, "TEST"      : Object.freeze({TEST: "CONFIG"})
    })
    return this._ARGUMENT_SYNONYMS
  }


  #configuration
  #connectionNames
  #schemaNames
  #jobNames
  #batchNames
  
  get CONFIGURATION() {
	 return this.#configuration
  }
  
  set CONFIGURATION(v) {
    this.#configuration = v
	if (this.CONFIGURATION.connections) {
      this.CONNECTION_NAMES = Object.keys(this.CONFIGURATION.connections)
    }
	else {
	  const err = new CommandLineError(`Invalid Configuration file: Missing 'connections' section`)
      throw err
	}
	if (this.CONFIGURATION.schemas) {
	  this.SCHEMA_NAMES = Object.keys(this.CONFIGURATION.schemas)
    }
	else {
	  const err = new CommandLineError(`Invalid Configuration file: Missing 'schemas' section`)
      throw err
	}
	this.JOB_NAMES = Object.keys(this.CONFIGURATION.jobs || {})
	this.BATCH_NAMES = Object.keys(this.CONFIGURATION.batchOperations || {})
  }
  
  set CONNECTION_NAMES(v) {
	 this.#connectionNames = v
  }
  
  get CONNECTION_NAMES() {
	return this.#connectionNames
  }
  
  set SCHEMA_NAMES(v) {
	 this.#schemaNames = v
  }
  
  get SCHEMA_NAMES() {
	return this.#schemaNames
  }
  
  set JOB_NAMES(v) {
	 this.#jobNames = v
  }
  
  get JOB_NAMES() {
	return this.#jobNames
  }
  
  set BATCH_NAMES(v) {
	 this.#batchNames = v
  }
  
  get BATCH_NAMES() {
	return this.#batchNames
  }
  
  createYadamu() {	  
	return new Yadamu(this.command);
  }
  
  #DATABASE_DRIVERS = {}
  get DATABASE_DRIVERS() { 
    return this.#DATABASE_DRIVERS
  }
  set DATABASE_DRIVERS(v) {
    this.#DATABASE_DRIVERS = v 
  }
  
  constructor() {  

    this.DATABASE_DRIVERS = YadamuConstants.YADAMU_DRIVERS

    const className = this.constructor.name.toUpperCase()
	switch (className) {
	  case 'BATCH':
	    this.command = this.getOperation('COPY')
	    break;
	  case 'EXPORT':
	  case 'UNLOAD':
	  case 'IMPORT':
	  case 'LOAD':
	  case 'IMPORT':
	  case 'UPLOAD':
	  case 'COPY':
	  case 'TEST':
	  case 'SERVICE':
	  case 'COMPARE':
	    this.command = this.getOperation(className);
	    break;
      default:
	    this.command = this.getOperation(className)
	}		

    this.yadamu = this.createYadamu()
    
    this.yadamuLogger = this.yadamu.LOGGER

	try {
	  this.validateParameters(this.yadamu.OPERATION)	
	} catch (e) {
      try {
		// Should this be 'awaited'...
	    this.yadamu.close()
      } catch(e) {
	    console.log(e)
	  }
      throw e
	}
  }

  reportError(e) {
	YadamuLibrary.reportError(e)
  }

  checkFileExists(targetFile,argumentName) {
	const resolvedPath = path.resolve(targetFile);
	try {
	  if (!fs.statSync(resolvedPath).isFile()) {
	    const err = new CommandLineError(`Found Directory ["${targetFile}"]. The path specified for the ${argumentName} argument must not resolve to a directory.`)
	    throw err;
	  }
	}
    catch (e) {
	  if (e.code && e.code === 'ENOENT') {
        const err = new CommandLineError(`File not found ["${targetFile}"]. The path specified for the ${argumentName} argument must resolve to existing file.`)
	    throw err
	  }
      throw e;
    } 
  }

  checkFileDoesNotExist(targetFile,argumentName) {
	 
	const resolvedPath = path.resolve(targetFile);
	try {
      if (fs.statSync(resolvedPath)) {
	    const err = new CommandLineError(`File exists ["${targetFile}"]. The path specified for the ${argumentName} argument must not resolve to an existing file. Specify OVERWRITE=true to allow the file to be overwritten.`)
	    throw err;
	  }
	} catch (e) {
	  if (e.code && e.code === 'ENOENT') {
	  }
	  else {
	    throw e
	  }
 	}
  }

  validateParameters(command) {
	for (const synonym of Object.keys(YadamuCLI.ARGUMENT_SYNONYMS[command] || {})) {
      if (this.yadamu.COMMAND_LINE_PARAMETERS[synonym] !== undefined) {
	    const argument = YadamuCLI.ARGUMENT_SYNONYMS[command][synonym] 
		this.yadamu.appendSynonym(argument,this.yadamu.parameters[synonym])
	  }
    }
	  
	for (const argument of YadamuCLI.REQUIRED_ARGUMENTS[command]) {
	  if (this.yadamu.COMMAND_LINE_PARAMETERS[argument] === undefined) {
	    const err = new CommandLineError(`"${command}" requires that the following arguments ${JSON.stringify(YadamuCLI.REQUIRED_ARGUMENTS[command])} be provided on the command line`)
        throw err
	  }
	}
    
	// TODO ### Check for illegal arguments

    if (YadamuCLI.CONFIGURATION_REQUIRED.includes(command)) {
	  this.checkFileExists(this.yadamu.CONFIG,'CONFIG') 
    }
	
	if (YadamuCLI.FILE_MUST_EXIST.includes(command)) {
	  this.checkFileExists(this.yadamu.FILE,'FILE')
	}
	
	if ((YadamuCLI.FILE_MUST_NOT_EXIST.includes(command)) && (this.yadamu.COMMAND_LINE_PARAMETERS.OVERWRITE !== true)) {
      this.checkFileDoesNotExist(this.yadamu.FILE,'FILE')
	}

    for (const parameter of Object.keys(this.yadamu.COMMAND_LINE_PARAMETERS)) {
	  if (YadamuCLI.ENUMERATED_PARAMETERS[parameter] !== undefined) {
	    if (!YadamuCLI.ENUMERATED_PARAMETERS[parameter].includes(this.yadamu.COMMAND_LINE_PARAMETERS[parameter])) {
  	      const err = new CommandLineError(`Valid values for parameter "${parameter}" are ${JSON.stringify(YadamuCLI.ENUMERATED_PARAMETERS[parameter])}.`)
          throw err
	    }
	  }
    }	
  }
  
  getCommand() {
	return this.command
  }
  
  setParameter(parameterName,parameterValue) {

    switch (parameterName.toUpperCase()) {
	  case 'EXPORT':		  
	  case '--EXPORT':
        commands.push('EXPORT')
        this.parameters.FILE = parameterValue;
	    break;
      case 'IMPORT':		  
      case '--IMPORT':
        commands.push('IMPORT')
        this.parameters.FILE = parameterValue;
	    break;
      case 'UPLOAD':		  
      case '--UPLOAD':
        commands.push('UPLOAD')
        this.parameters.FILE = parameterValue;
	    break;
      case 'COPY':		  
      case '--COPY':
        commands.push('COPY')
        this.parameters.CONFIG = parameterValue;
	    break;
      case 'OVERWRITE':		  
      case '--OVERWRITE':
        this.parameters.OVERWRITE = parameterValue.toUpperCase();
	    break;
      case 'FILE':		  
      case '--FILE':
        this.parameters.FILE = parameterValue;
	    break;
      case 'CONFIG':		  
      case '--CONFIG':
      case 'CONFIGURATION':		  
      case '--CONFIGURATION':
        this.parameters.CONFIG = parameterValue;
	    break;   
	}
  }  
  
  getOperation(className) {

	const commands = []
    this.parameters = {}
	
	process.argv.forEach((arg) => {
      if (arg.indexOf('=') > -1) {
        const parameterName = arg.substring(0,arg.indexOf('='));
        const parameterValue = arg.substring(arg.indexOf('=')+1);
		this.setParameter(parameterName,parameterValue)
      }
    })	

	let err
	let command
	
	if (YadamuCLI.REQUIRES_SWITCH.indexOf(className) > -1) {
  	  switch (commands.length) {
	    case 0:
          err = new CommandLineError(`Must specify one of ${JSON.stringify(YadamuCLI.VALID_SWITCHES)} on the command line`)
	      throw err;
	      break;
        case 1:
		  command = commands[0]
          break;
	    default:
	      err = new CommandLineError(`Conflicting operations: Please specify just one of ${JSON.stringify(YadamuCLI.VALID_SWITCHES)} on the command line`)
	      throw err;
	  }
	}
	else {
      switch (commands.length) {
	    case 0:
		  command = className
	      break;
	    default:
		  err = new CommandLineError(`The switches ${JSON.stringify(YadamuCLI.VALID_SWITCHES)} are not valid with the "${this.constructor.name}" command.`)
	      throw err;
	  }
	}
	
	return command;
  }
	    
  async close() {
    await this.yadamu.close()
  }
    
  getParameters() {
	return this.parameters;
  }

  getYadamu() {
	return this.yadamu;
  }
  
  validateConfiguration() {
  }
  
  
  expandConfiguration(configuration,parentFile) {
      
    if (typeof configuration.connections === "string") {
      configuration.connections = YadamuLibrary.loadIncludeFile(configuration.connections,parentFile,this.yadamuLogger)
    }
    if (typeof configuration.schemas === "string") {
      configuration.schemas = YadamuLibrary.loadIncludeFile(configuration.schemas,parentFile,this.yadamuLogger)
    }
    if (typeof configuration.parameters === "string") {
      configuration.parameters = YadamuLibrary.loadIncludeFile(configuration.parameter,parentFile,this.yadamuLogger)
    }
    if (typeof configuration.jobs === "string") {
      configuration.jobs = YadamuLibrary.loadIncludeFile(configuration.jobs,parentFile,this.yadamuLogger)
    }    
    if (typeof configuration.tasks === "string") {
      configuration.tasks = YadamuLibrary.loadIncludeFile(configuration.tasks,parentFile,this.yadamuLogger)
    }

    this.validateConfiguration()
  }
  
  loadConfigurationFile() {
	if (YadamuCLI.CONFIGURATION_REQUIRED.indexOf(this.command) > -1) {
	  const configuration = YadamuLibrary.loadJSON(this.yadamu.CONFIG,this.yadamuLogger);
      this.expandConfiguration(configuration,this.yadamu.CONFIG);
	  return configuration;
	}
  }
 
  async getDatabaseInterface(yadamu,driver,connectionSettings,configurationParameters,schema) {
	  
    let dbi = undefined
    
	// Force parameter names to Upper Case.
	
	const parameters = Object.fromEntries(Object.entries(configurationParameters).map(([k, v]) => [k.toUpperCase(), v]));
	
	// clone the connectionSettings
	const connectionInfo = {
	  ...connectionSettings
	}

    if (this.DATABASE_DRIVERS.hasOwnProperty(driver)) { 
	  const DBI = (await import(this.DATABASE_DRIVERS[driver])).default
	  dbi = YadamuConstants.FILE_BASED_DRIVERS.includes(driver) ? new DBI(yadamu,connectionInfo,parameters) : new DBI(yadamu,null,connectionInfo,parameters);
    }	
    else {   
	  const message = `Unsupported database interface "${driver}".`
      this.yadamuLogger.info([`${this.constructor.name}.getDatabaseInterface()`],message);  
	  const err = new ConfigurationFileError(`[${this.constructor.name}.getDatabaseInterface()]: ${message}`);
	  throw err
    }

    dbi.setParameters(parameters);
	return dbi;
  }
   
  getDescription(connectionName,dbi) {
	return `${connectionName}://${dbi.DESCRIPTION}`
  }
  
  addParameters(parameters,jobParameters) {
    Object.keys(jobParameters || {}).forEach((parameterName) => {
	  parameters[parameterName.toUpperCase()] = jobParameters[parameterName]
	})
  }
   
  async getSourceConnection(yadamu,job) {
  	assert(this.CONNECTION_NAMES.includes(job.source.connection),new ConfigurationFileError(`Source Connection "${job.source.connection}" not found. Valid connections: "${this.CONNECTION_NAMES}".`))
    const sourceConnection = this.CONFIGURATION.connections[job.source.connection]

    const sourceDatabase =   YadamuLibrary.getVendorName(sourceConnection)
    const sourceParameters = {
	  ...job.parameters
	, CONNECTION_NAME : job.source.connection
	}
	
	let sourceSchema
	if (!YadamuConstants.FILE_BASED_DRIVERS.includes(sourceDatabase)) {
      assert(this.SCHEMA_NAMES.includes(job.source.schema),new ConfigurationFileError(`Source Schema: "${job.source.schema}" not found. Valid schemas: "${this.SCHEMA_NAMES}".`))
      sourceSchema = this.CONFIGURATION.schemas[job.source.schema]
	}

    const dbi = await this.getDatabaseInterface(yadamu,sourceDatabase,sourceConnection,sourceParameters);
	dbi.setSchema(sourceSchema,'FROM_USER')
	delete dbi.parameters.TO_USER
	return dbi
	
  }

  async getTargetConnection(yadamu,job) {

    assert(this.CONNECTION_NAMES.includes(job.target.connection),new ConfigurationFileError(`Target Connection "${job.target.connection}" not found. Valid connections: "${this.CONNECTION_NAMES}".`))
	const targetConnection = this.CONFIGURATION.connections[job.target.connection]
	
    const targetDatabase = YadamuLibrary.getVendorName(targetConnection);
	const targetParameters = {
      ...job.parameters
	, CONNECTION_NAME : job.target.connection
	}
	
	let targetSchema
	if (!YadamuConstants.FILE_BASED_DRIVERS.includes(targetDatabase)) {
      assert(this.SCHEMA_NAMES.includes(job.target.schema),new ConfigurationFileError(`Target Schema: "${job.target.schema}" not found. Valid schemas: "${this.SCHEMA_NAMES}".`))
      targetSchema = this.CONFIGURATION.schemas[job.target.schema]
	}

    const dbi = await this.getDatabaseInterface(yadamu,targetDatabase,targetConnection,targetParameters);
	dbi.setSchema(targetSchema,'TO_USER')
	delete dbi.parameters.FROM_USER
	return dbi
	
  }
	    
  async doCompare() {
     const compareDBI = await this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
	 const comparator = await compareDBI.getComparator({});
	 await comparator.doCompare()
  }
	   
  printResults(jobName,sourceDescription,targetDescription,elapsedTime) {
    this.yadamuLogger.info([`YADAMU`,jobName],`Operation complete. Source:[${sourceDescription}]. Target:[${targetDescription}].`)
  }

  async executeJob(yadamu,configuration,job,jobName = 'JOB') {

    // console.log(configuration,job,jobName,yadamu.parameters)
	  	  
	this.CONFIGURATION = configuration

	const jobParameters = {
	  ...configuration.parameters || {}
    , ...job.parameters || {} 
    }
	
    // yadamu.reloadParameters(jobParameters);
    yadamu.updateParameters(jobParameters);
		      		
 	// Initialize constructor parameters with values from configuration file and merge job specific parameters
    
	if (job.parser === 'SQL' ) {
      const targetDBI = await this.getTargetConnection(yadamu,job)
	  const startTime = performance.now()
	  const metrics = await this.yadamu.doUpload(targetDBI);
	  const endTime = performance.now();
	  const elapsedTime = endTime - startTime

      const targetDescription = this.getDescription(job.target.connection,targetDBI)

      this.printResults(jobName,targetDBI.FILE,targetDescription,elapsedTime)   

      return {
        startTime            : startTime
	  , endTime              : endTime
	  , elapsedTime          : YadamuLibrary.stringifyDuration(elapsedTime)
	  , source               : {
   	      description        : targetDBI.FILE
	    }
 	  , target               : this.getTargetState(targetDBI,targetDescription)
   	  , metrics              : metrics
	  }
    }
    else {
	  const sourceDBI = await this.getSourceConnection(yadamu,job)
      const targetDBI = await this.getTargetConnection(yadamu,job)
	  const startTime = performance.now()
	  const metrics = await yadamu.pumpData(sourceDBI,targetDBI);     
	  const endTime = performance.now();
	  const elapsedTime = endTime - startTime

      const sourceDescription = this.getDescription(job.source.connection,sourceDBI)
      const targetDescription = this.getDescription(job.target.connection,targetDBI)
	      
      this.printResults(jobName,sourceDescription,targetDescription,elapsedTime)   
	  
      return {
        startTime            : startTime
	  , endTime              : endTime
	  , elapsedTime          : YadamuLibrary.stringifyDuration(elapsedTime)
      , source               : this.getSourceState(sourceDBI,sourceDescription)
 	  , target               : this.getTargetState(targetDBI,targetDescription)
   	  , metrics              : metrics
	  }
   	}
  }
  
  getSchemaDefinition(key) {
	switch (this.yadamu.RDBMS) {
	  case 'mssql':
	    return {
	      owner     : this.yadamu.COMMAND_LINE_PARAMETERS[key]
        , database  : this.yadamu.COMMAND_LINE_PARAMETERS.DATABASE
		}
      case 'mssql':
	    return {
	      schema    : this.yadamu.COMMAND_LINE_PARAMETERS[key]
        , database  : this.yadamu.COMMAND_LINE_PARAMETERS.DATABASE
	    }
	  default:
	    return {
   		  schema    : this.yadamu.COMMAND_LINE_PARAMETERS[key]
		}
    }
  }
  
  async doLoadStagedData1() {

	this.CONFIGURATION = {
	  connections             : {
	    target                : { [this.yadamu.RDBMS] : { /* All Paramaters come from command line Arguments */ }}
	  }
	, schemas                 : {
		source                : {  /* All Paramaters come from command line Arguments */ }
      , target                : { schema : this.yadamu.COMMAND_LINE_PARAMETERS.FROM_USER }
	  }
	}
	
	const job = {
      source      : {
	  }
    , target      : {
	  }
	}
	
	const jobParameters = {
	  ...configuration.parameters || {}
    , ...job.parameters || {} 
    }
	
    // yadamu.reloadParameters(jobParameters);
    yadamu.updateParameters(jobParameters);
	
	// The source is obtained from the target
	
	const targetDBI = await this.getTargetConnection(yadamu,job)
    this.CONFIGURATION.source = {
	  [dbi.STAGING_PLATFORM] : {}
	}
	
	const sourceDBI = await this.getSourceConnection(yadamu,job)
    
	const startTime = performance.now()
    // const metrics = await this.pumpData(yadamu,sourceDBI,targetDBI);        
	const metrics = await yadamu.pumpData(sourceDBI,targetDBI);        
	const endTime = performance.now();
	const elapsedTime = endTime - startTime

    const sourceDescription = this.getDescription(job.source.connection,sourceDBI)
    const targetDescription = this.getDescription(job.target.connection,targetDBI)
          
    this.printResults(jobName,sourceDescription,targetDescription,elapsedTime)   
	  
    return {
      startTime            : startTime
	, endTime              : endTime
	, elapsedTime          : YadamuLibrary.stringifyDuration(elapsedTime)
    , source               : this.getSourceState(sourceDBI,sourceDescription)
 	, target               : this.getTargetState(targetDBI,targetDescription)
   	, metrics              : metrics
	}
  }
    
  async doUpload() {
	const configuration = {
	  connections             : {
	    target                : { [this.yadamu.RDBMS] : { /* All Paramaters come from command line Arguments */ }}
	  }
	, schemas                 : {
		target                : this.getSchemaDefinition('TO_USER')
	  }
	}
	const job = {
	  target        : {
	    connection  : 'target'
	  , schema      : 'target'
      }
	, parser       : "SQL"
	}
	await this.executeJob(this.yadamu,configuration,job,'UPLOAD')	
    await this.yadamu.close()
  }
    
  async doUnload() {
	const configuration = {
	  connections             : {
	    [this.yadamu.RDBMS]   : { [this.yadamu.RDBMS] : { /* All Paramaters come from command line Arguments */ }}
	  , loader                : { loader : { /* All Paramaters come from command line Arguments */ }}
	  }
	, schemas                 : {
		source                : this.getSchemaDefinition('FROM_USER')
      , target                : this.getSchemaDefinition('FROM_USER') 
	  }
	}
	
	const job = {
  	  source        : {
		connection  : this.yadamu.RDBMS 
      , schema      : 'source' 
	  }
    , target        : {
	    connection  : 'loader'
	  , schema      : 'target'
      }
	}
	await this.executeJob(this.yadamu,configuration,job,'UNLOAD')	
    await this.yadamu.close()
  }

  async doLoad() {
	const configuration = {
	  connections             : {
		loader                : { loader : { /* All Paramaters come from command line Arguments */ }}
	  , [this.yadamu.RDBMS]   : { [this.yadamu.RDBMS] : { /* All Paramaters come from command line Arguments */ }}
	  }
	, schemas                 : {
		source                : this.getSchemaDefinition('FROM_USER')
      , target                : this.getSchemaDefinition('FROM_USER')
	  }
	}
	const job = {
  	  source        : {
		connection  : 'loader'
      , schema      : 'source' 
	  }
    , target        : {
	    connection  : this.yadamu.RDBMS
	  , schema      : 'target'
      }
	}
	await this.executeJob(this.yadamu,configuration,job,'LOAD')	
    await this.yadamu.close()
  }

  async doExport() {
	const configuration = {
	  connections             : {
	    [this.yadamu.RDBMS]   : { [this.yadamu.RDBMS] : { /* All Paramaters come from command line Arguments */ }}
	  , file                  : { file : { /* All Paramaters come from command line Arguments */ }}
	  }
	, schemas                 : {
		source                : this.getSchemaDefinition('FROM_USER')
      , target                : {  /* All Paramaters come from command line Arguments */ }
	  }
	}
	const job = {
  	  source        : {
		connection  : this.yadamu.RDBMS
      , schema      : 'source' 
	  }
    , target        : {
	    connection  : 'file'
	  , schema      : 'target'
      }
	}
	await this.executeJob(this.yadamu,configuration,job,'EXPORT')	
    await this.yadamu.close()
  }
   
  async doImport() {
	const configuration = {
	  connections             : {
		file                  : { file : { /* All Paramaters come from command line Arguments */ }}
	  , [this.yadamu.RDBMS]   : { [this.yadamu.RDBMS] : { /* All Paramaters come from command line Arguments */ }}
	  }
	, schemas                 : {
		source                : {  /* All Paramaters come from command line Arguments */ }
      , target                : this.getSchemaDefinition('TO_USER')
	  }
	}
	const job = {
  	  source        : {
		connection  : 'file'
      , schema      : this.yadamu.RDBMS
	  }
    , target        : {
	    connection  : this.yadamu.RDBMS
	  , schema      : 'target'
      }
	}
	await this.executeJob(this.yadamu,configuration,job,'IMPORT')	
    await this.yadamu.close()
  }

  getConnectionState(dbi,userKey,description) {
    return dbi.getDriverState(userKey,description)
  }

  getSourceState(dbi,description) {
	return this.getConnectionState(dbi,'FROM_USER',description)
  }
  
  getTargetState(dbi,description) {
	return this.getConnectionState(dbi,'TO_USER',description)
  }
	
  async runJob(job,captureLogRecords,jobName) {
  
    // Run a Job
	let stringWriter

    if (captureLogRecords) {
	  stringWriter = new StringWriter()
	  const stringLogger = YadamuLogger.streamLogger(stringWriter,this.yadamu.STATUS,this.EXCEPTION_FOLDER,this.EXCEPTION_FILE_PREFIX)
      this.yadamu.LOGGER = stringLogger
	}
	
	let results
	let startTime = performance.now()
	try {
	  this.yadamu.reset(job.parameters)
      results = await this.executeJob(this.yadamu,this.CONFIGURATION,job,jobName)
    } catch (e) {
	  if (job.abortOnError) {
		throw e 
	  }
	  const endTime = performance.now()
	  results = {
		startTime   : startTime
      , endTime     : endTime
	  , elapsedTime : YadamuLibrary.stringifyDuration(endTime - startTime)
      , metrics     : this.yadamu.METRICS
	  , error       : YadamuLibrary.serializeException(e)
      , e           : e
	  }
    }
	if (captureLogRecords) {
	  results.logs = stringWriter.toJSON()
    }
	return results
  }
  
  async runAnonymousJobs(captureLogRecords) {
	  
	// Runs an Array of Anonymous Jobs. Configuration.jobs maybe an array of joba ora jobs Object
	// Returns an Array of Results. The Last entry contains the start and end time for the operation
	
	const jobs = Array.isArray(this.CONFIGURATION.jobs) ? this.CONFIGURATION.jobs : Object.values(this.CONFIGURATION.jobs)
	const startTime = performance.now()
	results = []
	for (let job of jobs) {
	 results.push(await this.runJob(job,captureLogRecords))
	}
	results.push({
	  startTime : startTime
	, endTime   : endTime
	})
    return results
  }

  async runNamedJob(jobName,captureLogRecords) {

    // Run the specified job.
    
    const startTime = performance.now()

    let job
	let results
	try {
  	  assert(this.JOB_NAMES.includes(jobName),new ConfigurationFileError(`Job "${jobName}" not found. Valid job names: "${this.JOB_NAMES}".`))	
      job = this.CONFIGURATION.jobs[jobName]
  	  this.yadamu.reset(job.parameters)
	  const results = await this.runJob(job,captureLogRecords,jobName)
	  const endTime = performance.now()
      return {        
	    startTime   : startTime
	  , [jobName]   : results
	  , endTime     : performance.now()
	  , elapsedTime : YadamuLibrary.stringifyDuration(endTime - startTime)
	  }
	} catch(e) {
	  if (job && job.abortOnError) {
		throw e 
	  }
	  const endTime = performance.now()
	  return {
		startTime   : startTime
	  , endTime     : endTime
	  , elapsedTime : YadamuLibrary.stringifyDuration(endTime - startTime)
	  , error       : YadamuLibrary.serializeException(e)
      , e           : e
	  }
	}
  }

  async runAnonymousJobs(captureLogRecords) {
	  
	// Runs an Array of Anonymous Jobs. Configuration.jobs maybe an array of joba ora jobs Object
	// Returns an Array of Results. The Last entry contains the start and end time for the operation
	
	const results = []
	const startTime = performance.now()
	for (let job of this.CONFIGURATION.jobs) {
      results.push(await this.runJob(job,captureLogRecords))
	}
	const endTime = performance.now()
	results.push({
	  startTime : startTime
	, endTime   : endTime
	})
    return results
  }

  async runNamedJobs(captureLogRecords) {

    // Run all the jobs in the jobs object
 
	let results = {
	  startTime : performance.now()
	}
	const startTime = performance.now()
	try {
      for (let jobName of Object.keys(this.CONFIGURATION.jobs) ) {
		Object.assign(results, await this.runNamedJob(jobName,captureLogRecords) )
	  }
	  const endTime = performance.now()
      results.endTime     = endTime
	  results.elapsedTime = YadamuLibrary.stringifyDuration(endTime - startTime)
      await this.yadamu.close()
	  return results	
	} catch(e) {
	  const endTime = performance.now()
	  return {
		startTime   : startTime
	  , endTime     : endTime
	  , elapsedTime : YadamuLibrary.stringifyDuration(endTime - startTime)
	  , error       : YadamuLibrary.serializeException(e)
      , e           : e
	  }
	}
	return results
  }

  async runNamedBatch(batchName,captureLogRecords) {

    // Run the jobs in the specified batch	
	const startTime = performance.now()
    const results = {
	  startTime : startTime
	}
      
	try {
  	  assert(this.BATCH_NAMES.includes(batchName),new ConfigurationFileError(`Batch "${batchName}" not found. Valid Batch Names: "${this.BATCH_NAMES}".`))	
	  const batch = this.CONFIGURATION.batchOperations[batchName]
      for (let jobName of batch) {
		Object.assign(results, await this.runNamedJob(jobName,captureLogRecords))
	  }
	  const endTime = performance.now()
	  results.endTime     = endTime
	  results.elapsedTime = YadamuLibrary.stringifyDuration(endTime - startTime)
      await this.yadamu.close()
	  return results
	} catch(e) {
	  const endTime = performance.now()
	  return {
		startTime   : startTime
	  , endTime     : endTime
	  , elapsedTime : YadamuLibrary.stringifyDuration(endTime - startTime)
	  , error       : YadamuLibrary.serializeException(e)
      , e           : e
	  }
	}
  }
  
  async doCopy() {
	    
    this.CONFIGURATION = this.loadConfigurationFile()
	if (this.yadamu.COMMAND_LINE_PARAMETERS.BATCH_NAME && this.yadamu.COMMAND_LINE_PARAMETERS.JOB_NAME) {
      const err = new CommandLineError(`Parameters 'JOB_NAME and 'BATCH_NAME' are mutally exclusive.`)
	  throw err
	}
	let results
	switch (true) {
	  case (Array.isArray(this.CONFIGURATION.jobs) && (this.CONFIGURATION.jobs.length === 1)):
	    // Run a single job
	    results = await this.runJob(this.CONFIGURATION.jobs[0],false)
	    break
	  case (Array.isArray(this.CONFIGURATION.jobs)):
	    // Run a set of anonymous jobs
	    results = await this.runAnonymousJobs(false)
        break
      case (this.JOB_NAMES.length === 1):
	    // Run the only named job
	    results = await this.runNamedJob(this.JOB_NAMES[0],false)
	    break
	  case (this.yadamu.COMMAND_LINE_PARAMETERS.hasOwnProperty('JOB_NAME')):
	    // Run the specified job
	    results = await this.runNamedJob(this.yadamu.COMMAND_LINE_PARAMETERS.JOB_NAME,false)
        break
      case (this.BATCH_NAMES.length === 1):
	    // Run the only Batch
	    results = this.runNamedBatch(this.BATCH_NAMES[0],false)
        break
	  case (this.yadamu.COMMAND_LINE_PARAMETERS.BATCH_NAME):
	    // Run the specified Batch
	    results = this.runNamedBatch(this.yadamu.COMMAND_LINE_PARAMETERS.BATCH_NAME,false)
        break
	  default:
	    // Run all available jobs
	    results = await this.runNamedJobs(false)
	}
	
	if (results.e) {
	   throw results.e 
	 }
	 
  }
  
  async doEncrypt() {
	await this.yadamu.initialize()
	const startTime = performance.now();
    await this.yadamu.doEncrypt();
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`ENCRYPT`],`Operation complete: File:"${this.yadamu.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);  
  }
  
  async doDecrypt() {
	await this.yadamu.initialize()
	const startTime = performance.now();
    await this.yadamu.doDecrypt();
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`DECRYPT`],`Operation complete: File:"${this.yadamu.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);  
  }

}

export { YadamuCLI as default}

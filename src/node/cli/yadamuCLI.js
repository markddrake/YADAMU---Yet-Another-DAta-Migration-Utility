
import fs                 from 'fs';
import path               from 'path';

import { 
  performance
}                         from 'perf_hooks';

import assert             from 'assert';

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
    this._VALID_SWITCHES = this._VALID_SWITCHES || Object.freeze(['EXPORT','IMPORT','UPLOAD','COPY','TEST'])
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
    this._CONFIGURATION_REQUIRED = this._CONFIGURATION_REQUIRED || Object.freeze(['COPY','TEST','CONNECTIONTEST','SERVICE'])
    return this._CONFIGURATION_REQUIRED
  }

  static get ENUMERATED_PARAMETERS() {
    this._ENUMERATED_PARAMETERS = this._ENUMERATED_PARAMETERS || Object.freeze(['DDL_ONLY','DATA_ONLY','DDL_AND_DATA'])
    return this._ENUMERATED_PARAMETERS
  }

  static get SUPPORTED_ARGUMENTS() {
    this._SUPPORTED_ARGUMENTS = this._SUPPORTED_ARGUMENTS || Object.freeze({
      "YADAMUCMD"    : Object.freeze([])
    , "YADAMUGUI"    : Object.freeze(['INIT','IMPORT','LOAD','UPLOAD','EXPORT','UNLOAD','COPY','TEST'])
    , "YADAMUCLI"    : Object.freeze(['IMPORT','LOAD','UPLOAD','EXPORT','UNLOAD','COPY','TEST'])
    , "IMPORT"       : Object.freeze(['IMPORT'])
    , "DIRECTLOAD"   : Object.freeze(['FAST_IMPORT'])
    , "LOAD"         : Object.freeze(['LOAD'])
    , "UPLOAD"       : Object.freeze(['UPLOAD'])
    , "EXPORT"       : Object.freeze(['EXPORT'])
    , "UNLOAD"       : Object.freeze(['UNLOAD'])
    , "INIT"         : Object.freeze(['INIT'])
    , "COPY"         : Object.freeze(['COPY'])
    , "TEST"         : Object.freeze(['TEST'])
    , "SERVICE"      : Object.freeze([])
    , "COMPRARE"     : Object.freeze([])
    })
    return this._SUPPORTED_ARGUMENTS
  }

  static get REQUIRED_ARGUMENTS() {
    this._REQUIRED_ARGUMENTS = this._REQUIRED_ARGUMENTS || Object.freeze({
      "YADAMUCMD"      : Object.freeze([])
    , "YADAMUGUI"      : Object.freeze([])
    , "YADAMUCLI"      : Object.freeze([])
    , "IMPORT"         : Object.freeze(['TO_USER'])
    , "DIRECTLOAD"     : Object.freeze(['FROM_USER','TO_USER','REMOTE_STAGING_AREA'])
    , "LOAD"           : Object.freeze(['FROM_USER','TO_USER','DIRECTORY'])
    , "UPLOAD"         : Object.freeze(['TO_USER'])
    , "EXPORT"         : Object.freeze(['FROM_USER'])
    , "UNLOAD"         : Object.freeze(['FROM_USER','TO_USER','DIRECTORY'])
    , "COPY"           : Object.freeze(['CONFIG'])
    , "TEST"           : Object.freeze(['CONFIG'])
    , "SERVICE"        : Object.freeze([])
    , "COMPARE"        : Object.freeze([])
    , "CONNECTIONTEST" : Object.freeze(['CONFIG','RDBMS'])
    , "ENCRYPT"        : Object.freeze([])
    , "DECRYPT"        : Object.freeze([])
    })
    return this._REQUIRED_ARGUMENTS
  }

  static get ILLEGAL_ARGUMENTS() {
    this._ILLEGAL_ARGUMENTS = this._ILLEGAL_ARGUMENTS || Object.freeze({
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
    return this._ILLEGAL_ARGUMENTS
  }
  
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
	this.CONNECTION_NAMES = Object.keys(this.CONFIGURATION.connections)
	this.SCHEMA_NAMES = Object.keys(this.CONFIGURATION.schemas)
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
  
  constructor() {  

    const className = this.constructor.name.toUpperCase()
	switch (className) {
	  case 'PUMP':
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
      case 'TEST':		  
      case '--TEST':
        commands.push('TEST')
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
  
  async getDatabaseInterface(yadamu,driver,connectionSettings,configurationParameters) {
    let dbi = undefined
    
	// Force parameter names to Upper Case.
	
	const parameters = Object.fromEntries(Object.entries(configurationParameters).map(([k, v]) => [k.toUpperCase(), v]));
	
	// clone the connectionSettings
	const connectionInfo = {
	  ...connectionSettings
	}

    if (YadamuConstants.YADAMU_DRIVERS.hasOwnProperty(driver)) { 
	  const DBI = (await import(YadamuConstants.YADAMU_DRIVERS[driver])).default
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
    
  getUser(vendor,schema) {
     const user = vendor === 'file' || vendor === 'loader' 
		       ? 'YADAMU'
		       : vendor === 'mssql' 
	           ? schema.owner
		       : vendor === 'snowflake' 
		       ? schema.snowflake.schema 
		       : schema.schema
	 
	 assert.notStrictEqual(user,undefined,new ConfigurationFileError(`Incorrect schema specification for database vendor "${vendor}".`));
	 return user
			   
  }
    
  /*
  getDescription(db,connectionName,schemaInfo) {
    return `"${connectionName}"://"${db === 'mssql' ? `${schemaInfo.database}"."${schemaInfo.owner}` : schemaInfo.schema}"`
  }
  */
  
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
	  CONNECTION_NAME : job.source.connection
	}
	
	if (!YadamuConstants.FILE_BASED_DRIVERS.includes(sourceDatabase)) {
      assert(this.SCHEMA_NAMES.includes(job.source.schema),new ConfigurationFileError(`Source Schema: "${job.source.schema}" not found. Valid schemas: "${this.SCHEMA_NAMES}".`))
      const sourceSchema = this.CONFIGURATION.schemas[job.source.schema]
      sourceParameters.FROM_USER = this.getUser(sourceDatabase,sourceSchema)
	
      switch (sourceDatabase) {
          case 'mssql':
   	      case 'snowflake':
            sourceParameters.YADAMU_DATABASE = sourceSchema.database
           break;
         default:
      }
	}

    return await this.getDatabaseInterface(yadamu,sourceDatabase,sourceConnection,sourceParameters);
	
  }

  async getTargetConnection(yadamu,job) {

    assert(this.CONNECTION_NAMES.includes(job.target.connection),new ConfigurationFileError(`Target Connection "${job.target.connection}" not found. Valid connections: "${this.CONNECTION_NAMES}".`))
	const targetConnection = this.CONFIGURATION.connections[job.target.connection]
	
    const targetDatabase = YadamuLibrary.getVendorName(targetConnection);
	const targetParameters = {
      CONNECTION_NAME : job.target.connection
	}
	
	if (!YadamuConstants.FILE_BASED_DRIVERS.includes(targetDatabase)) {
      assert(this.SCHEMA_NAMES.includes(job.target.schema),new ConfigurationFileError(`Target Schema: "${job.target.schema}" not found. Valid schemas: "${this.SCHEMA_NAMES}".`))
      const targetSchema = this.CONFIGURATION.schemas[job.target.schema]
	
      targetParameters.TO_USER = this.getUser(targetDatabase,targetSchema)
     
  	   switch (targetDatabase) {
         case 'mssql':
	     case 'snowflake':
           targetParameters.YADAMU_DATABASE = targetSchema.database
           break;
         default:
      }
	}
	
    return await this.getDatabaseInterface(yadamu,targetDatabase,targetConnection,targetParameters);
	
  }
	    
  async doImport() {
	await this.yadamu.initialize()
	const dbi = await this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doImport(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`IMPORT`],`Operation complete: File:"${this.yadamu.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }

  async doCopyStagedData() {
	await this.yadamu.initialize()
    const dbi = await this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const stage = await this.getDatabaseInterface(this.yadamu,dbi.STAGING_PLATFORM,{},{})
	const startTime = performance.now();
    await this.yadamu.doCopy(stage,dbi);      
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`LOAD`],`Operation complete: Control File:"${stage.CONTROL_FILE_PATH}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }

  async doLoad() {
	await this.yadamu.initialize()
    const fs = await this.getDatabaseInterface(this.yadamu,'loader',{},{})
	const dbi = await this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doCopy(fs,dbi);      
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`LOAD`],`Operation complete: Control File:"${fs.CONTROL_FILE_PATH}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doUpload() {
	await this.yadamu.initialize()
	const dbi = await this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doUpload(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`UPLOAD`],`Operation complete: File:"${this.yadamu.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doExport() {
	await this.yadamu.initialize()
	const dbi = await this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doExport(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`EXPORT`],`Operation complete: File:"${this.yadamu.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doUnload() {
	await this.yadamu.initialize()
    const fs = await this.getDatabaseInterface(this.yadamu,'loader',{},{})
	const dbi = await this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doCopy(dbi,fs);      
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`UNLOAD`],`Operation complete: Control File:"${fs.CONTROL_FILE_PATH}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
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

  
  async doCompare() {
     const compareDBI = await this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
	 const comparator = await compareDBI.getComparator({});
	 await comparator.doCompare()
  }
	  
  async doCopy() {
  
    const configuration = this.loadConfigurationFile()
	this.yadamu.reloadParameters(configuration.parameters);
	await this.yadamu.initialize(configuration.parameters || {})
    const startTime = performance.now();
    for (const job of configuration.jobs) {
	  await this.executeJob(this.yadamu,configuration,job)
    }
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`COPY`],`Operation complete: Configuration:"${this.yadamu.CONFIG}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
    
  async doTests() {
    
	const configuration = this.loadConfigurationFile()
    const Yadamu = await import('../../qa/core/yadamuQA.js');
	const yadamu = new Yadamu.default(configuration,this.yadamu.activeConnections);
    await yadamu.initialize()
	const startTime = performance.now();
	const results = await yadamu.doTests(configuration);
	const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.log([`QA`,`YADAMU`,`REGRESSION`,`${this.yadamu.CONFIG}`],`${results} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);

  }
  
  async performJob() {
	this.CONFIGURATION = this.loadConfigurationFile()
	const jobName = this.yadamu.parameters.TASK
  	assert(this.JOB_NAMES.includes(jobName),new ConfigurationFileError(`Job "${jobName}" not found. Valid Job names: "${this.JOB_NAMES}".`))	
    const job = this.CONFIGURATION.jobs[jobName]
	await this.yadamu.initialize()
	const metrics = await this.executeJob(this.yadamu,this.CONFIGURATION,job)
    return metrics
  }

  async executeJob(yadamu,configuration,job) {
    
	
	this.CONFIGURATION = configuration
	// Initialize constructor parameters with values from configuration file and merge job specific parameters
    
	const jobParameters = {
	  ...configuration.parameters || {}
    , ...job.parameters || {} 
    }
	
    yadamu.reloadParameters(jobParameters);
	
    const sourceDBI = await this.getSourceConnection(yadamu,job)
    const targetDBI = await this.getTargetConnection(yadamu,job)
    const metrics = await yadamu.doCopy(sourceDBI,targetDBI);       
	
    const sourceDescription = this.getDescription(job.source.connection,sourceDBI)
    const targetDescription = this.getDescription(job.target.connection,targetDBI)
          
    this.yadamuLogger.info([`YADAMU`,`JOB`],`Operation complete. Source:[${sourceDescription}]. Target:[${targetDescription}].`);
	
	return {
      source           : {
		connection     : sourceDBI.parameters.CONNECTION_NAME
	  , schema         : sourceDBI.parameters.FROM_USER
	  , vendor         : sourceDBI.DATABASE_VENDOR
	  , description    : sourceDescription
	  }
	, target           : {
		connection     : targetDBI.parameters.CONNECTION_NAME
	  , schema         : targetDBI.parameters.TO_USER
	  , vendor         : targetDBI.DATABASE_VENDOR
	  , target         : targetDescription
	  } 
	, metrics          : metrics
	}
  }
    
}


export { YadamuCLI as default}
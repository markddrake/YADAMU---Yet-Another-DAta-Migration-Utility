"use strict" 

const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');
const assert = require('assert');

const Yadamu = require('./yadamu.js');
const YadamuConstants = require('./yadamuConstants.js');
const YadamuLibrary = require('./yadamuLibrary.js');
const YadamuLogger = require('./yadamuLogger.js');

const {CommandLineError, ConfigurationFileError}  = require('./yadamuException.js');

const {FileNotFound} = require('../file/node/fileException.js');

const YadamuCompare = require('../../YADAMU_QA/common/node/yadamuQA.js')


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
    this._CONFIGURATION_REQUIRED = this._CONFIGURATION_REQUIRED || Object.freeze(['COPY','TEST','CONNECTIONTEST'])
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
    })
    return this._SUPPORTED_ARGUMENTS
  }

  static get REQUIRED_ARGUMENTS() {
    this._REQUIRED_ARGUMENTS = this._REQUIRED_ARGUMENTS || Object.freeze({
      "YADAMUCMD"      : Object.freeze([])
    , "YADAMUGUI"      : Object.freeze([])
    , "YADAMUCLI"      : Object.freeze([])
    , "IMPORT"         : Object.freeze(['TO_USER'])
    , "DIRECTLOAD"     : Object.freeze(['FROM_USER','TO_USER','DIRECTORY'])
    , "LOAD"           : Object.freeze(['FROM_USER','TO_USER','DIRECTORY'])
    , "UPLOAD"         : Object.freeze(['TO_USER'])
    , "EXPORT"         : Object.freeze(['FROM_USER'])
    , "UNLOAD"         : Object.freeze(['FROM_USER','TO_USER','DIRECTORY'])
    , "COPY"           : Object.freeze(['CONFIG'])
    , "TEST"           : Object.freeze(['CONFIG'])
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
	  

  constructor() {  

    const className = this.constructor.name.toUpperCase()
	switch (className) {
	  case 'EXPORT':
	  case 'UNLOAD':
	  case 'IMPORT':
	  case 'LOAD':
	  case 'IMPORT':
	  case 'UPLOAD':
	  case 'COPY':
	  case 'TEST':
	    this.command = this.getOperation(className);
	    break;
      default:
	    this.command = this.getOperation(className)
	}		

    this.yadamu = new Yadamu(this.command);
    
    this.yadamuLogger = this.yadamu.LOGGER

	try {
	  this.validateParameters(this.command)	
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
	  
	for (const synonym of Object.keys(YadamuCLI.ARGUMENT_SYNONYMS[command])) {
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
  
  getOperation(className) {
	  
	const commands = []
    this.parameters = {}
    process.argv.forEach((arg) => {
 
      if (arg.indexOf('=') > -1) {
        const parameterName = arg.substring(0,arg.indexOf('='));
        const parameterValue = arg.substring(arg.indexOf('=')+1);
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
  
  getDatabaseInterface(yadamu,driver,connectionSettings,configurationparameters) {

    let dbi = undefined
    
	const parameters = Object.assign({}, configurationparameters || {})
    
	// clone the connectionSettings
	const connection = Object.assign({}, connectionSettings);
	
    if (YadamuConstants.YADAMU_DRIVERS.hasOwnProperty(driver)) { 
	  const DBI = require(YadamuConstants.YADAMU_DRIVERS[driver]);
	  dbi = new DBI(this.yadamu,connection,parameters);
    }	
    else {   
	  const message = `Unsupported database vendor "${driver}".`
      this.yadamuLogger.info([`${this.constructor.name}.getDatabaseInterface()`],message);  
	  const err = new ConfigurationFileError(`[${this.constructor.name}.getDatabaseInterface()]: ${message}`);
	  throw err
    }

    dbi.setParameters(parameters);
	return dbi;
  }
  
  getUser(vendor,schema) {
    
     const user = vendor === 'mssql' ? schema.owner : schema.schema
	 assert.notStrictEqual(user,undefined,new ConfigurationFileError(`Incorrect schema specification for database vendor "${vendor}".`));
	 return user
     
  }
  
  getDescription(db,connectionName,schemaInfo) {
    return `"${connectionName}"://"${db === 'mssql' ? `${schemaInfo.database}"."${schemaInfo.owner}` : schemaInfo.schema}"`
  }
  
  async doImport() {
	await this.yadamu.initialize()
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doImport(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`IMPORT`],`Operation complete: File:"${this.yadamu.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }

  async doCopyBasedImport() {
	await this.yadamu.initialize()
	this.yadamu.STAGING_AREA = 'loader'
    const stage = this.getDatabaseInterface(this.yadamu,this.yadamu.STAGING_AREA,{},{})
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doCopyBasedImport(stage,dbi);      
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`LOAD`],`Operation complete: Control File:"${stage.controlFilePath}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }

  async doLoad() {
	await this.yadamu.initialize()
    const fs = this.getDatabaseInterface(this.yadamu,'loader',{},{})
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doCopy(fs,dbi);      
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`LOAD`],`Operation complete: Control File:"${fs.controlFilePath}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doUpload() {
	await this.yadamu.initialize()
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doUpload(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`UPLOAD`],`Operation complete: File:"${this.yadamu.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doExport() {
	await this.yadamu.initialize()
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doExport(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`EXPORT`],`Operation complete: File:"${this.yadamu.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doUnload() {
	await this.yadamu.initialize()
    const fs = this.getDatabaseInterface(this.yadamu,'loader',{},{})
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doCopy(dbi,fs);      
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`UNLOAD`],`Operation complete: Control File:"${fs.controlFilePath}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
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

  async doCopy() {
  
    const configuration = this.loadConfigurationFile()
	this.yadamu.reloadParameters(configuration.parameters);
	await this.yadamu.initialize(configuration.parameters || {})
    const startTime = performance.now();
    for (const job of configuration.jobs) {

      // Initialize constructor parameters with values from configuration file
      const jobParameters = Object.assign({} , configuration.parameters ? configuration.parameters : {})
      // Merge job specific parameters
      Object.assign(jobParameters,job.parameters ? job.parameters : {})
	
      const sourceConnection = configuration.connections[job.source.connection]
	  assert.notStrictEqual(sourceConnection,undefined,new ConfigurationFileError(`Source Connection "${job.source.connection}" not found. Valid connections: "${Object.keys( configuration.connections)}".`))
	  
      const sourceSchema = configuration.schemas[job.source.schema]
	  assert.notStrictEqual(sourceSchema,undefined,new ConfigurationFileError(`Source Schema: Named schema "${job.source.schema}" not found. Valid schemas: "${Object.keys( configuration.schemas)}".`))
	  
      const sourceDatabase =  YadamuLibrary.getVendorName(sourceConnection)
      const sourceDescription = this.getDescription(sourceDatabase,job.source.connection,sourceSchema)

      const targetConnection = configuration.connections[job.target.connection]
	  assert.notStrictEqual(targetConnection,undefined,new ConfigurationFileError(`Target Connection "${job.target.connection}" not found. Valid connections: "${Object.keys( configuration.connections)}".`))

      const targetSchema = configuration.schemas[job.target.schema]
	  assert.notStrictEqual(targetSchema,undefined,new ConfigurationFileError(`Target Schema: Named schema "${job.source.schema}" not found. Valid schemas: "${Object.keys( configuration.schemas)}".`))

      const targetDatabase =  YadamuLibrary.getVendorName(targetConnection);
      const targetDescription = this.getDescription(targetDatabase,job.target.connection,targetSchema)
          
	  const sourceParameters = Object.assign({},jobParameters || {})
      sourceParameters.FROM_USER = this.getUser(sourceDatabase,sourceSchema)
	  
      switch (sourceDatabase) {
         case 'mssql':
		 case 'snowflake':
           sourceParameters.YADAMU_DATABASE = sourceSchema.database
           break;
         default:
      }
	  
	  const targetParameters = Object.assign({},jobParameters || {})
      targetParameters.TO_USER = this.getUser(targetDatabase,targetSchema)
	  
	  switch (targetDatabase) {
         case 'mssql':
		 case 'snowflake':
           targetParameters.YADAMU_DATABASE = targetSchema.database
           break;
         default:
      }
	  
      this.yadamu.reloadParameters(jobParameters);
	  if (this.yadamu.MODE === 'COMPARE') {
		const compare = new YadamuCompare({parameters: jobParameters},new Set());
	    await compare.doCompare(this.yadamu,sourceConnection,targetConnection,sourceSchema,targetSchema)
	  }
	  else {
	    const sourceDBI = this.getDatabaseInterface(this.yadamu,sourceDatabase,sourceConnection,sourceParameters)
        const targetDBI = this.getDatabaseInterface(this.yadamu,targetDatabase,targetConnection,targetParameters)    
        if (this.yadamu.MODE === 'COPY') {
  		  await this.yadamu.doCopyBasedImport(sourceDBI,targetDBI) 
	    }
	    else {
          await this.yadamu.doCopy(sourceDBI,targetDBI);      
	    }
      }
      this.yadamuLogger.info([`YADAMU`,`COPY`],`Operation complete. Source:[${sourceDescription}]. Target:[${targetDescription}].`);
    }
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`COPY`],`Operation complete: Configuration:"${this.yadamu.CONFIG}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
    
  async doTests() {
    
	const configuration = this.loadConfigurationFile()
    const YadamuQA = require('../../YADAMU_QA/common/node/yadamuQA.js');
	const yadamuQA = new YadamuQA(configuration,this.yadamu.activeConnections);
    await yadamuQA.initialize()
	const startTime = performance.now();
	const results = await yadamuQA.doTests(configuration);
	const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.qa([`YADAMU`,`REGRESSION`,`${this.yadamu.CONFIG}`],`${results} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);

  }
  
}

module.exports = YadamuCLI;
"use strict" 

const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');
const assert = require('assert');

const Yadamu = require('./yadamu.js');
const YadamuConstants = require('./yadamuConstants.js');
const YadamuLibrary = require('./yadamuLibrary.js');
const YadamuLogger = require('./yadamuLogger.js');
const {YadamuError, UserError, CommandLineError, ConfigurationFileError, ConnectionError} = require('./yadamuError.js');

const FileDBI = require('../file/node/fileDBI.js');

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
** THe Command line argument file must be specified when using the Import, Export, Upload Copy and Test classes.
** 
** IMPORT, UPLOAD, COPY, TEST and INIT operaitons require that the file exists
** EXPORT operations require that the file does not exists unless the command line argument OVERWRITE=YES is specified
**
*/

class YadamuCLI {

  static get REQUIRES_SWITCH() {
    YadamuCLI._REQUIRES_SWITCH = YadamuCLI._REQUIRES_SWITCH || Object.freeze(['YADAMUCLI' , 'YADAMUCMD'])
    return YadamuCLI._REQUIRES_SWITCH
  }

  static get VALID_SWITCHES() {
    YadamuCLI._VALID_SWITCHES = YadamuCLI._VALID_SWITCHES || Object.freeze(['EXPORT','IMPORT','UPLOAD','COPY','TEST'])
    return YadamuCLI._VALID_SWITCHES
  }

  static get FILE_MUST_EXIST() {
    YadamuCLI._FILE_MUST_EXIST = YadamuCLI._FILE_MUST_EXIST || Object.freeze(['IMPORT'])
    return YadamuCLI._FILE_MUST_EXIST
  }
  
  static get FILE_MUST_NOT_EXIST() {
    YadamuCLI._FILE_MUST_NOT_EXIST = YadamuCLI._FILE_MUST_NOT_EXIST || Object.freeze(['EXPORT'])
    return YadamuCLI._FILE_MUST_NOT_EXIST
  }
  
  static get CONFIGURATION_REQUIRED() {
    YadamuCLI._CONFIGURATION_REQUIRED = YadamuCLI._CONFIGURATION_REQUIRED || Object.freeze(['COPY','TEST','CONNECTIONTEST'])
    return YadamuCLI._CONFIGURATION_REQUIRED
  }

  static get ENUMERATED_PARAMETERS() {
    YadamuCLI._ENUMERATED_PARAMETERS = YadamuCLI._ENUMERATED_PARAMETERS || Object.freeze(['DDL_ONLY','DATA_ONLY','DDL_AND_DATA'])
    return YadamuCLI._ENUMERATED_PARAMETERS
  }

  static get SUPPORTED_ARGUMENTS() {
    YadamuCLI._SUPPORTED_ARGUMENTS = YadamuCLI._SUPPORTED_ARGUMENTS || Object.freeze({
      "YADAMUCMD" : Object.freeze([])
    , "YADAMUGUI" : Object.freeze(['INIT','IMPORT','LOAD','UPLOAD','EXPORT','UNLOAD','COPY','TEST'])
    , "YADAMUCLI" : Object.freeze(['IMPORT','LOAD','UPLOAD','EXPORT','UNLOAD','COPY','TEST'])
    , "IMPORT"    : Object.freeze(['IMPORT'])
    , "LOAD"      : Object.freeze(['LOAD'])
    , "UPLOAD"    : Object.freeze(['UPLOAD'])
    , "EXPORT"    : Object.freeze(['EXPORT'])
    , "UNLOAD"    : Object.freeze(['UNLOAD'])
    , "INIT"      : Object.freeze(['INIT'])
    , "COPY"      : Object.freeze(['COPY'])
    , "TEST"      : Object.freeze(['TEST'])
    })
    return YadamuCLI._SUPPORTED_ARGUMENTS
  }

  static get REQUIRED_ARGUMENTS() {
    YadamuCLI._REQUIRED_ARGUMENTS = YadamuCLI._REQUIRED_ARGUMENTS || Object.freeze({
      "YADAMUCMD"      : Object.freeze([])
    , "YADAMUGUI"      : Object.freeze([])
    , "YADAMUCLI"      : Object.freeze([])
    , "IMPORT"         : Object.freeze(['TO_USER'])
    , "LOAD"           : Object.freeze(['TO_USER'])
    , "UPLOAD"         : Object.freeze(['TO_USER'])
    , "EXPORT"         : Object.freeze(['FROM_USER'])
    , "UNLOAD"         : Object.freeze(['FROM_USER'])
    , "COPY"           : Object.freeze(['CONFIG'])
    , "TEST"           : Object.freeze(['CONFIG'])
    , "CONNECTIONTEST" : Object.freeze(['CONFIG','RDBMS'])
    })
    return YadamuCLI._REQUIRED_ARGUMENTS
  }

  static get ILLEGAL_ARGUMENTS() {
    YadamuCLI._ILLEGAL_ARGUMENTS = YadamuCLI._ILLEGAL_ARGUMENTS || Object.freeze({
      "YADAMUCLI" : Object.freeze(['FILE','CONFIG','CONFIGURATION'])
    , "YADAMUCMD" : Object.freeze(['FILE','CONFIG','CONFIGURATION'])
    , "IMPORT"    : Object.freeze(['CONFIG','CONFIGURATION',"FROM_USER","OVERWRITE"])
    , "LOAD"      : Object.freeze(['CONFIG','CONFIGURATION',"FROM_USER","OVERWRITE"])
    , "UPLOAD"    : Object.freeze(['CONFIG','CONFIGURATION',"FROM_USER","OVERWRITE"])
    , "EXPORT"    : Object.freeze(['CONGIG','CONFIGURATION',"TO_USER"])
    , "UNLOAD"    : Object.freeze(['CONGIG','CONFIGURATION',"TO_USER"])
    , "COPY"      : Object.freeze(['FILE',"FROM_USER","TO_USER"])
    , "TEST"      : Object.freeze(['FILE',"FROM_USER","TO_USER"])
    })
    return YadamuCLI._ILLEGAL_ARGUMENTS
  }

  static reportError(e) {
	if (e instanceof UserError) {
      console.log(e.message);
	  if (process.env.YADAMU_SHOW_CAUSE === 'TRUE') {	  
	    console.log(e); 
      }
  	}
	else {
      console.log(e);
  	}
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
        const err = new CommandLineError(`File not found ["${targetFile}"]. THe path specified for the ${argumentName} argument must resolve to existing file.`)
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

  loadJSON(path) {
  
    /*
    ** 
    ** Use instead of 'requiring' configuration files. Avoids loading configuration files into node's "Require" cache
    **
    ** ### TODO : Check file exists and has reasonable upper limit for size before processeing
    ** 
    */ 
		 
	try {
      const fileContents = fs.readFileSync(path);
	  try {
	    return JSON.parse(fileContents);
      } catch (e) {
        const message = `JSON parsing error "${e.message}" while parsing "${path}".`;
		this.yadamuLogger.error([`${this.constructor.name}.loadJSON()`],message)   
        throw new ConfigurationFileError(`[${this.constructor.name}.loadJSON()] ${message}`) 
      } 
	} catch (e) {
      switch (true) {
		case (e.errno && (e.errno === -4058)):
		  let message = `Cannot load JSON file "${path}".`;
		  this.yadamuLogger.error([`${this.constructor.name}.loadJSON()`],message)   
          throw new CommandLineError(`[${this.constructor.name}.loadJSON()] ${message}`)
	    default:
          throw e;		
	  }
	}
  }
	  
	  
  validateConfiguration() {
  }
  
  expandConfiguration(configuration) {
      
    if (typeof configuration.connections === "string") {
      configuration.connections = this.loadJSON(path.resolve(configuration.connections))
    }
    if (typeof configuration.schemas === "string") {
      configuration.schemas = this.loadJSON(path.resolve(configuration.schemas))
    }
    if (typeof configuration.parameters === "string") {
      configuration.parameters = this.loadJSON(path.resolve(configuration.parameters))
    }
    if (typeof configuration.jobs === "string") {
      configuration.jobs = this.loadJSON(path.resolve(configuration.jobs))
    }    
    if (typeof configuration.tasks === "string") {
      configuration.tasks = this.loadJSON(path.resolve(configuration.tasks))
    }

    this.validateConfiguration()
  }
  
  loadConfigurationFile() {
	if (YadamuCLI.CONFIGURATION_REQUIRED.indexOf(this.command) > -1) {
      const configuration = this.loadJSON(this.yadamu.CONFIG);
      this.expandConfiguration(configuration);
	  return configuration;
	}
  }
  
  getDatabaseInterface(yadamu,driver,connectionProperties,parameters) {

    let dbi = undefined
    
    if (YadamuConstants.YADAMU_DRIVERS.hasOwnProperty(driver)) { 
	  const DBI = require(YadamuConstants.YADAMU_DRIVERS[driver]);
	  dbi = new DBI(this.yadamu);
    }	
    else {   
	  const message = `Unsupported database vendor "${driver}".`
      this.yadamuLogger.info([`${this.constructor.name}.getDatabaseInterface()`],message);  
	  const err = new ConfigurationFileError(`[${this.constructor.name}.getDatabaseInterface()]: ${message}`);
	  throw err
    }

    dbi.setConnectionProperties(connectionProperties);
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
  
 
  async doCopy() {
  
    const configuration = this.loadConfigurationFile()
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
	  
      const sourceDatabase =  Object.keys(sourceConnection)[0];
      const sourceDescription = this.getDescription(sourceDatabase,job.source.connection,sourceSchema)

      const targetConnection = configuration.connections[job.target.connection]
	  assert.notStrictEqual(targetConnection,undefined,new ConfigurationFileError(`Target Connection "${job.target.connection}" not found. Valid connections: "${Object.keys( configuration.connections)}".`))

      const targetSchema = configuration.schemas[job.target.schema]
	  assert.notStrictEqual(targetSchema,undefined,new ConfigurationFileError(`Target Schema: Named schema "${job.source.schema}" not found. Valid schemas: "${Object.keys( configuration.schemas)}".`))

      const targetDatabase =  Object.keys(targetConnection)[0];
      const targetDescription = this.getDescription(targetDatabase,job.target.connection,targetSchema)
          
	  const sourceParameters = {
		FROM_USER: this.getUser(sourceDatabase,sourceSchema)
	  }
	  
      switch (sourceDatabase) {
         case 'mssql':
           sourceParameters.YADAMU_DATABASE = sourceSchema.database
           break;
         default:
      }
	  
	  const targetParameters = {
		TO_USER: this.getUser(targetDatabase,targetSchema)
	  }
      switch (targetDatabase) {
         case 'mssql':
           targetParameters.YADAMU_DATABASE = targetSchema.database
           break;
         default:
      }

      this.yadamu.reloadParameters(jobParameters);
      const sourceDBI = this.getDatabaseInterface(this.yadamu,sourceDatabase,sourceConnection[sourceDatabase],sourceParameters)
      const targetDBI = this.getDatabaseInterface(this.yadamu,targetDatabase,targetConnection[targetDatabase],targetParameters)    
      
      await this.yadamu.doCopy(sourceDBI,targetDBI);      
      this.yadamuLogger.info([`YADAMU`,`COPY`],`Operation complete. Source:[${sourceDescription}]. Target:[${targetDescription}].`);
    }
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`COPY`],`Operation complete: Configuration:"${this.yadamu.CONFIG}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doImport() {
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doImport(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`IMPORT`],`Operation complete: File:"${this.yadamu.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doLoad() {
    const fs = this.getDatabaseInterface(this.yadamu,'loader',{},{})
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doCopy(fs,dbi);      
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`LOAD`],`Operation complete: Control File:"${fs.controlFilePath}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doUpload() {
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doUpload(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`UPLOAD`],`Operation complete: File:"${this.yadamu.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doExport() {
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doExport(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`EXPORT`],`Operation complete: File:"${this.yadamu.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doUnload() {
    const fs = this.getDatabaseInterface(this.yadamu,'loader',{},{})
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.RDBMS,{},{})
    const startTime = performance.now();
    await this.yadamu.doCopy(dbi,fs);      
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`YADAMU`,`UNLOAD`],`Operation complete: Control File:"${fs.controlFilePath}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doTests() {
    
	const configuration = this.loadConfigurationFile()
    const YadamuQA = require('../../YADAMU_QA/common/node/yadamuQA.js');
	const yadamuQA = new YadamuQA(configuration);
	const startTime = performance.now();
	const results = await yadamuQA.doTests(configuration);
	const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.qa([`YADAMU`,`TEST`,`${this.yadamu.CONFIG}`],`${results} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);

  }
  
}

module.exports = YadamuCLI;
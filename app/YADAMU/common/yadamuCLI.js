"use strict" 

const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');
const assert = require('assert');

const Yadamu = require('./yadamu.js');
const YadamuLibrary = require('./yadamuLibrary.js');
const YadamuLogger = require('./yadamuLogger.js');
const {YadamuError, UserError, CommandLineError, ConfigurationFileError, ConnectionError} = require('./yadamuError.js');

const FileDBI = require('../file/node/fileDBI.js');

const REQUIRES_SWITCH = ['YADAMUCLI' , 'YADAMUCMD']
const VALID_SWITCHES = ['EXPORT','IMPORT','UPLOAD','COPY','TEST']

const FILE_MUST_EXIST = ['IMPORT']
const FILE_MUST_NOT_EXIST = ['EXPORT']
const CONFIGURATION_REQUIRED = ['COPY','TEST']

const SUPPORTED_ARGUMENTS = {
  "YADAMUCMD" : []
, "YADAMUGUI" : ['INIT','IMPORT','UPLOAD','EXPORT','COPY','TEST']
, "YADAMUCLI" : ['IMPORT','UPLOAD','EXPORT','COPY','TEST']
, "IMPORT"    : ['IMPORT']
, "UPLOAD"    : ['UPLOAD']
, "EXPORT"    : ['EXPORT']
, "INIT"      : ['INIT']
, "COPY"      : ['COPY']
, "TEST"      : ['TEST']
}

const REQUIRED_ARGUMENTS = {
  "YADAMUCMD" : []
, "YADAMUGUI" : []
, "YADAMUCLI" : []
, "IMPORT" : ['TO_USER']
, "UPLOAD" : ['TO_USER']
, "EXPORT" : ['FROM_USER']
, "COPY"   : ['CONFIG']
, "TEST"   : ['CONFIG']
}

const ILLIEGAL_ARGUMENTS = {
  "YADAMUCLI" : ['FILE','CONFIG','CONFIGURATION']
, "YADAMUCMD" : ['FILE','CONFIG','CONFIGURATION']
, "IMPORT"    : ['CONFIG','CONFIGURATION',"FROM_USER","OVERWRITE"]
, "UPLOAD"    : ['CONFIG','CONFIGURATION',"FROM_USER","OVERWRITE"]
, "EXPORT"    : ['CONGIG','CONFIGURATION',"TO_USER","FROM_USER"]
, "COPY"      : ['FILE',"FROM_USER","TO_USER"]
, "TEST"      : ['FILE',"FROM_USER","TO_USER"]
}

const ENUMERATED_PARAMETERS = {
  "MODE" : ['DDL_ONLY','DATA_ONLY','DDL_AND_DATA']
}

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
  
  checkFileExists(argument) {
	const relativePath = this.parameters[argument]
	const resolvedPath = path.resolve(relativePath);
	try {
	  if (!fs.statSync(resolvedPath).isFile()) {
	    const err = new CommandLineError(`Found Directory ["${relativePath}"]. The path specified for the ${argument} argument must not resolve to a directory.`)
	    throw err;
	  }
	}
    catch (e) {
	  if (e.code && e.code === 'ENOENT') {
        const err = new CommandLineError(`File not found ["${relativePath}"]. THe path specified for the ${argument} argument must resolve to existing file.`)
	    throw err
	  }
      throw e;
    } 
  }

  checkFileDoesNotExist(argument) {
	 
	const relativePath = this.parameters[argument]
	const resolvedPath = path.resolve(relativePath);
	try {
      if (fs.statSync(resolvedPath)) {
	    const err = new CommandLineError(`File exists ["${relativePath}"]. The path specified for the ${argument} argument must not resolve to an existing file. Specify OVERWRITE=true to allow the file to be overwritten.`)
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
    const requiredArguments = REQUIRED_ARGUMENTS[command];
    const cmdLineParamters = this.yadamu.getCommandLineParameters()
	
	for (const argument of requiredArguments) {
	  if (cmdLineParamters[argument] === undefined) {
	    const err = new CommandLineError(`"${command}" requires that the following arguments ${JSON.stringify(requiredArguments)} be provided on the command line`)
        throw err
	  }
	}
    
	const illegalArguments = ILLIEGAL_ARGUMENTS[command]
	// TODO ### Check for illegal arguments

    if (CONFIGURATION_REQUIRED.includes(command)) {
	  this.checkFileExists('CONFIG') 
    }
	
	if (FILE_MUST_EXIST.includes(command)) {
	  this.checkFileExists('FILE')
	}
	
	if ((FILE_MUST_NOT_EXIST.includes(command)) && (cmdLineParamters.OVERWRITE !== true)) {
      this.checkFileDoesNotExist('FILE')
	}

    for (const parameter of Object.keys(cmdLineParamters)) {
	  if (ENUMERATED_PARAMETERS[parameter] !== undefined) {
	    if (!ENUMERATED_PARAMETERS[parameter].includes(cmdLineParamters[parameter])) {
  	      const err = new CommandLineError(`Valid values for parameter "${parameter}" are ${JSON.stringify(ENUMERATED_PARAMETERS[parameter])}.`)
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
    process.argv.forEach(function (arg) {
 
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
    },this)	
	
	let err
	let command
	
	if (REQUIRES_SWITCH.indexOf(className) > -1) {
  	  switch (commands.length) {
	    case 0:
          err = new CommandLineError(`Must specify one of ${JSON.stringify(VALID_SWITCHES)} on the command line`)
	      throw err;
	      break;
        case 1:
		  command = commands[0]
          break;
	    default:
	      err = new CommandLineError(`Conflicting operations: Please specify just one of ${JSON.stringify(VALID_SWITCHES)} on the command line`)
	      throw err;
	  }
	}
	else {
      switch (commands.length) {
	    case 0:
		  command = className
	      break;
	    default:
		  err = new CommandLineError(`The switches ${JSON.stringify(VALID_SWITCHES)} are not valid with the "${this.constructor.name}" command.`)
	      throw err;
	  }
	}
	
	return command;
  }
	  

  constructor() {  

    const className = this.constructor.name.toUpperCase()
	switch (className) {
	  case 'EXPORT':
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
    this.yadamuLogger = this.yadamu.getYadamuLogger()

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
	if (CONFIGURATION_REQUIRED.indexOf(this.command) > -1) {
      const configuration = this.loadJSON(this.parameters.CONFIG);
      this.expandConfiguration(configuration);
	  return configuration;
	}
  }
  
  getDatabaseInterface(yadamu,driver,connectionProperties,parameters) {

    let dbi = undefined
    
    switch (driver) {
      case "oracle"  : 
        const OracleDBI = require('../oracle/node/oracleDBI.js');
        dbi = new OracleDBI(yadamu)
        break;
      case "postgres" :
        const PostgresDBI = require('../postgres/node/postgresDBI.js');
        dbi = new PostgresDBI(yadamu)
        break;
      case "mssql" :
        const MsSQLDBI = require('../mssql/node/mssqlDBI.js');
        dbi = new MsSQLDBI(yadamu)
        break;
      case "mysql" :
        const MySQLDBI = require('../mysql/node/mysqlDBI.js');
        dbi = new MySQLDBI(yadamu)
        break;
      case "mariadb" :
        const MariaDBI = require('../mariadb/node/mariadbDBI.js');
        dbi = new MariaDBI(yadamu)
        break;
      case "mongodb" :
        const MongoDBI = require('../mongodb/node/mongoDBI.js');
        dbi = new MongoDBI(yadamu)
        break;
      case "snowflake" :
        const SnowflakeDBI = require('../snowflake/node/snowflakeDBI.js');
        dbi = new SnowflakeDBI(yadamu)
        break;
      case "file" :
        dbi = new FileDBI(yadamu)
        break;
      default:
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
           sourceParameters.MSSQL_SCHEMA_DB = sourceSchema.database
           break;
         default:
      }
	  
	  const targetParameters = {
		TO_USER: this.getUser(targetDatabase,targetSchema)
	  }
      switch (targetDatabase) {
         case 'mssql':
           targetParameters.MSSQL_SCHEMA_DB = targetSchema.database
           break;
         default:
      }

      this.yadamu.reloadParameters(jobParameters);
      const sourceDBI = this.getDatabaseInterface(this.yadamu,sourceDatabase,sourceConnection[sourceDatabase],sourceParameters)
      const targetDBI = this.getDatabaseInterface(this.yadamu,targetDatabase,targetConnection[targetDatabase],targetParameters)    
      
      await this.yadamu.doCopy(sourceDBI,targetDBI);      
      this.yadamuLogger.info([`${this.constructor.name}.doCopy()`],`Operation complete. Source:[${sourceDescription}]. Target:[${targetDescription}].`);
    }
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`${this.constructor.name}.doCopy()`],`Operation complete: Configuration:"${this.parameters.CONFIG}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doImport() {
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.getDefaultDatabase(),{},{})
    const startTime = performance.now();
    await this.yadamu.doImport(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`${this.constructor.name}.doImport()`],`Operation complete: File:"${this.parameters.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doUpload() {
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.getDefaultDatabase(),{},{})
    const startTime = performance.now();
    await this.yadamu.doUpload(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`${this.constructor.name}.doUpload()`],`Operation complete: File:"${this.parameters.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doExport() {
	const dbi = this.getDatabaseInterface(this.yadamu,this.yadamu.getDefaultDatabase(),{},{})
    const startTime = performance.now();
    await this.yadamu.doExport(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`${this.constructor.name}.doExport()`],`Operation complete: File:"${this.parameters.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doTests() {
	
	const configuration = this.loadConfigurationFile()
    const YadamuQA = require('../../YADAMU_QA/common/node/yadamuQA.js');
	const yadamuQA = new YadamuQA(configuration,this.yadamuLogger);
	const startTime = performance.now();
	await yadamuQA.doTests(configuration);
	const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`${this.constructor.name}.doTests()`,`TESTS`],`Completed: Configuration:"${this.parameters.CONFIG}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);

  }
  
}

module.exports = YadamuCLI;
"use strict" 

const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');
const assert = require('assert');

const Yadamu = require('./yadamu.js');
const YadamuLibrary = require('./yadamuLibrary.js');
const YadamuLogger = require('./yadamuLogger.js');
const {YadamuError, CommandLineError, ConfigurationFileError} = require('./yadamuError.js');

const FileDBI = require('../file/node/fileDBI.js');

const CHECK_SWITCH = ['YADAMUCLI' , 'YADAMUCMD', 'YADAMUGUI']

const FILE_REQURIED = ['INIT','IMPORT','UPLOAD','COPY','TEST']
const CONFIGURATION_REQUIRED = ['INIT','COPY','TEST']
const SUPPORTED_SWITCHES = ['INIT','IMPORT','UPLOAD','EXPORT','COPY','TEST']

const REQUIRES_CONFIG = ['INIT','COPY','TEST']

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
, "EXPORT" : ['FROM_USER']
, "COPY"   : ['CONFIG']
, "TEST"   : ['CONFIG']
}

const UNSUPPORTED_ARGUMENTS = {
  "YADAMUCLI" : ['FILE','CONFIG','CONFIGURATION']
, "YADAMUCMD" : ['FILE','CONFIG','CONFIGURATION']
, "IMPORT"    : ['CONFIG','CONFIGURATION']
, "UPLOAD"    : ['CONFIG','CONFIGURATION']
, "EXPORT"    : ['CONGIG','CONFIGURATION']
, "COPY"      : ['FILE']
, "TEST"      : ['FILE']
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

  constructor() {  
	this.command = this.constructor.name.toUpperCase()
	 // Load this.parameters with the command line switches
    this.processCommandLineSwitches();
    this.validateSwitch();
  }
  
  setLogger(logger) {
  
    this.yadamuLogger = logger;

  }
  
  getCommand() {
	return this.command
  }
  
  
  validateSwitch() {
	   
	// Check for invalid combinations of command line swtiches. This is depenedant on which subclass of yadamuCLI is instantiaed.
	
	let operation = CHECK_SWITCH.indexOf(this.command) > -1 ? undefined : this.command;
    const validSwitches = SUPPORTED_ARGUMENTS[this.command] 
	const argNames = Object.keys(this.parameters);
	for (const op of validSwitches) {
      if (argNames.includes(op)) {
  	    if (operation === undefined) {
  	      operation = op
		}
		else {
	      const err = new CommandLineError(`Conflicting operations: Please specify just one of ${JSON.stringify(validSwitches)} on the command line`)
	      throw err;
	    }
	  }
    }
  }

  processCommandLineSwitches() {
   
    /*
	**
	** Read Command Line Parameters that specify the operation to be performed.
	**
	*/
   
    this.parameters = {}
 
    process.argv.forEach(function (arg) {
     
      if (arg.indexOf('=') > -1) {
        const parameterName = arg.substring(0,arg.indexOf('='));
        const parameterValue = arg.substring(arg.indexOf('=')+1);
        switch (parameterName.toUpperCase()) {
	      case 'INIT':		  
	      case '--INIT':
  	        this.parameters.INIT = parameterValue;
  	        this.parameters.CONFIG = parameterValue;
		    break;
	      case 'COPY':		  
	      case '--COPY':
  	        this.parameters.COPY = parameterValue;
  	        this.parameters.CONFIG = parameterValue;
		    break;
	      case 'TEST':		  
	      case '--TEST':
  	        this.parameters.TEST = parameterValue;
  	        this.parameters.CONFIG = parameterValue;
		    break;
	      case 'IMPORT':		  
	      case '--IMPORT':
  	        this.parameters.IMPORT = parameterValue;
  	        this.parameters.FILE = parameterValue;
		    break;
	      case 'UPLOAD':		  
	      case '--UPLOAD':
  	        this.parameters.UPLOAD = parameterValue;
  	        this.parameters.FILE = parameterValue;
		    break;
	      case 'EXPORT':		  
	      case '--EXPORT':
  	        this.parameters.EXPORT = parameterValue;
  	        this.parameters.FILE = parameterValue;
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
  }

  getParameters() {
	return this.parameters;
  }


  validateParameters(parameters) {
    const requiredArguments = REQUIRED_ARGUMENTS[this.command];
	for (const argument of requiredArguments) {
	  if (parameters[argument] === undefined) {
	    const err = new CommandLineError(`["${this.command}" requires that the following arguments ${JSON.stringify(requiredArguments)} be provided on the command line`)
        throw err
	  }
	}
	
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
		  let message = `Cannot l JSON file "${path}".`;
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
	if (REQUIRES_CONFIG.indexOf(this.command) > -1) {
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
        this.yadamuLogger.log([`${this.constructor.name}.getDatabaseInterface()`],message);  
		const err = new ConfigurationFileError(`[${this.constructor.name}.getDatabaseInterface()]: ${message}`);
		throw err
      }
	  
	  dbi.setConnectionProperties(connectionProperties);
      dbi.setParameters(parameters);
      return dbi;
  }
  
  getUser(vendor,schema) {
    
     return vendor === 'mssql' ? schema.owner : (vendor === 'snowflake' ? schema.snowflake.schema : schema.schema)
     
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
	  assert.notStrictEqual(targetConnection,undefined,new ConfigurationFileError(`Target Connection "${job.source.connection}" not found. Valid connections: "${Object.keys( configuration.connections)}".`))

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

      const yadamu = new Yadamu(this.command,jobParameters);
      const sourceDBI = this.getDatabaseInterface(yadamu,sourceDatabase,sourceConnection[sourceDatabase],sourceParameters)
      const targetDBI = this.getDatabaseInterface(yadamu,targetDatabase,targetConnection[targetDatabase],targetParameters)    
      
      await yadamu.doCopy(sourceDBI,targetDBI);      
      this.yadamuLogger.info([`${this.constructor.name}.doCopy()`],`Operation complete. Source:[${sourceDescription}]. Target:[${targetDescription}].`);
    }
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`${this.constructor.name}.doCopy()`],`Operation complete: Configuration:"${this.parameters.CONFIG}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doImport() {
	const yadamu = new Yadamu(this.command);
	const dbi = this.getDatabaseInterface(yadamu,yadamu.getDefaultDatabase(),{},{})
    const startTime = performance.now();
    await yadamu.doImport(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`${this.constructor.name}.doImport()`],`Operation complete: File:"${this.parameters.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doUpload() {
	const yadamu = new Yadamu(this.command);
	const dbi = this.getDatabaseInterface(yadamu,yadamu.getDefaultDatabase(),{},{})
    const startTime = performance.now();
    await yadamu.doUpload(dbi);
    const elapsedTime = performance.now() - startTime;
    this.yadamuLogger.info([`${this.constructor.name}.doUpload()`],`Operation complete: File:"${this.parameters.FILE}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
  }
  
  async doExport() {
	const yadamu = new Yadamu(this.command);
	this.validateParameters(yadamu.getCommandLineParameters());
	const dbi = this.getDatabaseInterface(yadamu,yadamu.getDefaultDatabase(),{},{})
    const startTime = performance.now();
    await yadamu.doExport(dbi);
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
    this.yadamuLogger.info([`${this.constructor.name}.doTests()`,`TESTS`],`Complete: Configuration:"${this.parameters.CONFIG}". Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);

  }
  
}

module.exports = YadamuCLI;
	
    /*

    if ((cmdLineOperation === undefined )  && (operation !== 'YADAMUGUI')) {
      const err = new Error(`[${operation}] requires one of the following arguments" ${JSON.stringify(validSwitches)} be specified on the command line`)
	  throw err
	}
	
	
	
	if (operation !== 'DEFAULT') {
      this.targetPath = path.resolve(this.parameters[operation]);
	  if (FILE_REQURIED.includes(operation)) {
        try {
		  if (!fs.statSync(this.targetPath).isFile()) {
	        const err = new Error(`The value provided for the "${operation}" argument must not be a directory`)
		    throw err;
		  }
	    }
        catch (e) {
		  if (e.code && e.code === 'ENOENT') {
            const err = new Error(`The file specified for the "${operation}" argument ("${this.targetPath}") must exist`)
	        throw err
		  }
		  throw e;
	    }
	  }
      else {
        if (this.parameters.OVERWRITE !== 'YES') {
          try {
  		    if (fs.statSync(this.targetPath)) {
	          const err = new Error(`The file specified for the "${operation}" argument ("${this.targetPath}") must not exist`)
		      throw err;
		    }
	      }
          catch (e) {
			if (e.code && e.code === 'ENOENT') {
		    }
			else {
  		      throw e;
			}
	      }
	    }
      }	  
	}
	this.operation = operation;
	*/
  
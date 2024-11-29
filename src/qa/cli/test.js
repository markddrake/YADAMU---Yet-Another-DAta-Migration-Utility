
import path               from 'path'
import fs                 from 'fs';
import fsp                from 'fs/promises';
import assert             from 'assert'
import { 
  fileURLToPath }         from 'url';

import { 
  performance 
}                         from 'perf_hooks';

import { 
  Transform 
}                         from 'stream'

import { 
  pipeline 
}                         from 'stream/promises'

import StringWriter       from '../../node/util/stringWriter.js'
import YadamuLogger       from '../../node/core/yadamuLogger.js';

import YadamuConstants    from '../../node/lib/yadamuConstants.js';
import YadamuLibrary      from '../../node/lib/yadamuLibrary.js';
import YadamuCLI          from '../../node/cli/yadamuCLI.js';
import JSONParser         from '../../node/dbi/file/jsonParser.js';
import LoaderDBI          from '../../node/dbi/loader/loaderDBI.js';
import DBIConstants       from '../../node/dbi/base/dbiConstants.js';

import {
  ConfigurationFileError
}                         from '../..//node/core/yadamuException.js';

import Metrics            from '../core/yadamuMetrics.js';
import Yadamu             from '../core/yadamu.js';

// import wtf from 'wtfnode';

// const YadamuDefaults = require('./yadamuDefaults.json')

const  __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const YadamuDefaults = fs.readFileSync(path.join(__dirname,'../cfg/yadamuDefaults.json'),'utf-8');
  
class YadamuExportParser extends Transform {

  get LOGGER()             { return this._LOGGER }
  set LOGGER(v)            { this._LOGGER = v }
  
  get DEBUGGER()           { return this._DEBUGGER }
  set DEBUGGER(v)          { this._DEBUGGER = v }
  
  constructor(yadamuLogger) {
    super({objectMode: true });
    this.LOGGER = yadamuLogger
  }
  
  async parse(inputFile, targetName) {
    this.targetName = targetName
    this.inputStream = await new Promise((resolve,reject) => {
      const inputStream = fs.createReadStream(inputFile);
      inputStream.on('open',() => {resolve(inputStream)}).on('error',(err) => {reject(err)})
    })
    this.jsonParser =  new JSONParser('DDL_ONLY',inputFile,DBIConstants.PIPELINE_STATE,this.LOGGER);
    try {
      await pipeline([this.inputStream,this.jsonParser,this]);
    } catch (e) {
      if (e.code !== 'ERR_STREAM_PREMATURE_CLOSE') {
        throw e
      }
    }
  }
  
  async doTransform(obj,encdoing) {
	switch (Object.keys(obj)[0]) {
       case this.targetName:
         this.jsonParser.pause()
         this.jsonParser.end();
         this.target = obj[this.targetName]
         this.destroy()
       default:
    }
  }	
      
  _transform(obj, encoding, callback) {
	 
	this.doTransform(obj, encoding).then(() => {
	  callback()
	}).catch((e) => {
     this.LOGGER.logException([`${this.constructor.name}._transform()`],e);
     callback(e);
	})
  }
  
  getTarget() {
    return this.target
  }
}

class Test extends YadamuCLI {

  get VERIFY_OPERATION()                          { return this.test.hasOwnProperty('verifyOperation') ? this.test.verifyOperation : this.TEST_CONFIGURATION.hasOwnProperty('verifyOperation') ? this.TEST_CONFIGURATION.verifyOperation : false }
  get RECREATE_SCHEMA()                           { return this.test.hasOwnProperty('recreateSchema') ? this.test.recreateSchema : this.TEST_CONFIGURATION.hasOwnProperty('recreateSchema') ? this.TEST_CONFIGURATION.recreateSchema : false }
  get TARGET_SCHEMA_SUFFIX()                      { return this.test.hasOwnProperty('targetSchemaSuffix') ? this.test.targetSchemaSuffix : this.TEST_CONFIGURATION.hasOwnProperty('targetSchemaSuffix') ? this.TEST_CONFIGURATION.targetSchemaSuffix : "1" }
  get COMPARE_SCHEMA_SUFFIX()                     { return this.test.hasOwnProperty('compareSchemaSuffix') ? this.test.compareSchemaSuffix : this.TEST_CONFIGURATION.hasOwnProperty('compareSchemaSuffix') ? this.TEST_CONFIGURATION.compareSchemaSuffix : "1" }
  get TEST_LOST_CONNECTION()                      { return this.test.hasOwnProperty('kill') || this.TEST_CONFIGURATION.hasOwnProperty('kill') }
  get TERMINATION_CONFIGURATION()                 { return this.test.hasOwnProperty('kill') ? this.test.kill : this.TEST_CONFIGURATION.hasOwnProperty('kill') ? this.TEST_CONFIGURATION.kill : {} }
  get STAGING_AREA()                              { return this.test.hasOwnProperty('stagingArea') ? this.test.stagingArea : this.TEST_CONFIGURATION.hasOwnProperty('stagingArea') ? this.TEST_CONFIGURATION.stagingArea : undefined }
  get RELOAD_STAGING_AREA()                       { return this.test.hasOwnProperty('reloadStagingArea') ? this.test.reloadStagingArea : this.TEST_CONFIGURATION.hasOwnProperty('reloadStagingArea') ? this.TEST_CONFIGURATION.reloadStagingArea : false }
  get EMPTY_STRING_IS_NULL()                      { return this.test.hasOwnProperty('emptyStringIsNull') ? this.test.emptyStringIsNull : this.TEST_CONFIGURATION.hasOwnProperty('emptyStringIsNull') ? this.TEST_CONFIGURATION.emptyStringIsNull : undefined }
  get MIN_BIGINT_IS_NULL()                        { return this.test.hasOwnProperty('minBigEntIsNull') ? this.test.minBigEntIsNull : this.TEST_CONFIGURATION.hasOwnProperty('minBigEntIsNull') ? this.TEST_CONFIGURATION.minBigEntIsNull : undefined }
  get SKIP_DATA_STAGING()                         { return this.test.hasOwnProperty('skipDataStaging') ? this.test.skipDataStaging : this.TEST_CONFIGURATION.hasOwnProperty('skipDataStaging') ? this.TEST_CONFIGURATION.skipDataStaging : false }
  get FORCE_HOMOGENEOUS_OPERATION()               { return this.test.hasOwnProperty('homogeneousOperation') ? this.test.homogeneousOperation : this.TEST_CONFIGURATION.hasOwnProperty('homogeneousOperation') ? this.TEST_CONFIGURATION.homogeneousOperation : false }
  get EXPORT_PATH()                               { return this.test.exportPath || this.TEST_CONFIGURATION.exportPath || '' }
  get IMPORT_PATH()                               { return this.test.importPath || this.TEST_CONFIGURATION.importPath || '' }
  get OPERATION()                                 { return this.test.operation  || this.TEST_CONFIGURATION.operation }
  get OPERATION_NAME()                            { return this.OPERATION.toUpperCase() }

  get LOGGER()             { return this.yadamu.LOGGER }

  
  get DEBUGGER()           { return this._DEBUGGER }
  set DEBUGGER(v)          { this._DEBUGGER = v }

  #IDENTIFIER_MAPPINGS                = {}
  get IDENTIFIER_MAPPINGS()           { return this.#IDENTIFIER_MAPPINGS }
  set IDENTIFIER_MAPPINGS(v)          { this.#IDENTIFIER_MAPPINGS = v }
  
  #COMPARE_MAPPINGS                   = {}
  get COMPARE_MAPPINGS(   )           { return this.#COMPARE_MAPPINGS }
  set COMPARE_MAPPINGS(v)             { this.#COMPARE_MAPPINGS = v }
  
  #TABLE_FILTER                       = []
  get TABLE_FILTER()                  { return this.#TABLE_FILTER }
  set TABLE_FILTER(v)                 { this.#TABLE_FILTER = v }
  
  constructor() {
	super()
    this.DATABASE_DRIVERS = Yadamu.QA_DRIVER_MAPPINGS
    this.TEST_CONFIGURATION = this.loadConfigurationFile()
    this.expandedTaskList = []
    this.failedOperations = {}
  }
  
  async initialize() {
    await this.yadamu.initialize()
  }
  
  async getDatabaseInterface(yadamu,driver,connectionSettings,configurationParameters) {
    const dbi = await super.getDatabaseInterface(yadamu,driver,connectionSettings,configurationParameters)
	this.yadamu.TERMINATION_CONFIGURATION = this.TEST_LOST_CONNECTION  ? this.TERMINATION_CONFIGURATION : undefined
	if (this.TABLE_FILTER.length > 0) {
      dbi.parameters.TABLES = this.TABLE_FILTER
	}
    return dbi;
  }

  async getSourceConnection(yadamu,job) {
    const dbi = await super.getSourceConnection(yadamu,job) 
	dbi.setOption('recreateSchema',false);
    return dbi
  }
		
  async getTargetConnection(yadamu,job) {
	const dbi = await super.getTargetConnection(yadamu,job)
	dbi.setOption('recreateSchema',job.recreateSchema)
	if (job.reverseIdentifierMappings) {
	  this.yadamu.IDENTIFIER_MAPPINGS = this.reverseIdentifierMappings(this.IDENTIFIER_MAPPINGS)
	}
    return dbi
  }
		
  async getCompareConnection(yadamu,driver,connectionSettings,configurationParameters) {
    const dbi = await super.getDatabaseInterface(yadamu,driver,connectionSettings,configurationParameters)
    return dbi;
  }

  getConnection(connectionList, connectionName) {
   
    const connection = connectionList[connectionName]
    if (connection === undefined) {4
      throw new ConfigurationFileError(`Named connection "${connectionName}" not found. Valid connections: "${Object.keys(connectionList)}".`);
    }
    return connection;
    
  }

  getPrefixedSchema(prefix,schema) {
    return prefix ? `${prefix}_${schema}` : schema
  }
 
  getSourceMapping(vendor,operation) {
      
    let schema
    let database
    const schemaInfo = (typeof operation.source === 'string') ? { schema : operation.source } : {...operation.source}; 
    switch (operation.vendor) {
      case 'mssql': 
        // MsSQL style schema information
        switch (vendor) {
          case 'mssql':
            return schemaInfo;
            break;
          case 'snowflake':
            return {database: schemaInfo.database, schema: schemaInfo.owner};
            break;
          case 'mongo':
            let database = schemaInfo.owner === 'dbo' ? schemaInfo.database : schemaInfo.owner
            database = this.getPrefixedSchema(operation.schemaPrefix,database) 
            return { "database" : database }
            break;
          default:
            let schema = schemaInfo.owner === 'dbo' ? schemaInfo.database : schemaInfo.owner
            schema = this.getPrefixedSchema(operation.schemaPrefix,schema) 
            return { "schema" : schema }
        }
        break;
      case 'snowflake':
        // Snowflake style schema informaton
        return schemaInfo;
        break;
      case 'mongodb':
        // Mongo 
        switch (vendor) {
          case 'mssql':
            database = this.getPrefixedSchema(operation.schemaPrefix,schemaInfo.database) 
            return {"database": database, "owner" : "dbo"}
            break;
          case 'snowflake':
            // ### TODO : Snowflake schema mappings
            return schemaInfo;
            break;
          case 'mongo':
            return schemaInfo;
            break;
          default:
            return { "schema" : schemaInfo.database }
        }
        break;
      default:
        // Oracle, Mysql, MariaDB, Postgress 
        switch (vendor) {
          case 'mssql':
            database = this.getPrefixedSchema(operation.schemaPrefix,schemaInfo.schema) 
            return {"database": database, "owner" : "dbo"}
            break;
          case 'snowflake':
            return {database : operation.vendor.toUpperCase(), schema : schemaInfo.schema}
            break;
          case 'mongo':
            return {"database" : schemaInfo.schema};
            break;
          default:
            return schemaInfo
        }
    }
  }
 
  getTargetMapping(vendor,operation,modifier = '') {
    let schema
    let database
    let schemaInfo = this.getSourceMapping(vendor,operation)

    switch (vendor) {
      case 'mssql': 
        return schemaInfo.owner === 'dbo' ? {database: `${schemaInfo.database}${modifier}`,  owner:schemaInfo.owner}  : { database: `${this.getPrefixedSchema(operation.schemaPrefix, schemaInfo.owner)}${modifier}`, owner: 'dbo'}
      case 'snowflake':
        return schemaInfo.schema === 'dbo' ? {database: `${this.getPrefixedSchema(operation.schemaPrefix, schemaInfo.database)}${modifier}`, schema:schemaInfo.schema}  : {database: schemaInfo.database, schema: `${schemaInfo.schema}${modifier}`}    
      case 'mongo':
        return {"database" : `${schemaInfo.database}${modifier}`};
     default:
       return {schema: `${schemaInfo.schema}${modifier}`}
    } 
  }
  
  printLogRecords(logRecords) {
	for (let logRecord of logRecords) {
	  this.LOGGER.log(logRecord)
    }
  }  
  
  printResults(operation,sourceDescription,targetDescription,elapsedTime) {
    const stepMetrics = this.LOGGER.getMetrics(true)
	this.metrics.adjust(stepMetrics)
	
	if (this.metrics.error) {
	  this.LOGGER.handleException([this.OPERATION_NAME],this.metrics.error)
	  stepMetrics.errors++
	}
    
    if (this.LOGGER.FILE_BASED_LOGWRITER) {
      
      const colSizes = [24,128,14]
      let seperatorSize = (colSizes.length * 3) - 1;
      colSizes.forEach((size)  => {
        seperatorSize += size;
      });
    
      this.LOGGER.writeDirect('\n+' + '-'.repeat(seperatorSize) + '+' + '\n') 
     
      this.LOGGER.writeDirect(`| ${'TIMESTAMP'.padEnd(colSizes[0])} |`
                                   + ` ${'OPERATION'.padEnd(colSizes[1])} |`
                                   + ` ${'ELASPED TIME'.padStart(colSizes[2])} |` 
                                   + '\n');
      this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      
      this.LOGGER.writeDirect(`| ${new Date().toISOString().padEnd(colSizes[0])} |`
                                   + ` ${(sourceDescription + ' --> ' + targetDescription).padEnd(colSizes[1])} |`
                                   + ` ${YadamuLibrary.stringifyDuration(elapsedTime).padStart(colSizes[2])} |` 
                                   + '\n');
                 
      this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
    else {
	  this.LOGGER.qa([operation,`COPY`,sourceDescription,targetDescription],`${this.metrics.formatMetrics(stepMetrics)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
    }

    this.metrics.aggregateSubTask(stepMetrics);
    
  }
  
  async compare(sourceConnectionName,targetConnectionName,testId,comparator) {
	      
	try {
	  const compareResults = await comparator.compare()
	  if (compareResults.failed.length > 0) {
	    this.metrics.recordFailed(compareResults.failed.length)
        this.failedOperations[sourceConnectionName] = {...this.failedOperations[sourceConnectionName]}
        this.failedOperations[sourceConnectionName][targetConnectionName] = {...this.failedOperations[sourceConnectionName][targetConnectionName]}
        compareResults.failed.forEach((failed,idx) => {
          this.failedOperations[sourceConnectionName][targetConnectionName][testId] = {...this.failedOperations[sourceConnectionName][targetConnectionName][testId]}
          this.failedOperations[sourceConnectionName][targetConnectionName][testId][failed[2]] = compareResults.failed[idx]
        })
        // this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
      }
    	  
      if (YadamuLibrary.isEmpty(compareResults)) {
        this.LOGGER.qa(['COMPARE',sourceConnectionName,targetConnectionName,testId],'Compare Operation failed')
        return {};
      }
	  return compareResults
	} catch (e) {
	  // Failed all tables ???
      this.metrics.recordFailed(0)
      this.LOGGER.handleException([this.OPERATION_NAME,`COMPARE`,sourceConnecionName,targetConnectionName],e)
      return {}
    } 
  }
 
  async compareFiles(sourceConnectionName,targetConnectionName,testId,comparator) {

	try {
      const compareResults = await comparator.compareFiles()
	
      if (compareResults.length > 0) {
        this.failedOperations[sourceConnectionName] = {...this.failedOperations[sourceConnectionName]}
        this.failedOperations[sourceConnectionName][targetConnectionName] = {...this.failedOperations[sourceConnectionName][targetConnectionName]}
        compareResults.forEach((failed,idx) => {
          this.failedOperations[sourceConnectionName][targetConnectionName][testId] = {...this.failedOperations[sourceConnectionName][targetConnectionName][testId]}
          this.failedOperations[sourceConnectionName][targetConnectionName][testId][failed[2]] = failed
        })
	  }
	  return compareResults
	} catch (e) {
      this.LOGGER.handleException([`FILE-COMPARE`],e)
      return {}
    } 
   
  }

  dbRoundtripResults(operationsList,elapsedTime) {
          
    if (this.LOGGER.FILE_BASED_LOGWRITER) {
      
      const colSizes = [24,128,14]
      let seperatorSize = (colSizes.length * 3) - 1;
      colSizes.forEach((size)  => {
        seperatorSize += size;
      });
    
      this.LOGGER.writeDirect('\n+' + '-'.repeat(seperatorSize) + '+' + '\n') 
     
      this.LOGGER.writeDirect(`| ${'TIMESTAMP'.padEnd(colSizes[0])} |`
                                   + ` ${'OPERATION'.padEnd(colSizes[1])} |`
                                   + ` ${'ELASPED TIME'.padStart(colSizes[2])} |`     
                                   + '\n');
      this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      
      this.LOGGER.writeDirect(`| ${new Date().toISOString().padEnd(colSizes[0])} |`
                                   + ` ${(operationsList[0] + ' --> ' + operationsList[operationsList.length-1]).padEnd(colSizes[1])} |`
                                   + ` ${(YadamuLibrary.stringifyDuration(elapsedTime)+"ms").padStart(colSizes[2])} |` 
                                   + '\n');
                 
      this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
    else {
      this.LOGGER.qa([`DBROUNDTRIP`,`STEP`].concat(operationsList),`${this.metrics.formatMetrics(this.metrics.task)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
    }
     
    this.reportTimings(this.metrics.timings)
        
  }
  
  setPsuedoKeyTransformation(sourceDatabase,targetDatabase,parameters) {
     
	if (sourceDatabase === 'mongodb') {
       parameters.MONGO_STRIP_ID = parameters.MONGO_STRIP_ID || false
    }
    
    if (targetDatabase === 'mongodb') {
      parameters.MONGO_STRIP_ID = (parameters.MONGO_STRIP_ID === false) ? false : true
    }
      
    if (sourceDatabase === 'cockroach') {
       parameters.COCKROACH_STRIP_ROWID = parameters.COCKROACH_STRIP_ROWID || false
    }
    
	if (targetDatabase === 'cockroach') {
      parameters.COCKROACH_STRIP_ROWID = (parameters.COCKROACH_STRIP_ROWID === false) ? false : true
    }
  
  }
  
  /*
  **
  ** Compare the Conttrol File with the System Information section from the SourceDBI to check that the data set is usable
  **
  */
  
  async dataIsPreStaged(sourceDatabase,sourceConnection,sourceSchema,stagingDatabase,stagingConnection,parameters) {
	 
	 let sourceDBI
     let stagingDBI	  
	  
     try {
  	   const sourceDBI = await this.getDatabaseInterface(this.yadamu,sourceDatabase,sourceConnection,parameters)
       sourceDBI.setSchema(sourceSchema,'FROM_USER')
       await sourceDBI.initialize()
       const sourceInstance = await sourceDBI.getYadamuInstanceInfo() 
       await sourceDBI.final()
	   
	   const stagingDBI = await this.getDatabaseInterface(this.yadamu,stagingDatabase,stagingConnection,parameters)
	   stagingDBI.setSchema(sourceSchema,'FROM_USER')
       await stagingDBI.initialize()
	   try {
         await stagingDBI.loadControlFile()
	   } catch (e) {
		 // Unable to load control file - Probably does not exist but regardless pre-staged data is not not available
		 return false
	   }
       const stagingInstance = await stagingDBI.getYadamuInstanceInfo() 
       await stagingDBI.final()
       
	   if (stagingDBI.controlFile.settings.contentType === 'CSV') {
         if ((sourceInstance.yadamuInstanceID === stagingInstance.yadamuInstanceID) && (sourceInstance.yadamuInstallationTimestamp === stagingInstance.yadamuInstallationTimestamp)) {
           this.LOGGER.qa([sourceDBI.DATABASE_VENDOR,stagingDBI.DATABASE_VENDOR,'COPY'],`Using existing Data Set "${stagingDBI.CONTROL_FILE_PATH}" with ID "${stagingInstance.yadamuInstanceID}".`);
           return true;
         } 
         else {
           this.LOGGER.qa([sourceDBI.DATABASE_VENDOR,stagingDBI.DATABASE_VENDOR,'COPY'],`Cannot use existing Data Set "${stagingDBI.CONTROL_FILE_PATH}". Exepected ID "${sourceInstance.yadamuInstanceID}", found ID "${stagingDBI.yadamuInstanceID}".`);
         }
       }
       else {
         this.LOGGER.qa([sourceDBI.DATABASE_VENDOR,stagingDBI.DATABASE_VENDOR,'COPY'],`Cannot use existing Data Set "${stagingDBI.CONTROL_FILE_PATH}". Exepected format "CSV", found format "${stagingDBI.controlFile.settings.contentType}".`);
       }
	 } catch (e) {
	   this.LOGGER.handleException([sourceDBI?.DATABASE_VENDOR,stagingDBI?.DATABASE_VENDOR,'COPY'],e)
	   try {
		 sourceDBI && await sourceDBI.final()
		 stagingDBI && await stagingDBI.final()
	   } catch (e) { /* If anything goes wrong the staged data is not valid  */ console.log(e) } 
	 }
	 return false     
  }


  async dbRoundtrip(task,configuration,test,targetConnectionName,parameters) {
      
    /*
    **

    The first step is to clone the source schena using a DDL_ONLY operaton. 
    
    There are two modes of operation
    
    DIRECT: Data is copied directly from the source database to the target database and then
            back to the source database.
            
    STAGED: Data is copied from source database to a staging area. 
            The staging area is a file system that the target database has direct access to.
            The target database loads the staged data directly from the staging area.
            The data is the target database is then copied back to the source database.
            
    After the copy operations are completed SQL is used to compare the ontents of the clone with the contents of the original schmea
    to check that there has been no loss of fidelity as a result of the copy operations(s).
      
    Homogeneous operations (where the source and target database are identifcal) are optimized as follows:
    
    DIRECT: The first step is performed in DDL_AND_DATA mode. A single operation is required to clone the schema and copy the data.
    
    STAGED: The database loads the data directly from the staging area. 
    
    To enable STAGED copy mode use the setting "stagingArea" to specify the connection to be used for data staging 
    To temporarily disable data staging and force a conventional copy with a 'STAGED' configuration file use the setting "skipDataStaging: true"

    +-------------------+-----------------------------------+--------------------------------+
    |                   |                                   |                                |
    |                   |              Direct               |           Staged               |
    |                   |                                   |                                |
    +-------------------+-----------------------------------+--------------------------------+
    |                   |                                   | Source --[DDL_ONLY]--> Compare |
    | Homogeneous       | Source --[DDL_AND_DATA]-> Compare | Source --[MODE]------> Stage   |
    |                   |                                   | Stage  --[DATA_ONLY]-> Compare |
    +-------------------+-----------------------------------+--------------------------------+
    |                   |                                   | Source --[DDL_ONLY]--> Compare |
    | Hetrogeneous      | Source --[DDL_ONLY]--> Compare    | Source --[MODE]------> Stage   |   
    |                   | Source --[DATA_ONLY]-> Target     | Stage  --[DATA_ONLY]-> Target  |
    |                   | Target --[DATA_ONLY]-> Compare    | Target --[DATA_ONLY]-> Compare |
    +-------------------+-----------------------------------+--------------------------------+

    If the source and target connections are different then aditional copy operatons are performed. 
    The first operation copies the data from the source schema in the source database to a intermeidate schema in the target database.
    The second operaiton copies the database from intermediate schema in the target database to the clone schema in the source database.
    
    If a staging schema is specified then three operations are required
    The first operation stages the data from the source database in the staging area.
    The second operation uses a database COPY to load the database from the statging area directly into the intermediate schema in the target database.
    The third operaiton then copies the database from intermediate schema in the target database directly into the clone schema in the source database.
     
    When all copy operations are complete the contents of the source schema are compared with the contents of the cloned schema.
    
    Each database interface must be used for a single operation. It is not valid to use the same DBI for more than operation due to state that may be initialized during instance creation.
    
    **
    */        

    // this.LOGGER.trace([this.OPERATION],'Started')	
 	
	const jobs = {}
	const batch = []
	                 
    let sourceConnectionName = test.source
    
    const stagedCopy = (this.STAGING_AREA !== undefined) && !this.SKIP_DATA_STAGING
    const homogeneousCopy = sourceConnectionName === targetConnectionName || this.FORCE_HOMOGENEOUS_OPERATION
    const taskMode = parameters.MODE || this.yadamu.MODE 

    let results
    let stepMetrics
    
    let identifierMappings = {}
    let outboundParameters
    
    const sourceConnection = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnection = this.getConnection(configuration.connections,targetConnectionName)
    
    const sourceDatabase =  YadamuLibrary.getVendorName(sourceConnection);
    const targetDatabase =  YadamuLibrary.getVendorName(targetConnection);

	this.setPsuedoKeyTransformation(sourceDatabase,targetDatabase,parameters);
	
	const connections = {
	  [sourceConnectionName] : sourceConnection
	, [targetConnectionName] : targetConnection
	}
	  
	const schemas = {
	  source  : this.getSourceMapping(sourceDatabase,task)
    , target  : this.getTargetMapping(targetDatabase,task,this.TARGET_SCHEMA_SUFFIX)
	, compare : this.getTargetMapping(sourceDatabase,task,this.TARGET_SCHEMA_SUFFIX)
	}
		
	let jobName = "CLONE"
	let job = {
	  source : {
	    connection : sourceConnectionName
	  , schema     : "source"
	  }
	  , target : {
   	    connection : sourceConnectionName
	  , schema     : "compare"
	  }
	  , parameters : { 
        ...parameters
	    , MODE : (taskMode === 'DDL_ONLY') 
		       ? 'DDL_ONLY' 
			   : homogeneousCopy && !stagedCopy 
			   ? 'DDL_AND_DATA' 
			   : 'DDL_ONLY' 
	  }
	  , abortOnError : true
	  , recreateSchema : this.RECREATE_SCHEMA
	}
	jobs[jobName] = job
	batch.push(jobName)
	
	    
    this.yadamu.MACROS = {
	  ...this.yadamu.MACROS
    , connection           : targetConnectionName
    , location             : this.TEST_CONFIGURATION.tasks.datasetLocation[task.vendor]
    , mode                 : taskMode
    , operation            : this.OPERATION
    , task                 : task.taskName 
    , vendor               : targetDatabase
    , sourceConnection     : sourceConnectionName 
    , targetConnection     : targetConnectionName
    // , sourceUser        : sourceParameters.FROM_USER
    // , targetUser        : targetParameters.TO_USER
    }
	
	if (job.parameters.MODE !== 'DDL_AND_DATA') {
      /*
      **
      ** CLONE job will create a clone of the source schema in the source database using DDL_ONLY mode operation
      **
      */
  	  if (stagedCopy)  {
	    
        const stagingConnectionName     = this.STAGING_AREA
        const stagingConnection         = this.getConnection(configuration.connections,stagingConnectionName)
        const stagingDatabase           = YadamuLibrary.getVendorName(stagingConnection);
		// Use getTargetMapping to get a qualified schema name eg WWI_
        const stagingSchema             = this.getTargetMapping(stagingDatabase,task)
		schemas.stage = stagingSchema
         
        connections[stagingConnectionName] = stagingConnection		 
		
        const stagingParameters         = {
		  ...parameters
        , OUTPUT_FORMAT       : 'CSV'
		}
		
		const dataStaged  = await this.dataIsPreStaged(sourceDatabase,sourceConnection,schemas.source,stagingDatabase,stagingConnection,parameters)
		  
        if (!dataStaged) {
          /*
          **
          ** Copy Source to Stage
          **
          */
          jobName = "SOURCE STAGE"
          job = {
            source : {
              connection : sourceConnectionName
            , schema     : "source"
            }
          , target : {
             connection : stagingConnectionName
             , schema     : "stage"
            }
          , parameters : { 
             ...parameters
            , MODE           : 'DATA_ONLY'
			, OUTPUT_FORMAT  :  'CSV'
          }
          , abortOnError   : true
          , recreateSchema : this.RECREATE_SCHEMA
          }
          jobs[jobName] = job
          batch.push(jobName)	     
        }
		
        /*
        **
        ** Load Target from Stage
        **
        */

        jobName = "STAGE TARGET"
        job = {
           source : {	   
            connection : stagingConnectionName
          , schema     : "stage"
          }
        , target : {
   	        connection : targetConnectionName
          , schema     : "target"
          }
        , parameters : { 
           ...parameters
          , MODE: 'DATA_ONLY' 
          }
        , abortOnError   : true
        , recreateSchema : this.RECREATE_SCHEMA
        }
        jobs[jobName] = job
        batch.push(jobName)
	  }
	  else {
        /*
        **
        ** Copy Source to Target
        **
        */
        jobName = "SOURCE TARGET"
        job = {
          source : {
            connection : sourceConnectionName
          , schema     : "source"
          }
        , target : {
           connection : targetConnectionName
           , schema     : "target"
          }
        , parameters : { 
           ...parameters
          , MODE : 'DATA_ONLY'
        }
        , abortOnError   : true
        , recreateSchema : this.RECREATE_SCHEMA
        }
        jobs[jobName] = job
        batch.push(jobName)
  	  }
	  /*
      **
      ** Copy Target to Source
      **
      */
	  if (!homogeneousCopy) {
        jobName = "TARGET COMPARE"
        job = {
          source : {
            connection : targetConnectionName
          , schema     : "target"
          }
          ,  target : {
             connection : sourceConnectionName
           , schema     : "compare"
          }
          , parameters : { 
             ...parameters
            , MODE : 'DATA_ONLY'
          }
          , abortOnError   : true
          , recreateSchema : false
		  , reverseIdentifierMappings : true
        }
        jobs[jobName] = job        
        batch.push(jobName)
	  }
	}

	this.CONFIGURATION = {
	  connections      : connections
	, schemas          : schemas
	, jobs             : jobs
	, batchOperations  : {
		"steps"        : batch
	  }
	}
	
    // console.dir(this.CONFIGURATION,{depth:null})
	
	let testResults
	try {
      testResults = await this.runNamedBatch("steps",false)
	} catch (e) {
	  this.LOGGER.handleException([this.OPERATION_NAME,sourceConnectionName,targetConnectionName,'EXECUTE'],e)
      this.metrics.recordError(this.LOGGER.getMetrics(true))
      return;
    }

    // console.dir(testResults,{depth:null})
	
	if (testResults.hasOwnProperty('e')) {
	  this.LOGGER.handleException([this.OPERATION_NAME,sourceConnectionName,targetConnectionName,'RESULTS'],testResults.e)
      this.metrics.recordError(this.LOGGER.getMetrics(true))
      return;
	}

    const operationsList = [testResults[batch[0]].source.description]
	for (let job of batch) {
	  const jobSummary = testResults[job]
	  operationsList.push(jobSummary.target.description)	 
      this.metrics.recordTaskTimings([task.taskName,job,jobSummary.target.mode,jobSummary.source.connection,jobSummary.target.connection,jobSummary.elapsedTime])
	}

    if (batch.length > 1) {       
      this.metrics.recordTaskTimings([task.taskName,'TASK','',sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(testResults.endTime - testResults.startTime)])
	}

    const compareParameters = {...parameters}
    delete compareParameters.IDENTIFIER_MAPPING_FILE	
	const compareDBI = await this.getSourceConnection(this.yadamu,jobs[batch[0]])

	const compareConfiguration = {
	  source                   : {
		vendor                 : sourceDatabase
	  , version                : testResults[batch[0]].source.version
	  , schema                 : compareDBI.getSchema(schemas.source)
	  }
	, target                   : {
		vendor                 : targetDatabase
	  ,	version                : testResults[batch[batch.length-1]].source.version
	  ,	schema                 : compareDBI.getSchema(this.getTargetMapping(sourceDatabase,task,this.COMPARE_SCHEMA_SUFFIX))
	  }
    , parameters               : compareParameters
	, metrics                  : testResults[batch[batch.length-1]].metrics
	, includeRowCounts         : false
	, identifierMappings       : identifierMappings
	, copyFromCSV              : stagedCopy
	, ddlApplied               : testResults[batch[0]].target.ddlValid
	, includeMaterializedViews : testResults[batch[batch.length-1]].target.hasMaterializedViews
    }
	
	const comparator = await compareDBI.getComparator(compareConfiguration);
	
	let compareResults
    const startTime = performance.now()    
	try {
	  compareResults = await this.compare(sourceConnectionName,targetConnectionName,task.taskName,comparator)
	} catch (e) {
	  await compareDBI.final();
      this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,'',YadamuLibrary.stringifyDuration(performance.now() = startTime)])
      this.metrics.recordError(this.LOGGER.getMetrics(true))
      return
    }

    await compareDBI.final();
    const endTime =  performance.now() 
	
    this.metrics.recordTaskTimings([task.taskName,'COMPARE','',test.source,targetConnectionName,YadamuLibrary.stringifyDuration(endTime - startTime)])
	
    const elapsedTime =  performance.now() - testResults.startTime
    this.metrics.recordTaskTimings([task.taskName,'TOTAL','',test.source,targetConnectionName,YadamuLibrary.stringifyDuration(elapsedTime)])
	this.dbRoundtripResults(operationsList,elapsedTime)	
	return	
	
  }
  
  supportsDDLGeneration(instance) {
    do { 
      if (Object.getOwnPropertyDescriptor(instance,'getDDLOperations')  !== null) return instance.constructor.name;
    } while ((instance = Object.getPrototypeOf(instance)))
  }	  
  
  async fileRoundtrip(task,configuration,test,targetConnectionName,parameters) {
	 
	const jobs = {}
	const batch = []
	                 
    const sourceConnectionName = test.source
    
	const sourceConnection = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnection = this.getConnection(configuration.connections,targetConnectionName)
    
    const sourceDatabase =  YadamuLibrary.getVendorName(sourceConnection);
    const targetDatabase =  YadamuLibrary.getVendorName(targetConnection);

	const connections = {
	  [sourceConnectionName] : sourceConnection
	, [targetConnectionName] : targetConnection
	}
	  
	const schemas = {
	  source : this.getSourceMapping(sourceDatabase,task)
    , target1 : this.getTargetMapping(targetDatabase,task,'1')
    , target2 : this.getTargetMapping(targetDatabase,task,'2')
	}

    this.yadamu.MACROS = {
	  ...this.yadamu.MACROS
    , connection           : sourceConnectionName
    , location             : this.TEST_CONFIGURATION.tasks.datasetLocation[task.vendor]
    , mode                 : this.yadamu.MODE
    , operation            : this.OPERATION
    , task                 : task.taskName 
    , vendor               : sourceDatabase
    , sourceConnection     : sourceConnectionName 
    , targetConnection     : targetConnectionName
    , importPath           : this.IMPORT_PATH
    , exportPath           : this.EXPORT_PATH
    }

    let filename = task.hasOwnProperty('schemaPrefix') ? `${task.schemaPrefix}_${task.file}` : task.file
    const sourceDirectory = this.test.parameters?.SOURCE_DIRECTORY || this.TEST_CONFIGURATION.parameters?.SOURCE_DIRECTORY || this.test.parameters?.DIRECTORY || this.TEST_CONFIGURATION.parameters?.DIRECTORY
    const targetDirectory = this.test.parameters?.TARGET_DIRECTORY || this.TEST_CONFIGURATION.parameters?.TARGET_DIRECTORY || this.test.parameters?.DIRECTORY || this.TEST_CONFIGURATION.parameters?.DIRECTORY
    const importDirectory = YadamuLibrary.macroSubstitions(path.join(sourceDirectory,this.EXPORT_PATH),this.yadamu.MACROS)  
    const exportDirectory = YadamuLibrary.macroSubstitions(path.join(targetDirectory,this.IMPORT_PATH),this.yadamu.MACROS)  
    
    const sourcePathComponents = path.parse(filename);
    let filename1 = sourcePathComponents.name + ".1" + sourcePathComponents.ext
    const filename2 = sourcePathComponents.name + ".2" + sourcePathComponents.ext

    const sourceSchema = this.getSourceMapping(targetDatabase,task)
    const targetSchema1  = this.getTargetMapping(targetDatabase,task,'1')
    const targetSchema2  = this.getTargetMapping(targetDatabase,task,'2');

    // For an Upload Operation construct a FileDBI to resolve the full path to the file.

	
	if (test.parser === 'SQL') {
      const fileDBI =	await this.getDatabaseInterface(this.yadamu,sourceDatabase,sourceConnection,parameters)
	  fileDBI.parameters.FILE = filename
	  fileDBI.DIRECTORY = parameters.SOURCE_DIRECTORY
	  filename = fileDBI.FILE 
	  fileDBI.final()
	}
 
    // Source File to Target Schema #1

    let jobName = "IMPORT#1"
	let job = {
	  source             : {
	    connection       : sourceConnectionName
	  }
	, target             : {
        connection       : targetConnectionName
	  , schema           : "target1"
      }
    , parameters : { 
        ...parameters
      , FILE             : filename
      , SOURCE_DIRECTORY : importDirectory
      }
    , abortOnError       : true
    , recreateSchema     : this.RECREATE_SCHEMA
	, parser             : test.parser 
    }
    jobs[jobName] = job
    batch.push(jobName)


    // Target Schema #1 to File #1

	jobName = "EXPORT#1"
	job = {
	  source             : {
        connection       : targetConnectionName
	  , schema           : "target1"
	  }
	, target             : {
	    connection       : sourceConnectionName
	  }
    , parameters         : { 
        ...parameters
      , FILE             : filename1
      , TARGET_DIRECTORY : exportDirectory               
      }
    , abortOnError       : true
    , recreateSchema     : this.RECREATE_SCHEMA
    }
    jobs[jobName] = job
    batch.push(jobName)

    // File#1 to Target Schema #2

	if (test.parser === 'SQL') {
      const fileDBI =	await this.getDatabaseInterface(this.yadamu,sourceDatabase,sourceConnection,parameters)
	  fileDBI.parameters.FILE = filename1
	  fileDBI.DIRECTORY = parameters.TARGET_DIRECTORY
	  filename1 = fileDBI.FILE 
	  fileDBI.final()
	}

    jobName = "IMPORT#2"
	job = {
	  source             : {
	    connection       : sourceConnectionName
	  }
	, target             : {
        connection       : targetConnectionName
	  , schema           : "target2"
      }
    , parameters         : { 
        ...parameters
      , FILE             : filename1
      , SOURCE_DIRECTORY : exportDirectory
      }
    , abortOnError       : true
    , recreateSchema     : this.RECREATE_SCHEMA
    , parser             : test.parser 
    }
    jobs[jobName] = job
    batch.push(jobName)

    // Target Schema #2 to File#2

	jobName = "EXPORT#2"
	job = {
      source             : {
        connection       : targetConnectionName
	  , schema           : "target2"
	  }
	, target             : {
	    connection       : sourceConnectionName
	  }
    , parameters         : { 
        ...parameters
      , FILE             : filename2
      , TARGET_DIRECTORY : exportDirectory               
      }
    , abortOnError       : true
    , recreateSchema     : this.RECREATE_SCHEMA
    }
    jobs[jobName] = job
    batch.push(jobName)
    
	this.CONFIGURATION = {
	  connections      : connections
	, schemas          : schemas
	, jobs             : jobs
	, batchOperations  : {
		"steps"        : batch
	  }
	}
	
    // console.dir(this.CONFIGURATION,{depth:null})
	
	let testResults
	try {
      testResults = await this.runNamedBatch("steps",false)
	} catch (e) {
	  this.LOGGER.handleException([this.OPERATION_NAME,sourceConnectionName,targetConnectionName,'EXECUTE'],e)
      this.metrics.recordError(this.LOGGER.getMetrics(true))
      return;
    }

    // console.dir(testResults,{depth:null})

	if (testResults.hasOwnProperty('e')) {
	  this.LOGGER.handleException([this.OPERATION_NAME,sourceConnectionName,targetConnectionName,'RESULTS'],testResults.e)
      this.metrics.recordError(this.LOGGER.getMetrics(true))
      return;
	}

    const operationsList = [testResults[batch[0]].source.description]
	for (let job of batch) {
	  const jobSummary = testResults[job]
	  operationsList.push(jobSummary.target.description)	 
      this.metrics.recordTaskTimings([task.taskName,job,jobSummary.target.mode,jobSummary.source.connection || sourceConnectionName,jobSummary.target.connection,jobSummary.elapsedTime])
	}
	
	if (batch.length > 1) {
      this.metrics.recordTaskTimings([task.taskName,'TASK','',sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(testResults.endTime - testResults.startTime)])
	}

    // Compare Results
    
	const dbDBI = await this.getTargetConnection(this.yadamu,jobs['IMPORT#1'])
	const dbComparator = await dbDBI.getComparator({})

    // Import #1: Array Size Vs Rows Imported
	
    const compareConfiguration1 = {
      target             : {
 	    vendor           : targetDatabase
      ,	schema           : dbDBI.getSchema(targetSchema1)
    }
    , parameters         : parameters
    , metrics            : testResults['IMPORT#1'].metrics
    , identifierMappings : this.COMPARE_MAPPINGS
    }
   
    let startTime = performance.now()
	dbComparator.configuration = compareConfiguration1
    await dbComparator.reportRowCounts() 
    let elapsedTime = performance.now() - startTime 
    this.metrics.recordTaskTimings([task.taskName,'COUNT','',targetConnectionName,'',YadamuLibrary.stringifyDuration(elapsedTime)])

    // Import #2: Array Size Vs Rows Imported
	
    const compareConfiguration2 = {
      target             : {
 	    vendor           : targetDatabase
      ,	schema           : dbDBI.getSchema(targetSchema2)
    }
    , parameters         : parameters
    , metrics            : testResults['IMPORT#2'].metrics
    , identifierMappings : this.COMPARE_MAPPINGS
    }

    startTime = performance.now()
	dbComparator.configuration = compareConfiguration2
    await dbComparator.reportRowCounts() 
    elapsedTime = performance.now() - startTime 
    this.metrics.recordTaskTimings([task.taskName,'COUNT','',targetConnectionName,'',YadamuLibrary.stringifyDuration(elapsedTime)])

    const initialExportFile = test.parser === 'SQL' ? filename : testResults['IMPORT#1'].source.file
    const parser = new YadamuExportParser(this.LOGGER)
    await parser.parse(initialExportFile,'systemInformation');
    const systemInformation = parser.getTarget()
		
    // If the export file originated in the target database compare the imported schema with the source schema.

    if ((testResults['IMPORT#1'].target.vendor === systemInformation.vendor) && (testResults['IMPORT#1'].target.version === systemInformation.databaseVersion)) {

   	  const results = testResults['IMPORT#1']

      // Compare Source Schema #1 and Target Schema #1
		
	  const compareConfiguration = {
	    source                   : {
		  vendor                 : results.target.vendor
	    , version                : results.target.version
	    , schema                 : dbDBI.getSchema(sourceSchema)
  	    }
	  , target                   : {
		  vendor                 : results.target.vendor
	    , version                : results.target.version
	    , schema                 : dbDBI.getSchema(targetSchema1)
	    }
      , parameters               : parameters
	  , metrics                  : results.metrics
	  , includeRowCounts         : false
      , identifierMappings       : this.COMPARE_MAPPINGS
	  , ddlApplied               : results.target.ddlValid
	  , includeMaterializedViews : results.target.hasMaterializedViews
	  }

      dbComparator.configuration = compareConfiguration
	  let compareResults
	  try {
        compareResults = await this.compare(targetConnectionName, targetConnectionName, task.taskName, dbComparator);	  
	  } catch (e) {
	    await dbDBI.final();
        this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,'',YadamuLibrary.stringifyDuration(performance.now() = startTime)])
        this.metrics.recordError(this.LOGGER.getMetrics(true))
        return
      }
      this.metrics.recordTaskTimings([task.taskName,'COMPARE','',targetConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])	  
    }

    // Compare Target Schema #1 and Target Schema #2

 	const results = testResults['IMPORT#2']

    const compareConfiguration = {
	  source                   : {
	    vendor                 : results.target.vendor
      , version                : results.target.version
      , schema                 : dbDBI.getSchema(targetSchema1)
  	  }
	, target                   : {
	    vendor                 : results.target.vendor
      , version                : results.target.version
	  , schema                 : dbDBI.getSchema(targetSchema2)
	  }
    , parameters               : parameters
	, metrics                  : results.metrics
	, includeRowCounts         : false
	, identifierMappings       : this.COMPARE_MAPPINGS
    , ddlApplied               : results.target.ddlValid
	, includeMaterializedViews : results.target.hasMaterializedViews
 	}

    let compareResults
    dbComparator.configuration = compareConfiguration
	try {
      compareResults = await this.compare(targetConnectionName, targetConnectionName, task.taskName, dbComparator);	  
	} catch (e) {
      await dbDBI.final();
      this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,'',YadamuLibrary.stringifyDuration(performance.now() = startTime)])
      this.metrics.recordError(this.LOGGER.getMetrics(true))
      return
    }
    this.metrics.recordTaskTimings([task.taskName,'COMPARE','',targetConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
	
	await dbDBI.final()

    const fileCompareConfiguration = {
	  source             : {
	    vendor           : results.target.vendor
      , version          : results.target.version
      , schema           : targetSchema1.database ? targetSchema1 : targetSchema1.schema 
  	  }
	, target             : {
	    vendor           : results.source.vendor
	  , version          : results.source.version
	  , files            : [initialExportFile, testResults['EXPORT#1'].target.file,testResults['EXPORT#2'].target.file]
	  }
    , parameters         : parameters
	, metrics            : Object.values(testResults).reduce((metrics,jobSummary) => {
		jobSummary.hasOwnProperty('metrics') && metrics.push(jobSummary.metrics); 
		return metrics 
	  },[])
	, includeRowCounts   : false
    , identifierMappings : this.COMPARE_MAPPINGS
	}

	const fileDBI = await this.getSourceConnection(this.yadamu,jobs['IMPORT#1']) 
	const fileComparator =  await fileDBI.getComparator(fileCompareConfiguration)
    startTime = performance.now();
	const fileCompareResults = await this.compareFiles(sourceConnectionName,targetConnectionName,task.taskName,fileComparator)
    elapsedTime = performance.now() - startTime
    this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceDatabase,'',YadamuLibrary.stringifyDuration(elapsedTime)])
    await fileDBI.final()
	
	elapsedTime = performance.now() - testResults.startTime
    this.LOGGER.qa([this.OPERATION_NAME,`${test.parser === 'SQL' ? 'SQL' : 'CLARINET'}`].concat(operationsList),`Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
    this.metrics.recordTaskTimings([task.taskName,'TOTAL','','file',targetConnectionName,YadamuLibrary.stringifyDuration(elapsedTime)])
	
    this.reportTimings(this.metrics.timings)
    return
	
  }
  
  async import(task,configuration,test,targetConnectionName,parameters) {
    
	const jobs = {}
	const batch = []
	                 
    const sourceConnectionName = test.source
    
	const sourceConnection = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnection = this.getConnection(configuration.connections,targetConnectionName)
    
    const sourceDatabase =  YadamuLibrary.getVendorName(sourceConnection);
    const targetDatabase =  YadamuLibrary.getVendorName(targetConnection);

	const connections = {
	  [sourceConnectionName] : sourceConnection
	, [targetConnectionName] : targetConnection
	}
	  
	const schemas = {
	  source : this.getSourceMapping(sourceDatabase,task)
    , target : this.getTargetMapping(targetDatabase,task)
	}

    this.yadamu.MACROS = {
	  ...this.yadamu.MACROS
    , connection           : sourceConnectionName
    , location             : this.TEST_CONFIGURATION.tasks.datasetLocation[task.vendor]
    , mode                 : this.yadamu.MODE
    , operation            : this.OPERATION
    , task                 : task.taskName 
    , vendor               : targetDatabase
    , sourceConnection     : sourceConnectionName 
    , targetConnection     : targetConnectionName
    , importPath           : this.IMPORT_PATH
    , exportPath           : this.EXPORT_PATH
    }

    const filename = task.hasOwnProperty('schemaPrefix') ? `${task.schemaPrefix}_${task.file}` : task.file
    const directory = test.parameters?.SOURCE_DIRECTORY || configuration.parameters?.SOURCE_DIRECTORY || test.parameters?.DIRECTORY || configuration.parameters?.DIRECTORY ||''
    const importDirectory = YadamuLibrary.macroSubstitions(path.join(directory,this.IMPORT_PATH),this.yadamu.MACROS)
    
    const jobName = "IMPORT"
	const job = {
	  source : {
	    connection       : sourceConnectionName
	  }
	, target : {
        connection       : targetConnectionName
	  , schema           : "target"
      }
    , parameters : { 
        ...parameters
      , FILE             : filename
      , SOURCE_DIRECTORY : importDirectory               
      }
    , abortOnError       : true
    , recreateSchema     : this.RECREATE_SCHEMA
    }
    jobs[jobName] = job
    batch.push(jobName)

	this.CONFIGURATION = {
	  connections      : connections
	, schemas          : schemas
	, jobs             : jobs
	, batchOperations  : {
		"steps"        : batch
	  }
	}
	
    // console.dir(this.CONFIGURATION,{depth:null})
    
	let testResults
	try {
      testResults = await this.runNamedBatch("steps",false)
	} catch (e) {
	  this.LOGGER.handleException([this.OPERATION_NAME,sourceConnectionName,targetConnectionName,'EXECUTE'],e)
	  this.metrics.recordError(this.LOGGER.getMetrics(true))
      return;
    }
      
    // console.dir(testResults,{depth:null})

	if (testResults.hasOwnProperty('e')) {
	  this.LOGGER.handleException([this.OPERATION_NAME,sourceConnectionName,targetConnectionName,'RESULTS'],testResults.e)
      this.metrics.recordError(this.LOGGER.getMetrics(true))
      return;
	}

    const operationsList = [testResults[batch[0]].source.description]
	for (let job of batch) {
	  const jobSummary = testResults[job]
	  operationsList.push(jobSummary.target.description)	 
      this.metrics.recordTaskTimings([task.taskName,job,jobSummary.target.mode,jobSummary.source.connection,jobSummary.target.connection,jobSummary.elapsedTime])
	}

    const results = testResults[jobName]

    if ((this.VERIFY_OPERATION === true) && (parameters.MODE !== 'DDL_ONLY')) { 
	
      const compareDBI = await this.getTargetConnection(this.yadamu,jobs[batch[0]])
	  
	  const compareParameters = {
		...parameters
	  }
      delete compareParameters.IDENTIFIER_MAPPING_FILE

	  const results = testResults[jobName]
 
      const compareConfiguration = {
	    target                     : {
    	  vendor                   : results.target.vendor
	    , schema                   : compareDBI.getSchema(schemas.target)
	    }
    	, parameters               : compareParameters
	    , metrics                  : results.metrics
	    , ddlApplied               : results.target.ddlValid
	    , includeMaterializedViews : results.target.hasMaterializedViews
      }
	  
      const comparator = await compareDBI.getComparator(compareConfiguration)
	  const startTime = performance.now()
      await comparator.reportRowCounts() 
      const elapsedTime = performance.now() - startTime 
      this.metrics.recordTaskTimings([task.taskName,'COUNT','',targetConnectionName,'',YadamuLibrary.stringifyDuration(elapsedTime)])
	  await compareDBI.final()
	  
	}

    const elapsedTime =  performance.now() - testResults.startTime
    this.metrics.recordTaskTimings([task.taskName,'TOTAL','',test.source,targetConnectionName,YadamuLibrary.stringifyDuration(elapsedTime)])
	this.reportTimings(this.metrics.timings)
    return  YadamuConstants.FILE_BASED_DRIVERS.includes(sourceDatabase)  ? results.source.file : path.dirname(fileReader.CONTROL_FILE_PATH)
  
  }
 
  async export(task,configuration,test,targetConnectionName,parameters) {
      	  
	const jobs = {}
	const batch = []
	                 
    const sourceConnectionName = test.source
    
	const sourceConnection = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnection = this.getConnection(configuration.connections,targetConnectionName)
    
    const sourceDatabase =  YadamuLibrary.getVendorName(sourceConnection);
    const targetDatabase =  YadamuLibrary.getVendorName(targetConnection);

	const connections = {
	  [sourceConnectionName] : sourceConnection
	, [targetConnectionName] : targetConnection
	}
	  
	const schemas = {
	  source : this.getSourceMapping(sourceDatabase,task)
    , target : this.getTargetMapping(targetDatabase,task,'1')
	}

    this.yadamu.MACROS = {
	  ...this.yadamu.MACROS
    , connection           : sourceConnectionName
    , location             : this.TEST_CONFIGURATION.tasks.datasetLocation[task.vendor]
    , mode                 : this.yadamu.MODE
    , operation            : this.OPERATION
    , task                 : task.taskName 
    , vendor               : sourceDatabase
    , sourceConnection     : sourceConnectionName 
    , targetConnection     : targetConnectionName
    , importPath           : this.IMPORT_PATH
    , exportPath           : this.EXPORT_PATH
    }

    const filename = task.hasOwnProperty('schemaPrefix') ? `${task.schemaPrefix}_${task.file}` : task.file
    const directory = this.test.parameters?.TARGET_DIRECTORY || this.TEST_CONFIGURATION.parameters?.TARGET_DIRECTORY || this.test.parameters?.DIRECTORY || this.TEST_CONFIGURATION.parameters?.DIRECTORY || ''
    const exportDirectory = YadamuLibrary.macroSubstitions(path.join(directory,this.EXPORT_PATH),this.yadamu.MACROS)    

    const jobName = "EXPORT"
	const job = {
	  source             : {
	    connection       : sourceConnectionName
	  , schema           : "source"
	  }
	, target             : {
        connection       : targetConnectionName
      }
    , parameters         : { 
        ...parameters
      , FILE             : filename
      , TARGET_DIRECTORY : exportDirectory               
      }
    , abortOnError       : true
    , recreateSchema     : this.RECREATE_SCHEMA
    }
    jobs[jobName] = job
    batch.push(jobName)

    if (this.VERIFY_OPERATION === true) {

      const jobName = "IMPORT"
	  const job = {
	    source             : {
          connection       : targetConnectionName
	    }
	  , target : {
	      connection       : sourceConnectionName
	    , schema           : "target"
	    }
      , parameters         : { 
          ...parameters
        , FILE             : filename
        , TARGET_DIRECTORY : exportDirectory               
        }
      , abortOnError       : true
      , recreateSchema     : true
	  }
      jobs[jobName] = job
      batch.push(jobName)						 
    }

	this.CONFIGURATION = {
	  connections      : connections
	, schemas          : schemas
	, jobs             : jobs
	, batchOperations  : {
		"steps"        : batch
	  }
	}
	
    // console.dir(this.CONFIGURATION,{depth:null})
    
	let testResults
	try {
      testResults = await this.runNamedBatch("steps",false)
	} catch (e) {
	  this.LOGGER.handleException([this.OPERATION_NAME,sourceConnectionName,targetConnectionName,'EXECUTE'],e)
	  this.metrics.recordError(this.LOGGER.getMetrics(true))
      return;
    }
      
    // console.dir(testResults,{depth:null})

	if (testResults.hasOwnProperty('e')) {
	  this.LOGGER.handleException([this.OPERATION_NAME,sourceConnectionName,targetConnectionName,'RESULTS'],testResults.e)
      this.metrics.recordError(this.LOGGER.getMetrics(true))
      return;
	}

    const operationsList = [testResults[batch[0]].source.description]
	for (let job of batch) {
	  const jobSummary = testResults[job]
	  operationsList.push(jobSummary.target.description)	 
      this.metrics.recordTaskTimings([task.taskName,job,jobSummary.target.mode,jobSummary.source.connection,jobSummary.target.connection,jobSummary.elapsedTime])
	}

    if (batch.length > 1) {
	  this.metrics.recordTaskTimings([task.taskName,'TASK','',sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(testResults.endTime - testResults.startTime)])
	}

    if ((this.VERIFY_OPERATION === true) && (parameters.MODE !== 'DDL_ONLY')) { 
	
      const compareDBI = await this.getSourceConnection(this.yadamu,jobs[batch[0]])
	  
	  const compareParameters = {...parameters}
      delete compareParameters.IDENTIFIER_MAPPING_FILE

	  const results = testResults[batch[batch.length-1]]
	  const compareConfiguration = {
	    source                     : {
		  vendor                   : sourceDatabase
	    , version                  : results.source.version
	    , schema                   : compareDBI.getSchema(schemas.source)
	    }
	  , target                     : {
		  vendor                   : sourceDatabase
	    , version                  : results.source.version
	    , schema                   : compareDBI.getSchema(schemas.target)
	    }
        , parameters               : compareParameters
	    , metrics                  : results.metrics
	    , includeRowCounts         : true
	    , identifierMappings       : this.COMPARE_MAPPINGS
	    , ddlApplied               : results.target.ddlValid
	    , includeMaterializedViews : results.target.hasMaterializedViews
      }
	  
  	  const comparator = await compareDBI.getComparator(compareConfiguration)
	
	  let compareResults
	  const startTime = performance.now()
	  try {
	    compareResults = await this.compare(sourceConnectionName,targetConnectionName,task.taskName,comparator)
	  } catch (e) {
	    await compareDBI.final();
        this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,'',YadamuLibrary.stringifyDuration(performance.now() - startTime)])
        this.metrics.recordError(this.LOGGER.getMetrics(true))
        return
      }
      await compareDBI.final();
      this.metrics.recordTaskTimings([task.taskName,'COMPARE','',test.source,targetConnectionName,YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
	}

    const elapsedTime =  performance.now() - testResults.startTime
    this.metrics.recordTaskTimings([task.taskName,'TOTAL','',test.source,targetConnectionName,YadamuLibrary.stringifyDuration(elapsedTime)])
	this.reportTimings(this.metrics.timings)
    return YadamuConstants.FILE_BASED_DRIVERS.includes(targetDatabase)  ? exportDirectory : path.dirname(fileWriter.CONTROL_FILE_PATH)

  }
  
  async copy(task,configuration,test,targetConnectionName,parameters) {
	
	// Used by Initialize.. Simulates a pure one direction copy between any two supported environments
	   
	const jobs = {}
	const batch = []
	                 
    const sourceConnectionName = test.source
    
	const sourceConnection = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnection = this.getConnection(configuration.connections,targetConnectionName)
    
    const sourceDatabase =  YadamuLibrary.getVendorName(sourceConnection);
    const targetDatabase =  YadamuLibrary.getVendorName(targetConnection);

	const connections = {
	  [sourceConnectionName] : sourceConnection
	, [targetConnectionName] : targetConnection
	}
	  
	const schemas = {
	  source : this.getSourceMapping(sourceDatabase,task)
    , target : this.getTargetMapping(targetDatabase,task,this.TARGET_SCHEMA_SUFFIX)
	}
	    
    const jobName = "COPY"
	const job = {
	  source : {
	    connection : sourceConnectionName
	  , schema     : "source"
	  }
	, target : {
        connection : targetConnectionName
	  , schema     : "target"
      }
    , parameters : { 
      ...parameters
      , MODE : 'DATA_ONLY'
      }
    , abortOnError   : true
    , recreateSchema : this.RECREATE_SCHEMA
    }
    jobs[jobName] = job
    batch.push(jobName)

    this.setPsuedoKeyTransformation(sourceDatabase,targetDatabase,parameters);

    this.yadamu.MACROS = {
      ...this.yadamu.MACROS
    , connection           : targetConnectionName
    , location             : this.TEST_CONFIGURATION.tasks.datasetLocation[task.vendor]
    , mode                 : this.yadamu.MODE
    , operation            : this.OPERATION
    , task                 : task.taskName 
    , vendor               : targetDatabase
    , sourceConnection     : sourceConnectionName 
    , targetConnection     : targetConnectionName
    // , sourceUser           : sourceParameters.FROM_USER
    // , targetUser           : targetParameters.TO_USER
    }
    
	this.CONFIGURATION = {
	  connections      : connections
	, schemas          : schemas
	, jobs             : jobs
	, batchOperations  : {
		"steps"        : batch
	  }
	}
	
    // console.dir(this.CONFIGURATION,{depth:null})
    

    let testResults
	try {
      testResults = await this.runNamedBatch("steps",false)
	} catch (e) {
	  this.LOGGER.handleException([this.OPERATION_NAME,sourceConnectionName,targetConnectionName,'EXECUTE'],e)
	  this.metrics.recordError(this.LOGGER.getMetrics(true))
      return;
    }
      
    // console.dir(testResults,{depth:null})

	if (testResults.hasOwnProperty('e')) {
	  this.LOGGER.handleException([this.OPERATION_NAME,sourceConnectionName,targetConnectionName,'RESULTS'],testResults.e)
      this.metrics.recordError(this.LOGGER.getMetrics(true))
      return;
	}

    const operationsList = [testResults[batch[0]].source.description]
	for (let job of batch) {
	  const jobSummary = testResults[job]
	  operationsList.push(jobSummary.target.description)	 
      this.metrics.recordTaskTimings([task.taskName,jobName,jobSummary.target.mode,jobSummary.source.connection,jobSummary.target.connection,jobSummary.elapsedTime])
	}

	const sourceDBI = await this.getSourceConnection(this.yadamu,job)
    const sourceComparator = await sourceDBI.getComparator({})
	
	const targetDBI = await this.getTargetConnection(this.yadamu,job) 
    const targetComparator = await targetDBI.getComparator({})
    
    const startTime = performance.now()
    const sourceRowCounts = await sourceComparator.getRowCounts(sourceDBI.getSchema(schemas.source))
	const targetRowCounts = await targetComparator.getRowCounts(targetDBI.getSchema(schemas.target))
	const endTime = performance.now()

    await sourceDBI.final();
    await targetDBI.final();
   
    const elapsedTime = performance.now() - startTime
    
	const compareResults = {
      successful : []
    , failed : []
    , elapsedTime : elapsedTime
    }
  
    targetRowCounts.forEach((targetTable) => {
	  const metrics = testResults[jobName].metrics
      if (metrics.hasOwnProperty(targetTable[1])) {
        const sourceTable = sourceRowCounts.find((sourceTable) => { return targetTable[1] === sourceTable[1]})
        const targetMetrics = metrics[targetTable[1]]
        if (sourceTable[2] === targetTable[2]) {
          compareResults.successful.push([sourceTable[0],targetTable[0],sourceTable[1],sourceTable[2],targetMetrics.elapsedTime,targetMetrics.throughput])
        }
        else {
          compareResults.failed.push([sourceTable[0],targetTable[0],sourceTable[1],sourceTable[2],targetTable[2],-1,-1,'','','',''])
        }
      }
    })
	
    targetComparator.printCompareResults(compareResults)
    
  }

  calculateColumnSizes(tabularResults) {
    const colSizes = new Array(tabularResults[0].length).fill(0)
    tabularResults.forEach((row) => {
      row.forEach((value,idx) => {
         switch (typeof value) {
           case 'number':
             row[idx] = value.toLocaleString()
             break;
           case 'boolean':
              row[idx] = Boolean(value).toString()
             break;
           case 'object':
             if (Array.isArray(value)) {
               value = value.map((v) => { return typeof v === 'number' ? v.toLocaleString() : v })
             } 
             else {
               row[idx] = JSON.stringify(value)
             }
             break;
           default:
         }
         colSizes[idx] = colSizes[idx] < row[idx].length ? row[idx].length : colSizes[idx]
      })
    })
    return colSizes
  }
  
  formatArray(summary,i) {
    let columnValues = summary.map((r) => {return r[i]})
    columnValues.shift();
    const colSizes = this.calculateColumnSizes(columnValues)
    columnValues = columnValues.map((r) => { 
      return r.map((c,i) => {
        return (typeof c === 'number' ? c.toLocaleString() : c).padStart(colSizes[i]+1)
      }).join(' | ')
    })
    columnValues.forEach((v,j) => {
      summary[j+1][i] = v
    })
  }
  
  formatArrays(summary) {
    summary[1].forEach((c,i) => {
      if (Array.isArray(c)) {
        this.formatArray(summary,i)
      }
    })
  }
  
  formatSummary(summary) {

    const colSizes = this.calculateColumnSizes(summary)      
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    });
   
    summary.forEach((row,idx) => {          
      if (idx < 2) {
        this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
      this.LOGGER.writeDirect(`|`)
      row.forEach((col,idx) => {this.LOGGER.writeDirect(` ${col.padStart(colSizes[idx])} |`)});
      this.LOGGER.writeDirect(`\n`)
    })

    if (summary.length > 1) {
      this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }     
  }
  
  printTargetSummary(summary) {
    
    const colSizes = this.calculateColumnSizes(summary)      
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    });

    summary.forEach((row,idx) => {          
      if (idx < 2) {
        this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
      this.LOGGER.writeDirect(`|`)
      row.forEach((col,idx) => {this.LOGGER.writeDirect(` ${col.padStart(colSizes[idx])} |`)});
      this.LOGGER.writeDirect(`\n`)
      if (row[3] === '') {
        this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
    })


    if (summary.length > 1) {
      this.LOGGER.writeDirect('\n') 
    }     
    
  }
  
  printSourceSummary(summary) {
    
    const colSizes = this.calculateColumnSizes(summary)      
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    });
   
    this.LOGGER.writeDirect(`\n`)

    summary.filter((r,idx) => {return ((idx ===0) || ((r[2] !== '') && (r[3] === '')))}).forEach((row,idx) => {          
      if (idx < 2) {
        this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
      this.LOGGER.writeDirect(`|`)
      row.forEach((col,idx) => {this.LOGGER.writeDirect(` ${col.padStart(colSizes[idx])} |`)});
      this.LOGGER.writeDirect(`\n`)
    })

    if (summary.length > 1) {
      this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }     
  }
  
  printFailedSummary() {
     
    if (YadamuLibrary.isEmpty(this.failedOperations)) return
      
    this.LOGGER.writeDirect(`\n`)
      
    const failed = [['Source','Target','Data Set','Table','Source Rows','Target Rows','Missing Rows','Extra Rows','Cause']]
    Object.keys(this.failedOperations).forEach((source) => {
      let col1 = source
      Object.keys(this.failedOperations[source]).forEach((target) => {
        let col2 = target
        Object.keys(this.failedOperations[source][target]).forEach((test) => {
          let col3 = test
          Object.keys(this.failedOperations[source][target][test]).forEach((table) => {
            const info = this.failedOperations[source][target][test][table]
            failed.push([col1,col2,col3,table,info[3],info[4],info[5],info[6],info[7] === null ? '' : info[7]])
            col1 = ''
            col2 = ''
            col3 = ''
          })
        })
      })
    })

    const colSizes = this.calculateColumnSizes(failed)      
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    });

    failed.forEach((row,idx) => {          
      if (idx < 2) {
        this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
      this.LOGGER.writeDirect(`|`)
      row.forEach((col,idx) => {this.LOGGER.writeDirect(` ${col.padStart(colSizes[idx])} |`)});
      this.LOGGER.writeDirect(`\n`)
    })

    if (failed.length > 1) {
      this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
    }   
  }

  reportTimings(timings) {
	  	  
    timings.unshift(['Data Set','Step','Mode','Source','Target','Elapsed Time'])
     
    const colSizes = this.calculateColumnSizes(timings)      
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    });

    this.LOGGER.writeDirect(`\n`)
   
    timings.forEach((row,idx) => {          
      if (idx < 2) {
        this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
      this.LOGGER.writeDirect(`|`)
      row.forEach((col,idx) => {this.LOGGER.writeDirect(` ${col.padStart(colSizes[idx])} |`)});
      this.LOGGER.writeDirect(`\n`)
    })

    if (timings.length > 1) {
      this.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }     
  }

  getTaskList(configuration,task) {
    if (typeof task === 'string') {
      if (this.expandedTaskList[task] === undefined) {
        this.expandedTaskList[task] = []
        if (Array.isArray(configuration.tasks[task])) {
          for (const subTask of configuration.tasks[task]) {
            const newTask = this.getTaskList(configuration,subTask)
            if (newTask.length === 1) {
              newTask[0].taskName = subTask
            }
            this.expandedTaskList[task] = this.expandedTaskList[task].concat(newTask)
          }
        }
        else {
          const taskList = configuration.tasks[task]
          if (taskList === undefined) {
            throw new ConfigurationFileError(`Named task "${task}" not defined. Valid values: "${Object.keys(configuration.tasks)}".`);
          }
          taskList.taskName = task
          this.expandedTaskList[task] = [taskList]
        }
      }
      const taskList = this.expandedTaskList[task]
      if (taskList === undefined) {
        throw new ConfigurationFileError(`Named task "${task}" not defined. Currently known tasks: "${Object.keys(configuration.tasks)}".`);
      }
      return taskList
    }
    else {
      task.taskName = 'Anonymous'
      return [task]
    }
  }
  
  nextTest() {
    this.metrics.newSubTask();
    this.IDENTIFIER_MAPPINGS = {}
	this.COMPARE_MAPPINGS = {}
	this.TABLE_FILTER = []
	return performance.now()
  }
  
  async doTests() {

    this.LOGGER.qa([`Environemnt`,process.arch,process.platform,process.version],`Running tests`);

    const sourceSummary = []
    this.metrics = new Metrics()
    const startTime = performance.now()
     
    const summary = [['End Time','Operation','Source','Target','Task','Results','Memory Usage','Elapsed Time']]
    try {    
      for (this.test of this.TEST_CONFIGURATION.tests) {
		// this.metrics = new Metrics()
        const startTime = performance.now()
       
        // Initialize Configuration Parameters with values from this.TEST_CONFIGURATION file
        const testParameters = {...this.TEST_CONFIGURATION.parameters || {}}
		// Merge test specific parameters
        Object.assign(testParameters, this.test.parameters || {})
        this.yadamu.initializeParameters(testParameters);

        let sourceDescription = this.test.source;
        const targets = this.test.target ? [this.test.target] : this.test.targets
        try {
          for (const target of targets) {
            this.metrics.newTarget()
            const startTime = performance.now()             
            let targetDescription = target
            const targetConnection = this.TEST_CONFIGURATION.connections[target]
            try {
              for (const task of this.test.tasks) {
                this.metrics.newTask();
                const startTime = performance.now()
                const subTasks = this.getTaskList(this.TEST_CONFIGURATION,task)
                try {
                  for (const subTask of subTasks) {
                    const startTime = this.nextTest()
                    try {
                      switch (this.OPERATION_NAME) {
                        case 'EXPORT':
                        case 'UNLOAD':
                          const exportPath = await this.export(subTask,this.TEST_CONFIGURATION,this.test,target,testParameters)
                          targetDescription = 'file://' + exportPath
                          break;
                        case 'IMPORT':
                        case 'LOAD':
                          const importPath = await this.import(subTask,this.TEST_CONFIGURATION,this.test,target,testParameters)
                          sourceDescription = 'file://' + importPath
                          break;
                        case 'FILEROUNDTRIP':
                        case 'LOADERROUNDTRIP':
                          await this.fileRoundtrip(subTask,this.TEST_CONFIGURATION,this.test,target,testParameters)
                          break;
                        case 'DBROUNDTRIP':
                          await this.dbRoundtrip(subTask,this.TEST_CONFIGURATION,this.test,target,testParameters)
                          break;
                        case 'LOSTCONNECTION':
                          await this.dbRoundtrip(subTask,this.TEST_CONFIGURATION,this.test,target,testParameters)
                          break;
                        case 'COPY':
                          await this.copy(subTask,this.TEST_CONFIGURATION,this.test,target,testParameters)
                          break;
                      }
                      // Report SubTask metrics and rollup to Task
                      const elapsedTime = performance.now() - startTime;
                      if (subTasks.length > 1 ) {
                        const elapsedTime = performance.now() - startTime;
                        this.LOGGER.qa([this.OPERATION_NAME,`SUB-TASK`,sourceDescription,targetDescription,task,subTask.taskName],`${this.metrics.formatMetrics(this.metrics.subTask)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
                        // summary.push([new Date().toISOString(),this.OPERATION_NAME,sourceDescription,targetDescription,Object.values(this.metrics.subTask),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
                      }
                      this.metrics.aggregateTask()
                    } catch (e) {
                      this.LOGGER.handleException([this.OPERATION_NAME,`SUB-TASK`,sourceDescription,targetDescription,typeof task === 'string' ? task : `Anonymous`],e);
                      this.metrics.aggregateTask()
                      throw (e)
                    }
                  } 
                  const elapsedTime = performance.now() - startTime;
                  this.LOGGER.qa([this.OPERATION_NAME,`TASK`,sourceDescription,targetDescription,typeof task === 'string' ? task : `Anonymous`],`${this.metrics.formatMetrics(this.metrics.task)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
                  summary.push([new Date().toISOString(),this.OPERATION_NAME,sourceDescription,targetDescription,typeof task === 'string' ? task : `Anonymous`,Object.values(this.metrics.task),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
                  this.metrics.aggregateTarget()
                } catch (e) {
                  this.LOGGER.handleException([this.OPERATION_NAME,`TASK`,sourceDescription,targetDescription,typeof task === 'string' ? task : `Anonymous`],e);
                  this.metrics.aggregateTarget()
                  throw (e)
                }
              }
              const elapsedTime = performance.now() - startTime;
              this.LOGGER.qa([this.OPERATION_NAME,`TARGET`,sourceDescription,targetDescription],`${this.metrics.formatMetrics(this.metrics.target)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);     
              summary.push([new Date().toISOString(),this.OPERATION_NAME,sourceDescription,targetDescription,'',Object.values(this.metrics.target),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
              this.metrics.aggregateTest()
            } catch (e) {
              this.LOGGER.handleException([this.OPERATION_NAME,`TARGET`,sourceDescription,targetDescription],e);
              this.metrics.aggregateTest()
              throw e
            }
          }        
          const elapsedTime = performance.now() - startTime;
          this.LOGGER.qa([this.OPERATION_NAME,`TEST`,`${sourceDescription}`],`${this.metrics.formatMetrics(this.metrics.test)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
          summary.push([new Date().toISOString(),this.OPERATION_NAME,sourceDescription,'','',Object.values(this.metrics.test),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
          this.metrics.aggregateSuite()
        } catch (e) {
          this.LOGGER.handleException([this.OPERATION_NAME,`TEST`,`${sourceDescription}`],e);
          this.metrics.aggregateSuite()
          throw e
        }      
      }
    } catch (e) {
		console.log(e)
    }

    const elapsedTime = performance.now() - startTime;
    summary.push([new Date().toISOString(),'','','','',Object.values(this.metrics.suite),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
    this.formatArrays(summary)
    this.printFailedSummary()
    this.printSourceSummary(summary)
    this.printTargetSummary(summary)
    // wtf.dump();
	
	const results = this.metrics.formatMetrics(this.metrics.suite)
    this.LOGGER.log([`QA`,`YADAMU`,`REGRESSION`,`${this.yadamu.CONFIG}`],`${results} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
	await this.yadamu.close(true)
	return results
  } 

  createYadamu() {	  
	return new Yadamu(this.command);
  }

  logJobResults(tags,message) {
	 // Supress the Job Results message in QA
  }

  reverseIdentifierMappings(tableMappings) {
      
    if (tableMappings) {
      const reverseMappings = {}
      Object.keys(tableMappings).forEach((table) => {
        const newKey = tableMappings[table].tableName || table
        reverseMappings[newKey] = {}
        if (newKey !== table) {
          reverseMappings[newKey].tableName = table
        }
        if (tableMappings[table].columnMappings) {
          const columnMappings = {};
          Object.keys(tableMappings[table].columnMappings).forEach((column) => {
            const newKey = tableMappings[table].columnMappings[column].name || column
            columnMappings[newKey] = {}
            if (newKey !== column) {
              columnMappings[newKey].name = column
            }
          });
          reverseMappings[newKey].columnMappings = columnMappings
        }
      })
      return reverseMappings;
    }
    return tableMappings;
  }
   
  getTargetState(dbi,description)  {
	// Cache Identifier Mappings used by target Connection. 
	this.IDENTIFIER_MAPPINGS = dbi.IDENTIFIER_MAPPINGS
	this.COMPARE_MAPPINGS = !YadamuLibrary.isEmpty(this.IDENTIFIER_MAPPINGS) ? this.IDENTIFIER_MAPPINGS : this.COMPARE_MAPPINGS
	// Cache Table Filter used by target Connection. 
	this.TABLE_FILTER = dbi.parameters.TABLES || []
	// Adjust any table filter to reflect the mapped table names from the IDENTIFIER_MAPPINGS returned by the target
	this.TABLE_FILTER = (!YadamuLibrary.isEmpty(this.IDENTIFIER_MAPPINGS) && (this.TABLE_FILTER.length > 0)) ? this.TABLE_FILTER.map((tableName) => { return this.IDENTIFIER_MAPPINGS?.[tableName]?.hasOwnProperty('tableName') ?  this.IDENTIFIER_MAPPINGS[tableName].tableName : tableName}) : this.TABLE_FILTER
	return super.getTargetState(dbi,description) 
  }

  async executeJob(yadamu,configuration,job,jobName) {
	
	const results = await super.executeJob(yadamu,configuration,job,jobName) 
	return results

  }
  
  expandConfiguration(configuration,parentFile) {
	 
	 // Support CONN paramtere to specifiy connection file.
	
	super.expandConfiguration(configuration,parentFile)
	if (this.parameters.hasOwnProperty('CONNECTION')) {
	  configuration.connections = YadamuLibrary.loadIncludeFile(this.parameters.CONNECTION,parentFile,this.yadamuLogger)
	}
  }

  setParameter(parameterName,parameterValue) {
	  
    super.setParameter(parameterName,parameterValue)

    switch (parameterName.toUpperCase()) {
      case 'CONN':		  
      case '--CONN':
      case 'CONNECTION':		  
      case '--CONNECTION':
        this.parameters.CONNECTION = parameterValue;
        break;   
   	}
  }  
  
}
 
export { Test as default}

async function main() {
 
  try {
    const testHarness = new Test();
    try {
	  await testHarness.doTests();
    } catch (e) {
	  testHarness.reportError(e)
    }
    await testHarness.close();
  } catch (e) {
	YadamuLibrary.reportError(e)
  }

}

main()

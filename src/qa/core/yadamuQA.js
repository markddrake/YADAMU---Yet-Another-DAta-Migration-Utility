
import path               from 'path'
import fs                 from 'fs';
import fsp                from 'fs/promises';

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

import YadamuConstants    from '../../node/lib/yadamuConstants.js';
import YadamuLibrary      from '../../node/lib/yadamuLibrary.js';
import JSONParser         from '../../node/dbi/file/jsonParser.js';
import LoaderDBI          from '../../node/dbi/loader/loaderDBI.js';

import {
  ConfigurationFileError
}                         from '../..//node/core/yadamuException.js';

import Yadamu                 from './yadamu.js';

// import wtf from 'wtfnode';

// const YadamuDefaults = require('./yadamuDefaults.json')

const  __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const YadamuDefaults = fs.readFileSync(path.join(__dirname,'../cfg/yadamuDefaults.json'),'utf-8');
  
class YadamuExportParser extends Transform {
  
  constructor(yadamuLogger) {
    super({objectMode: true });
    this.yadamuLogger = yadamuLogger
  }
  
  async parse(inputFile, targetName) {
    this.targetName = targetName
    this.inputStream = await new Promise((resolve,reject) => {
      const inputStream = fs.createReadStream(inputFile);
      inputStream.on('open',() => {resolve(inputStream)}).on('error',(err) => {reject(err)})
    })
    this.jsonParser =  new JSONParser(this.yadamuLogger,'DDL_ONLY',inputFile);
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
     this.yadamuLogger.logException([`${this.constructor.name}._transform()`],e);
     callback(e);
	})
  }
  
  getTarget() {
    return this.target
  }
}

class YadamuQA {

  get VERIFY_OPERATION()                          { return this.test.hasOwnProperty('verifyOperation') ? this.test.verifyOperation : this.configuration.hasOwnProperty('verifyOperation') ? this.configuration.verifyOperation : false }
  get RECREATE_SCHEMA()                           { return this.test.hasOwnProperty('recreateSchema') ? this.test.recreateSchema : this.configuration.hasOwnProperty('recreateSchema') ? this.configuration.recreateSchema : false }
  get TARGET_SCHEMA_SUFFIX()                      { return this.test.hasOwnProperty('targetSchemaSuffix') ? this.test.targetSchemaSuffix : this.configuration.hasOwnProperty('targetSchemaSuffix') ? this.configuration.targetSchemaSuffix : "1" }
  get COMPARE_SCHEMA_SUFFIX()                     { return this.test.hasOwnProperty('comapreSchemaSuffix') ? this.test.compareSchemaSuffix : this.configuration.hasOwnProperty('compareSchemaSuffix') ? this.configuration.compareSchemaSuffix : "1" }
  get KILL_CONNECTION()                           { return this.test.hasOwnProperty('kill') ? this.test.kill : this.configuration.hasOwnProperty('kill') ? this.configuration.kill : false }
  get STAGING_AREA()                              { return this.test.hasOwnProperty('stagingArea') ? this.test.stagingArea : this.configuration.hasOwnProperty('stagingArea') ? this.configuration.stagingArea : undefined }
  get RELOAD_STAGING_AREA()                       { return this.test.hasOwnProperty('reloadStagingArea') ? this.test.reloadStagingArea : this.configuration.hasOwnProperty('reloadStagingArea') ? this.configuration.reloadStagingArea : false }
  get EMPTY_STRING_IS_NULL()                      { return this.test.hasOwnProperty('emptyStringIsNull') ? this.test.emptyStringIsNull : this.configuration.hasOwnProperty('emptyStringIsNull') ? this.configuration.emptyStringIsNull : undefined }
  get MIN_BIGINT_IS_NULL()                        { return this.test.hasOwnProperty('minBigEntIsNull') ? this.test.minBigEntIsNull : this.configuration.hasOwnProperty('minBigEntIsNull') ? this.configuration.minBigEntIsNull : undefined }
  get SKIP_DATA_STAGING()                         { return this.test.hasOwnProperty('skipDataStaging') ? this.test.skipDataStaging : this.configuration.hasOwnProperty('skipDataStaging') ? this.configuration.skipDataStaging : false }
  
  get EXPORT_PATH()                               { return this.test.exportPath || this.configuration.exportPath || '' }
  get IMPORT_PATH()                               { return this.test.importPath || this.configuration.importPath || '' }
  get OPERATION()                                 { return this.test.operation  || this.configuration.operation }
  get OPERATION_NAME()                            { return this.OPERATION.toUpperCase() }

  constructor(configuration) {
      
    this.configuration = configuration
    this.yadamu = new Yadamu(configuration.parameters)
    this.metrics = this.yadamu.testMetrics;
    
    this.expandedTaskList = []
    this.operationsList = []
    this.failedOperations = {}
  }
  
  async initialize() {
    await this.yadamu.initialize()
  }
  
  async getDatabaseInterface(driver,connectionSettings,parameters,recreateSchema,identifierMappings) {

    let dbi = undefined

    // Reset yadamu with the parameters specified by the test - values specified for the test will override the QA defaults
    await this.yadamu.reset(parameters);
 
    // clone the connectionSettings
    const connectionInfo = Object.assign({}, connectionSettings);
    if (Yadamu.QA_DRIVER_MAPPINGS.hasOwnProperty(driver)) { 
      const DBI = (await import(Yadamu.QA_DRIVER_MAPPINGS[driver])).default
      dbi = driver === 'file' ? new DBI(this.yadamu,connectionInfo,parameters) : new DBI(this.yadamu,null,connectionInfo,parameters)
    }   
    else {   
      const err = new ConfigurationFileError(`[${this.constructor.name}.getDatabaseInterface()]: Unsupported database vendor "${driver}".`);  
      throw err
    }
    
	if (this.KILL_CONNECTION  && (dbi.ROLE === this.KILL_CONNECTION.process)) {
      this.yadamu.configureTermination(this.KILL_CONNECTION)
    }
   
    if (identifierMappings) {
      this.yadamu.IDENTIFIER_MAPPINGS = identifierMappings 
    }

    dbi.IDENTIFIER_MAPPINGS = identifierMappings
    dbi.setOption('recreateSchema',recreateSchema);
 
    return dbi;
  }

  getConnection(connectionList, connectionName) {
   
    const connection = connectionList[connectionName]
    if (connection === undefined) {
      throw new ConfigurationFileError(`Named connection "${connectionName}" not found. Valid connections: "${Object.keys(connectionList)}".`);
    }
    return connection;
    
  }

  getPrefixedSchema(prefix,schema) {
      
     return prefix ? `${prefix}_${schema}` : schema
     
  }
 
  getSourceMapping(vendor,operation) {
     const schemaInfo = this._getSourceMapping(vendor,operation)
     // console.log('getSourceMapping()',vendor,operation,schemaInfo)
     return schemaInfo
  }
 
  _getSourceMapping(vendor,operation) {
      
    let schema
    let database
    const schemaInfo = (typeof operation.source === 'string') ? { schema : operation.source } : Object.assign({}, operation.source); 
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
  
  getTargetMapping(vendor,operation,modifier) {
     const schemaInfo = this._getTargetMapping(vendor,operation,modifier)
     // console.log('getTargetMapping()',vendor,operation,modifier,schemaInfo)
     return schemaInfo
  }
  
  _getTargetMapping(vendor,operation,modifier) {
          
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
  
  getDescription(connectionName,dbi) {
    return `${connectionName}://"${dbi.DESCRIPTION}"`
  }

  setUser(parameters,key,db,schemaInfo,database) {
      
    switch (db) {
      case 'mssql':
        if (schemaInfo.schema) {
          parameters.YADAMU_DATABASE = schemaInfo.schema
          parameters[key] = 'dbo'
        }
        else {
          parameters.YADAMU_DATABASE = schemaInfo.database
          parameters[key] = schemaInfo.owner
        }
        break;
      case 'snowflake':
        parameters.YADAMU_DATABASE = schemaInfo.database || database.toUpperCase()
        parameters[key] = schemaInfo.schema ? schemaInfo.schema : schemaInfo.owner
        // ### Force Database name to uppercase otherwise queries against INFORMATION_SCHEMA fail
        // parameters.YADAMU_DATABASE = parameters.YADAMU_DATABASE.toUpperCase();
        break;
      default:
        parameters[key] = schemaInfo.schema !== undefined ? schemaInfo.schema : (schemaInfo.owner === 'dbo' ? schemaInfo.database : schemaInfo.owner)
    }
    return parameters

  }
  
  printResults(operation,sourceDescription,targetDescription,elapsedTime) {
    
    this.operationsList.push(sourceDescription);

    const stepMetrics = this.yadamu.LOGGER.getMetrics(true)
    this.metrics.adjust(stepMetrics)
    
    if (this.yadamu.LOGGER.FILE_LOGGER) {
      
      const colSizes = [24,128,14]
      let seperatorSize = (colSizes.length * 3) - 1;
      colSizes.forEach((size)  => {
        seperatorSize += size;
      });
    
      this.yadamu.LOGGER.writeDirect('\n+' + '-'.repeat(seperatorSize) + '+' + '\n') 
     
      this.yadamu.LOGGER.writeDirect(`| ${'TIMESTAMP'.padEnd(colSizes[0])} |`
                                   + ` ${'OPERATION'.padEnd(colSizes[1])} |`
                                   + ` ${'ELASPED TIME'.padStart(colSizes[2])} |` 
                                   + '\n');
      this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      
      this.yadamu.LOGGER.writeDirect(`| ${new Date().toISOString().padEnd(colSizes[0])} |`
                                   + ` ${(sourceDescription + ' --> ' + targetDescription).padEnd(colSizes[1])} |`
                                   + ` ${YadamuLibrary.stringifyDuration(elapsedTime).padStart(colSizes[2])} |` 
                                   + '\n');
                 
      this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
    else {
      this.yadamu.LOGGER.qa([operation,`COPY`,sourceDescription,targetDescription],`${this.metrics.formatMetrics(stepMetrics)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
    }

    this.metrics.aggregateSubTask(stepMetrics);
  
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
    
  fixupMetrics(metrics) {
    // If operations failed metrics may be undefined. If so replace with empty object to prevent errors when reporting
    metrics.forEach((t,i) => {
      if ((t === undefined) || (t === null)) {
        metrics[i] = {}
      }
    })
  }  
  
  unmapTableName(tableName,identifierMappings) {
    identifierMappings = identifierMappings || {}
    return Object.keys(identifierMappings)[Object.values(identifierMappings).findIndex((v) => { return v?.tableName === tableName})] || tableName
  }

  async compare(sourceConnectionName,targetConnectionName,testId,comparator) {
	  
    
	try {
	  const compareResults = await comparator.compare()
	
	  if (compareResults.failed.length > 0) {
        this.failedOperations[sourceConnectionName] = Object.assign({},this.failedOperations[sourceConnectionName])
        this.failedOperations[sourceConnectionName][targetConnectionName] = Object.assign({},this.failedOperations[sourceConnectionName][targetConnectionName])
        compareResults.failed.forEach((failed,idx) => {
          this.failedOperations[sourceConnectionName][targetConnectionName][testId] = Object.assign({},this.failedOperations[sourceConnectionName][targetConnectionName][testId])
          this.failedOperations[sourceConnectionName][targetConnectionName][testId][failed[2]] = compareResults.failed[idx]
        })
        // this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
      }
    	  
      if (YadamuLibrary.isEmpty(compareResults)) {
        this.yadamu.LOGGER.qa(['COMPARE',sourceConnectionName,targetConnectionName,testId],'Compare Operation failed')
        return {};
      }
	  
	  return compareResults
	} catch (e) {
      this.yadamu.LOGGER.handleException([`COMPARE`],e)
      return {}
    } 
  }
 
  async compareFiles(sourceConnectionName,targetConnectionName,testId,configuration) {

	try {
      const compareDBI = await this.getDatabaseInterface('file',{},configuration.parameters,null,configuration.identifierMappings)
	  const comparator = await compareDBI.getComparator(configuration)
	  const compareResults = await comparator.compareFiles(configuration.target.files[0],configuration.target.files[1],configuration.target.files[2],configuration.metrics)
	
      if (compareResults.length > 0) {
        this.failedOperations[sourceConnectionName] = Object.assign({},this.failedOperations[sourceConnectionName])
        this.failedOperations[sourceConnectionName][targetConnectionName] = Object.assign({},this.failedOperations[sourceConnectionName][targetConnectionName])
        compareResults.forEach((failed,idx) => {
          this.failedOperations[sourceConnectionName][targetConnectionName][testId] = Object.assign({},this.failedOperations[sourceConnectionName][targetConnectionName][testId])
          this.failedOperations[sourceConnectionName][targetConnectionName][testId][failed[2]] = failed
        })
	  }
	  return compareResults
	} catch (e) {
      this.yadamu.LOGGER.handleException([`FILE-COMPARE`],e)
      return {}
    } 
   
  }

  dbRoundtripResults(operationsList,elapsedTime) {
          
    if (this.yadamu.LOGGER.FILE_LOGGER) {
      
      const colSizes = [24,128,14]
      let seperatorSize = (colSizes.length * 3) - 1;
      colSizes.forEach((size)  => {
        seperatorSize += size;
      });
    
      this.yadamu.LOGGER.writeDirect('\n+' + '-'.repeat(seperatorSize) + '+' + '\n') 
     
      this.yadamu.LOGGER.writeDirect(`| ${'TIMESTAMP'.padEnd(colSizes[0])} |`
                                   + ` ${'OPERATION'.padEnd(colSizes[1])} |`
                                   + ` ${'ELASPED TIME'.padStart(colSizes[2])} |`     
                                   + '\n');
      this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      
      this.yadamu.LOGGER.writeDirect(`| ${new Date().toISOString().padEnd(colSizes[0])} |`
                                   + ` ${(operationsList[0] + ' --> ' + operationsList[operationsList.length-1]).padEnd(colSizes[1])} |`
                                   + ` ${(YadamuLibrary.stringifyDuration(elapsedTime)+"ms").padStart(colSizes[2])} |` 
                                   + '\n');
                 
      this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
    else {
      this.yadamu.LOGGER.qa([`DBROUNDTRIP`,`STEP`].concat(operationsList),`${this.metrics.formatMetrics(this.metrics.task)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
    }
     
    this.reportTimings(this.metrics.timings)
        
  }
  
  setPsuedoKeyTransformation(sourceDatabase,targetDatabase,parameters) {
     
    if (sourceDatabase === 'mongodb') {
       parameters.MONGO_STRIP_ID = parameters.MONGO_STRIP_ID || false
    }
    
    if (targetDatabase === 'mongodb') {
      parameters.MONGO_STRIP_ID = parameters.MONGO_STRIP_ID || true
    }
      
    if (sourceDatabase === 'cockroach') {
       parameters.COCKROACH_STRIP_ROWID = parameters.COCKROACH_STRIP_ROWID || false
    }
    
	if (targetDatabase === 'cockroach') {
      parameters.COCKROACH_STRIP_ROWID = parameters.COCKROACH_STRIP_ROWID || true
    }
  
  }
  
  /*
  **
  ** Compare the Conttrol File with the System Information section from the SourceDBI to check that the data set is usable
  **
  */
  
  async validateStagedData(source,staging) {
     try {
       await source.initialize()
       const sourceInstance = await source.getYadamuInstanceInfo() 
       await source.final()
	   
       staging.parameters.FROM_USER = staging.parameters.TO_USER
       delete staging.parameters.TO_USER
       await staging.initialize()
       await staging.loadControlFile()
       const targetInstance = await staging.getYadamuInstanceInfo();
	   await staging.final()
	   
	   if (staging.controlFile.settings.contentType === 'CSV') {
         if ((sourceInstance.yadamuInstanceID === targetInstance.yadamuInstanceID) && (sourceInstance.yadamuInstallationTimestamp === targetInstance.yadamuInstallationTimestamp)) {
           this.yadamu.LOGGER.qa([source.DATABASE_VENDOR,staging.DATABASE_VENDOR,'COPY',staging.DATABASE_KEY],`Using existing Data Set "${staging.CONTROL_FILE_PATH}" with ID "${targetInstance.yadamuInstanceID}".`);
           return true;
         } 
         else {
           this.yadamu.LOGGER.qa([source.DATABASE_VENDOR,staging.DATABASE_VENDOR,'COPY',staging.DATABASE_KEY],`Cannot use existing Data Set "${staging.CONTROL_FILE_PATH}". Exepected ID "${sourceInstance.yadamuInstanceID}", found ID "${targetInstance.yadamuInstanceID}".`);
         }
       }
       else {
         this.yadamu.LOGGER.qa([source.DATABASE_VENDOR,staging.DATABASE_VENDOR,'COPY',staging.DATABASE_KEY],`Cannot use existing Data Set "${staging.CONTROL_FILE_PATH}". Exepected format "CSV", found format "${staging.controlFile.settings.contentType}".`);
       }
     } catch (e) {
	   try {
		 await source.destroy(e);
		 await staging.destroy(e)
	   } catch (e) { /* If anything goes wrong the staged data is not valid  */ console.log(e) } 
	 }
     return false     
  }


  async copy(task,configuration,test,targetConnectionName,parameters) {
	
	// Used by Initialize.. Simulates a pure one direction copy between any two supported environments
	
    let results
    let keyMetrics
    
    const operationsList = []
    let identifierMappings = {}
    let outboundParameters
    
    let sourceConnectionName = test.source

    const sourceConnection = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnection = this.getConnection(configuration.connections,targetConnectionName)

    const sourceDatabase =  YadamuLibrary.getVendorName(sourceConnection);
    const targetDatabase =  YadamuLibrary.getVendorName(targetConnection);
    this.setPsuedoKeyTransformation(sourceDatabase,targetDatabase,parameters);
    
    const sourceSchema  = this.getSourceMapping(sourceDatabase,task);
    const targetSchema  = this.getTargetMapping(targetDatabase,task,this.TARGET_SCHEMA_SUFFIX);
        
    const sourceParameters  = Object.assign({},parameters)
    const targetParameters = Object.assign({},parameters)
    
    this.setUser(sourceParameters,'FROM_USER',sourceDatabase, sourceSchema)
    // this.setUser(targetParameters,'TO_USER', sourceDatabase, targetSchema)
    this.setUser(targetParameters,'TO_USER', targetDatabase, targetSchema)

    this.yadamu.MACROS = Object.assign(this.yadamu.MACROS, {
      connection           : targetConnectionName
    , location             : this.configuration.tasks.datasetLocation[task.vendor]
    , mode                 : this.yadamu.MODE
    , operation            : this.OPERATION
    , task                 : task.taskName 
    , vendor               : targetDatabase
    , sourceConnection     : sourceConnectionName 
    , targetConnection     : targetConnectionName
    , sourceUser           : sourceParameters.FROM_USER
    , targetUser           : targetParameters.TO_USER
    })
    
    let sourceDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)
    let targetDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,this.RECREATE_SCHEMA) 

    const taskStartTime  = performance.now();
    let stepStartTime = taskStartTime
    
    keyMetrics = await this.yadamu.pumpData(sourceDBI,targetDBI);
    let stepElapsedTime = performance.now() - stepStartTime

    let sourceVersion = sourceDBI.DATABASE_VERSION;
    let targetVersion = targetDBI.DATABASE_VERSION;
    const sourceDescription = this.getDescription(sourceConnectionName,sourceDBI)  
    const targetDescription = this.getDescription(targetConnectionName,targetDBI)  

    this.metrics.recordTaskTimings([task.taskName,'COPY',targetDBI.MODE,sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (keyMetrics instanceof Error) {
      const compareRules = this.getCompareRules(sourceDatabase,sourceVersion,targetDatabase,targetVersion,targetParameters)
      const compareResults = await this.compareSchemas( sourceDatabase, targetDatabase, sourceSchema, targetSchema, sourceConnection, parameters, compareRules, this.yadamu.metrics, false, undefined)
      this.printCompareResults(sourceConnectionName,targetConnectionName,task.taskName,compareResults)
      this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
      this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
      return;
    }

    sourceDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false) 
    const sourceComparator = await sourceDBI.getComparator({})
    stepStartTime = performance.now()
    const sourceRowCounts = await sourceComparator.getRowCounts(sourceDBI.getSchema(sourceSchema))
    stepElapsedTime = performance.now() - stepStartTime 
    await sourceDBI.final();
    
    targetDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,false) 
    const targetComparator = await targetDBI.getComparator({})
    stepStartTime = performance.now()
	const targetRowCounts = await targetComparator.getRowCounts(targetDBI.getSchema(targetSchema))
    stepElapsedTime = performance.now() - stepStartTime 
    await targetDBI.final();
   
    const elapsedTime = performance.now() - taskStartTime
    
    const compareResults = {
      successful : []
    , failed : []
    , elapsedTime : elapsedTime
    }
    
    targetRowCounts.forEach((targetTable) => {
      if (keyMetrics.hasOwnProperty(targetTable[1])) {
        const sourceTable = sourceRowCounts.find((sourceTable) => { return targetTable[1] === sourceTable[1]})
        const targetMetrics = keyMetrics[targetTable[1]]
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
    
    DIRECT: The first step is performed in DDL_AND_DATA mod. A single operation is required to clone the schema and copy the data.
    
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
                     
    let sourceConnectionName = test.source
    
    const stagedCopy = (this.STAGING_AREA !== undefined) && !this.SKIP_DATA_STAGING
    const homogeneousCopy = sourceConnectionName === targetConnectionName
    const taskMode = this.yadamu.MODE

    let results
    let keyMetrics
    
    const operationsList = []
    let identifierMappings = {}
    let outboundParameters
    
    const sourceConnection = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnection = this.getConnection(configuration.connections,targetConnectionName)
    
    const sourceDatabase =  YadamuLibrary.getVendorName(sourceConnection);
    const targetDatabase =  YadamuLibrary.getVendorName(targetConnection);
    this.setPsuedoKeyTransformation(sourceDatabase,targetDatabase,parameters);
    
    const sourceSchema  = this.getSourceMapping(sourceDatabase,task);
    const targetSchema  = this.getTargetMapping(targetDatabase,task,this.TARGET_SCHEMA_SUFFIX);
    const compareSchema = this.getTargetMapping(sourceDatabase,task,this.COMPARE_SCHEMA_SUFFIX);
       
    const sourceParameters  = Object.assign({},parameters)
    sourceParameters.MODE = (homogeneousCopy && !stagedCopy && (taskMode !== 'DDL_ONLY')) ? 'DDL_AND_DATA' : 'DDL_ONLY'

    // Do not apply IDENTIFIER MAPPINGS during the 'CLONE' phase
    
    const compareParameters = Object.assign({},parameters)
    compareParameters.MODE = sourceParameters.MODE;
    delete compareParameters.IDENTIFIER_MAPPING_FILE

    const targetParameters  = Object.assign({},parameters)
       
    this.setUser(sourceParameters,'FROM_USER',sourceDatabase, sourceSchema)
    this.setUser(targetParameters,'TO_USER', targetDatabase, targetSchema)
    this.setUser(compareParameters,'TO_USER', sourceDatabase, compareSchema)

    this.yadamu.MACROS = Object.assign(this.yadamu.MACROS, {
      connection           : targetConnectionName
    , location             : this.configuration.tasks.datasetLocation[task.vendor]
    , mode                 : taskMode
    , operation            : this.OPERATION
    , task                 : task.taskName 
    , vendor               : targetDatabase
    , sourceConnection     : sourceConnectionName 
    , targetConnection     : targetConnectionName
    , sourceUser           : sourceParameters.FROM_USER
    , targetUser           : targetParameters.TO_USER
    })
    
    /*
    **
    ** Perform a DDL_ONLY mode operation to createa clone of the source schema in the source database. 
    ** 
    ** For a direct, homogeneous copy this step becomes a DDL_AND_DATA mode operation.
    **
    */
                
    sourceParameters.MODE = (homogeneousCopy && !stagedCopy && (taskMode !== 'DDL_ONLY')) ? 'DDL_AND_DATA' : 'DDL_ONLY'
    
	let sourceDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)
    let targetDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,compareParameters,this.RECREATE_SCHEMA) 

    const taskStartTime = performance.now();
    
	let stepStartTime = taskStartTime
    keyMetrics = await this.yadamu.pumpData(sourceDBI,targetDBI);
    let stepElapsedTime = performance.now() - stepStartTime
    this.metrics.recordTaskTimings([task.taskName,'COPY',sourceParameters.MODE,sourceConnectionName,sourceConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    const sourceVersion = sourceDBI.DATABASE_VERSION;
    const sourceDescription = this.getDescription(sourceConnectionName,sourceDBI)  

    let targetVersion = targetDBI.DATABASE_VERSION;
    let targetDescription = this.getDescription(sourceConnectionName,targetDBI)  

	const compareDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,parameters,false)

	const compareConfiguration = {
	  source             : {
		vendor           : sourceDatabase
	  , version          : sourceVersion
	  , schema           : compareDBI.getSchema(sourceSchema)
	  }
	, target             : {
		vendor           : targetDatabase
	  ,	version          : targetVersion
	  ,	schema           : compareDBI.getSchema(compareSchema)
	  }
    , parameters         : compareParameters
	, metrics            : keyMetrics
	, includeRowCounts   : false
	, identifierMappings : identifierMappings
	, copyFromCSV        : stagedCopy
    }
    
    const comparator = await compareDBI.getComparator(compareConfiguration)
	
    if (keyMetrics instanceof Error) {
	  const compareResults = await this.compare(sourceConnectionName,targetConnectionName,task.taskName,comparator)
	  compareDBI.final();
      this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
      this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
      return
    }
       
    operationsList.push(sourceDescription)
    operationsList.push(targetDescription)
    this.printResults(this.OPERATION_NAME,sourceDescription,targetDescription,stepElapsedTime) 
        
    /*
    **
    ** If the first copy operation was perfomed in DDL_AND_DATA mode no further operations are necessary 
    **
    */
    
    if ((taskMode === 'DDL_ONLY') && homogeneousCopy) {
      return
    }

    if (sourceParameters.MODE !== 'DDL_AND_DATA') {

      // Reset the Operations List to avoid reporting the Schema Clone operation.
   
      operationsList.length = 0   
      sourceParameters.MODE = 'DATA_ONLY'    
      compareParameters.MODE = 'DATA_ONLY'
   
      sourceDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)
	  targetDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,this.RECREATE_SCHEMA)	
   
      if (stagedCopy) { 
       
        /*
        **
        ** Construct a DBI for the staging platform.
        **
        */
            
        let stagingDescription
                
        const stagingConnectionName     = this.STAGING_AREA
        const stagingConnection         = this.getConnection(configuration.connections,stagingConnectionName)
        const stagingDatabase           = YadamuLibrary.getVendorName(stagingConnection);
        const stagingSchema             = this.getTargetMapping(stagingDatabase,task,'1')
          
        const stagingParameters         = Object.assign({},targetParameters)
        stagingParameters.OUTPUT_FORMAT = 'CSV'
        this.setUser(stagingParameters,'TO_USER',stagingDatabase,stagingSchema)
        const stagingDBI = await this.getDatabaseInterface(stagingDatabase,stagingConnection,stagingParameters,this.RELOAD_STAGING_AREA)
        
	    targetDBI.verifyStagingSource(stagingDBI.DATABASE_KEY)
        
        /*
        **
        ** YADAMU re1oads the staging platform when the required data is not aleady present 
        **
        ** This behavoir can be overridden usimg the setting "realoadStagingArea: true"
        **
        */
   
        // Use YADAMU_INSTANCE_ID to check if the required data set is already available in the staging area. 
        
        stepStartTime = performance.now();
        const vSourceDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)
        const vStageDBI = await this.getDatabaseInterface(stagingDatabase,stagingConnection,stagingParameters,false)
        const dataStagingRequired = this.RELOAD_STAGING_AREA || !await this.validateStagedData(vSourceDBI,vStageDBI)
        stepElapsedTime = performance.now() - stepStartTime
		
        if (dataStagingRequired) {
          // Copy the dataset to the staging platform

          stepStartTime = performance.now();
          results = await this.yadamu.pumpData(sourceDBI,stagingDBI);
          stepElapsedTime = performance.now() - stepStartTime

          stagingDescription = this.getDescription(stagingConnectionName,stagingDBI)  

          this.metrics.recordTaskTimings([task.taskName,'STAGE',stagingDBI.MODE,sourceConnectionName,stagingConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
          if (results instanceof Error) {
	        const compareResults = await this.compare(sourceConnectionName,stagingConnectionName,task.taskName,comparator)
	        compareDBI.final();
            this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
            this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
            return;
          }
          operationsList.push(sourceDescription)
          operationsList.push(stagingDescription)
          this.printResults(this.OPERATION_NAME,sourceDescription,stagingDescription,stepElapsedTime)
        }
        else {
          this.metrics.recordTaskTimings([task.taskName,'STAGE','VERIFY',sourceConnectionName,stagingConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
        }   
		// Staging becomes the sourceDBI
        stagingParameters.FROM_USER = stagingParameters.TO_USER
        delete stagingParameters.TO_USER
        sourceDBI =  await this.getDatabaseInterface(stagingDatabase,stagingConnection,stagingParameters,false)
		sourceConnectionName = stagingConnectionName
      }  
      else {
        sourceDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)
      }
   
      if (!homogeneousCopy) {
          
        // Move the source / staged data to the target.
        
        stepStartTime = performance.now();        
		keyMetrics = await this.yadamu.pumpData(sourceDBI,targetDBI);
        stepElapsedTime = performance.now() - stepStartTime
        
		const stepFromDescription = this.getDescription(sourceConnectionName,sourceDBI) 
        
		compareConfiguration.target.version = targetDBI.DATABASE_VERSION
        targetDescription = this.getDescription(targetConnectionName,targetDBI)  
        identifierMappings = targetDBI.IDENTIFIER_MAPPINGS
          
        this.metrics.recordTaskTimings([task.taskName,'COPY',targetDBI.MODE,sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
		if (keyMetrics instanceof Error) {
          const compareResults = await this.compare(sourceConnectionName,targetConnectionName,task.taskName,comparator)
	      compareDBI.final();
          this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
          this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
          return;
        }
   
        operationsList.push(stepFromDescription)
        operationsList.push(targetDescription)
        this.printResults(this.OPERATION_NAME,stepFromDescription,targetDescription,stepElapsedTime)
          
        targetParameters.FROM_USER = targetParameters.TO_USER
        delete targetParameters.TO_USER
        
        /*
        **
        ** If a TABLES filter was supplied generate an updated TABLES filter for the reverse copy by applying identifier mappings to the original set of tables.
        **      
        */
   
        if (sourceParameters.TABLES && !YadamuLibrary.isEmpty(identifierMappings)) {
		  targetParameters.TABLES = sourceParameters.TABLES.map((tableName) => { return identifierMappings?.[tableName]?.hasOwnProperty('tableName') ?  identifierMappings[tableName]?.tableName : tableName}) 
        }     
   
        sourceDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,false)
		sourceConnectionName = targetConnectionName
      }
      
      targetDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,compareParameters,false) 
      sourceConnectionName = test.source        

	  await this.yadamu.reset(compareParameters);
	  this.yadamu.IDENTIFIER_MAPPINGS = this.reverseIdentifierMappings(identifierMappings)
        
      if ((sourceDBI instanceof LoaderDBI) && (targetDBI instanceof LoaderDBI)) {
        // Configure the the OUTPUT_FORMAT, COMPRESSION AND ENCRYPTION settings to the target Parameters
        targetDBI.setControlFileSettings(sourceDBI.getControlFileSettings())
      }   
        
	  // Copy the target back to the source	
		
      stepStartTime = performance.now();
      results = await this.yadamu.pumpData(sourceDBI,targetDBI);
      stepElapsedTime = performance.now() - stepStartTime
      
      this.metrics.recordTaskTimings([task.taskName,'COPY',targetDBI.MODE,targetConnectionName,test.source,YadamuLibrary.stringifyDuration(stepElapsedTime)])
      if (results instanceof Error) {
        const compareResults = await this.compare(test.source,targetConnectionName,task.taskName,comparator)
        compareDBI.final();
        this.metrics.recordTaskTimings([task.taskName,'COMPARE','',test.source,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
        this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
        return;
      }
      
      if (stagedCopy && homogeneousCopy) {
        keyMetrics = results
      }
        
      operationsList.push(sourceDescription)
      this.printResults(this.OPERATION_NAME,targetDescription,sourceDescription,stepElapsedTime)
       
      const taskElapsedTime =  performance.now() - taskStartTime
      this.metrics.recordTaskTimings([task.taskName,'TASK','',test.source,targetConnectionName,YadamuLibrary.stringifyDuration(taskElapsedTime)])
    } 
    
    this.yadamu.parameters.MODE = taskMode
    compareParameters.XML_STORAGE_OPTION = sourceDBI.DATA_TYPES?.storageOptions?.XML_TYPE
		
    /*
    const compareRules = this.getCompareRules(sourceDatabase,sourceVersion,targetDatabase,targetVersion,compareParameters)
    const compareResults = await this.compareSchemas( sourceDatabase, targetDatabase, sourceSchema, compareSchema, sourceConnection, parameters, compareRules, keyMetrics, false, identifierMappings)
	this.printCompareResults(test.source,targetConnectionName,task.taskName,compareResults)
	*/
    
	compareConfiguration.metrics = keyMetrics
	const compareResults = await this.compare(sourceConnectionName,targetConnectionName,task.taskName,comparator)
	compareDBI.final();

	this.metrics.recordFailed(compareResults.failed?.length || 0)
    this.metrics.recordTaskTimings([task.taskName,'COMPARE','',test.source,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
    const elapsedTime =  performance.now() - taskStartTime
    this.metrics.recordTaskTimings([task.taskName,'TOTAL','',test.source,targetConnectionName,YadamuLibrary.stringifyDuration(elapsedTime)])
	this.dbRoundtripResults(operationsList,elapsedTime)
   
    return;
    
  }
  
  supportsDDLGeneration(instance) {
    do { 
      if (Object.getOwnPropertyDescriptor(instance,'getDDLOperations')  !== null) return instance.constructor.name;
    } while ((instance = Object.getPrototypeOf(instance)))
  }
  
  async fileRoundtrip(task,configuration,test,targetConnectionName,parameters) {
  
  /*
  **
  ** QA Test 
  **
  ** 1. Create targetSchema#1 in targetDB
  ** 2. Import FILE to targetSchema#1
  ** 3. If File.vendor = targetDB compare sourceSchema with targetSchema#1
  ** 4. Export targetSchema#1 to FILE#1
  ** 5. Create targetSchema#2 in targetDB
  ** 6. Import FILE#1 into targetSchema#2.
  ** 7. Compare targetSchema#1 with targetSchema#2
  ** 8. Export targetSchema#2 to FILE#2
  ** 9. Compare FILE, FILE#1, FILE#2
  **
  */
  
    const metrics = []
    this.operationsList.length = 0;
    
    const sourceConnectionName = test.source

    const sourceConnection = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnection = this.getConnection(configuration.connections,targetConnectionName)

    const sourceDatabase =  YadamuLibrary.getVendorName(sourceConnection);
    const targetDatabase =  YadamuLibrary.getVendorName(targetConnection);

    this.yadamu.MACROS = Object.assign(this.yadamu.MACROS, {
      connection       : targetConnectionName
    , location         : this.configuration.tasks.datasetLocation[task.vendor]
    , mode             : this.yadamu.MODE
    , operation        : this.OPERATION
    , task             : task.taskName 
    , vendor           : targetDatabase
    , sourceConnection : sourceConnectionName 
    , targetconnection : targetConnectionName
    , importPath       : this.IMPORT_PATH
    , exportPath       : this.EXPORT_PATH
    })

    const filename = task.hasOwnProperty('schemaPrefix') ? `${task.schemaPrefix}_${task.file}` : task.file
    const sourceDirectory = this.test.parameters?.SOURCE_DIRECTORY || this.configuration.parameters?.SOURCE_DIRECTORY || this.test.parameters?.DIRECTORY || this.configuration.parameters?.DIRECTORY
    const targetDirectory = this.test.parameters?.TARGET_DIRECTORY || this.configuration.parameters?.TARGET_DIRECTORY || this.test.parameters?.DIRECTORY || this.configuration.parameters?.DIRECTORY
    const importDirectory = YadamuLibrary.macroSubstitions(path.join(sourceDirectory,this.EXPORT_PATH),this.yadamu.MACROS)  
    const exportDirectory = YadamuLibrary.macroSubstitions(path.join(targetDirectory,this.IMPORT_PATH),this.yadamu.MACROS)  
    
    const sourcePathComponents = path.parse(filename);
    const filename1 = sourcePathComponents.name + ".1" + sourcePathComponents.ext
    const filename2 = sourcePathComponents.name + ".2" + sourcePathComponents.ext

    const sourceSchema = this.getSourceMapping(targetDatabase,task)
    const targetSchema1  = this.getTargetMapping(targetDatabase,task,'1')
    const targetSchema2  = this.getTargetMapping(targetDatabase,task,'2');

    const taskStartTime = performance.now();

    // Source File to Target Schema #1
    
    let sourceParameters  = Object.assign({},parameters)
    if (sourceDatabase === "file") {
      sourceParameters.FILE = filename
      sourceParameters.SOURCE_DIRECTORY = importDirectory
	  sourceParameters.FROM_USER = YadamuConstants.READER_ROLE
    }
    
    let fileReader = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,null)
    
    let targetParameters  = Object.assign({},parameters)
    this.setUser(targetParameters,'TO_USER',targetDatabase, targetSchema1)

    let targetDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,this.RECREATE_SCHEMA)
    const targetDBVersion = targetDBI.DATABASE_VERSION;
    
    let sourceAndTargetMatch
    let stepStartTime = taskStartTime   
    if (test.parser === 'SQL') {
      fileReader.DIRECTORY = fileReader.SOURCE_DIRECTORY
      targetDBI.setParameters({"FILE" : fileReader.FILE})
      fileReader.setDescription(fileReader.FILE)
      metrics.push(await this.yadamu.uploadData(targetDBI))
      const parser = new YadamuExportParser(this.yadamu.LOGGER)
      await parser.parse(targetDBI.UPLOAD_FILE,'systemInformation');
      const systemInformation = parser.getTarget()
      sourceAndTargetMatch = ((targetDBI.DATABASE_VENDOR === systemInformation.vendor) && (targetDBI.DATABASE_VERSION === systemInformation.databaseVersion))
    }
    else {
      let results = await this.yadamu.pumpData(fileReader,targetDBI) 
      let sourceDescription = this.getDescription('file',fileReader)
      metrics.push(results)
      if (!(results instanceof Error)) {
        sourceAndTargetMatch = ((targetDBI.DATABASE_VENDOR === targetDBI.systemInformation.vendor) && (targetDBI.DATABASE_VERSION === targetDBI.systemInformation.databaseVersion))
      }
    }
    let stepElapsedTime = performance.now() - stepStartTime
    const sourceFile = fileReader.FILE
    let sourceDescription = this.getDescription('file',fileReader)
    let targetDescription = this.getDescription(targetConnectionName,targetDBI)
    
    this.metrics.recordTaskTimings([task.taskName,'IMPORT',targetDBI.MODE,sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
      return;
    }
    
    let targetVersion = targetDBI.DATABASE_VERSION
    let identifierMappings = targetDBI.IDENTIFIER_MAPPINGS
    this.printResults(this.OPERATION_NAME,sourceDescription,targetDescription,stepElapsedTime)
    sourceDescription = targetDescription

    sourceParameters  = Object.assign({},parameters)
    this.setUser(sourceParameters,'FROM_USER',targetDatabase, targetSchema1)
    
    let sourceDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,sourceParameters,null,{})

    targetParameters  = Object.assign({},parameters)
    targetParameters.FILE = filename1
    targetParameters.TARGET_DIRECTORY = exportDirectory
	targetParameters.TO_USER = YadamuConstants.WRITER_ROLE
    
	let tableFilterList = sourceDBI.parameters.TABLES 

    if (sourceParameters.TABLES && !YadamuLibrary.isEmpty(identifierMappings)) {
	  tableFilterList = sourceParameters.TABLES.map((tableName) => { return identifierMappings?.[tableName].hasOwnProperty('tableName') ?  identifierMappings[tableName]?.tableName : tableName})
    }     
   
    sourceDBI.parameters.TABLES = tableFilterList
    let fileWriter = await this.getDatabaseInterface(sourceDatabase,sourceConnection,targetParameters,this.RECREATE_SCHEMA,{})
	
    stepStartTime = performance.now();
    metrics.push(await this.yadamu.pumpData(sourceDBI,fileWriter))
    stepElapsedTime = performance.now() - stepStartTime
    targetDescription = this.getDescription('file',fileWriter)  
    const copyFile1 = fileWriter.FILE
    this.metrics.recordTaskTimings([task.taskName,'EXPORT',fileWriter.MODE,targetConnectionName,sourceDatabase,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
      return;
    }
    
    this.printResults(this.OPERATION_NAME,sourceDescription,targetDescription,stepElapsedTime)
    sourceDescription = targetDescription
    
    // File#1 to Target Schema #2

    sourceParameters  = Object.assign({},parameters)
    sourceParameters.FILE = filename1
    sourceParameters.SOURCE_DIRECTORY = exportDirectory
	sourceParameters.FROM_USER = YadamuConstants.READER_ROLE
	sourceParameters.TABLES = tableFilterList
    
    fileReader = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,null,{})
	
    targetParameters  = Object.assign({},parameters)
	targetParameters.TABLES = tableFilterList
    this.setUser(targetParameters,'TO_USER',targetDatabase, targetSchema2)
    targetDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,this.RECREATE_SCHEMA,{})
    
    stepStartTime = performance.now();
    
    if (test.parser === 'SQL') {
      targetDBI.setParameters({"FILE" : copyFile1})
      metrics.push(await this.yadamu.uploadData(targetDBI))
    }
    else {
      metrics.push(await this.yadamu.pumpData(fileReader,targetDBI))
    }
    stepElapsedTime = performance.now() - stepStartTime 
    targetDescription = this.getDescription(targetConnectionName,targetDBI)
    this.metrics.recordTaskTimings([task.taskName,'EXPORT',targetDBI.MODE,sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
      return;
    }
    
    targetVersion = targetDBI.DATABASE_VERSION
    this.printResults(this.OPERATION_NAME,sourceDescription,targetDescription,stepElapsedTime)
    sourceDescription = targetDescription

    // Target Schema #2 to File#2
    
    sourceParameters  = Object.assign({},parameters)
	sourceParameters.TABLES = tableFilterList
    this.setUser(sourceParameters,'FROM_USER',targetDatabase, targetSchema2)
    sourceDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,sourceParameters,false,{})

    targetParameters  = Object.assign({},parameters)
    targetParameters.FILE = filename2
    targetParameters.TARGET_DIRECTORY = exportDirectory
    targetParameters.TO_USER =  YadamuConstants.WRITER_ROLE
    fileWriter = await this.getDatabaseInterface(sourceDatabase,sourceConnection,targetParameters,null,{})
    
    stepStartTime = performance.now();
    metrics.push(await this.yadamu.pumpData(sourceDBI,fileWriter))
    stepElapsedTime = performance.now() - stepStartTime
    targetDescription = this.getDescription(sourceDatabase,fileWriter)
    const copyFile2 = fileWriter.FILE
    this.metrics.recordTaskTimings([task.taskName,'IMPORT',fileWriter.MODE,targetConnectionName,sourceDatabase,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
      return;
    }

    this.printResults(this.OPERATION_NAME,sourceDescription,targetDescription,stepElapsedTime)
    this.operationsList.push(targetDescription);
    this.fixupMetrics(metrics);   

    const taskElapsedTime =  performance.now() - taskStartTime
    this.metrics.recordTaskTimings([task.taskName,'TASK','',sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(taskElapsedTime)])
   
    // Compare Results
    
    /*
    **
    ** Import #1: Array Size Vs Rows Imported
    ** Import #2: Array Size Vs Rows Imported
    **
    */
    
	/*
	stepStartTime = performance.now();
    this.reportRowCounts(await compareDBI.getRowCounts(targetSchema1),metrics[0],parameters,identifierMappings) 
    */

    const compareDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,{},false)

    const compareConfiguration1 = {
      target             : {
 	    vendor           : targetDatabase
      ,	schema           : compareDBI.getSchema(targetSchema1)
    }
    , parameters         : parameters
    , metrics            : metrics[0]
	, identifierMappings : identifierMappings
    }

    const comparator = await compareDBI.getComparator(compareConfiguration1)
    
    stepStartTime = performance.now()
    await comparator.reportRowCounts(targetConnection) 
    stepElapsedTime = performance.now() - stepStartTime 
    this.metrics.recordTaskTimings([task.taskName,'COUNT','',targetConnectionName,'',YadamuLibrary.stringifyDuration(stepElapsedTime)])

    /*
    stepStartTime = performance.now();
    this.reportRowCounts(await compareDBI.getRowCounts(targetSchema2),metrics[2],parameters,identifierMappings) 
    */

    const compareConfiguration2 = {
      target             : {
 	    vendor           : targetDatabase
      ,	schema           : compareDBI.getSchema(targetSchema2)
    }
    , parameters         : parameters
    , metrics            : metrics[2]
	, identifierMappings : identifierMappings
    }


    stepStartTime = performance.now()
	
	comparator.configuration = compareConfiguration2
    await comparator.reportRowCounts(compareConfiguration2) 
    stepElapsedTime = performance.now() - stepStartTime 
    this.metrics.recordTaskTimings([task.taskName,'COUNT','',targetConnectionName,'',YadamuLibrary.stringifyDuration(stepElapsedTime)])

    // await compareDBI.final();
	    
    // If the content of the export file originated in the target database compare the imported schema with the source schema.

    if (sourceAndTargetMatch) {
		
      const testParameters = {} // parameters ? Object.assign({},parameters) : {}

	  /*
      const compareRules = this.getCompareRules(targetDatabase,targetVersion,targetDatabase,targetVersion,testParameters)
      const compareResults = await this.compareSchemas( targetDatabase, targetDatabase, sourceSchema, targetSchema1, targetConnection, parameters, compareRules, ,false)
      this.printCompareResults('File',targetConnectionName,task.taskName,compareResults)
	  */
	  
	  const compareConfiguration = {
	    source             : {
		  vendor           : targetDatabase
	    , version          : targetVersion
	    , schema           : compareDBI.getSchema(sourceSchema)
  	    }
	  , target             : {
		  vendor           : targetDatabase
	    , version          : targetVersion
	    , schema           : compareDBI.getSchema(targetSchema1)
	    }
      , parameters         : parameters
	  , metrics            : metrics[1]
	  , includeRowCounts   : false
      , identifierMappings : {}
	  }
	
	  
      comparator.configuration = compareConfiguration
      const compareResults = await this.compare(targetConnectionName, targetConnectionName, task.taskName, comparator);	  
      this.metrics.recordFailed(compareResults.failed.length)
      this.metrics.recordTaskTimings([task.taskName,'COMPARE','',targetConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
    }

    // Compare Target Schema #1 and Target Schema #2

    const testParameters = parameters ? Object.assign({},parameters) : {}

    /*
    const compareRules = this.getCompareRules(targetDatabase,targetVersion,targetDatabase,targetVersion,testParameters)
    const compareResults = await this.compareSchemas( targetDatabase, targetDatabase, targetSchema1, targetSchema2, targetConnection, parameters, compareRules, metrics[3],false,{})
    this.printCompareResults('File',targetConnectionName,task.taskName,compareResults)
	*/	

    const compareConfiguration = {
	  source             : {
	    vendor           : targetDatabase
      , version          : targetVersion
      , schema           : compareDBI.getSchema(targetSchema1)
  	  }
	, target             : {
	    vendor           : targetDatabase
	  , version          : targetVersion
	  , schema           : compareDBI.getSchema(targetSchema2)
	  }
    , parameters         : testParameters
	, metrics            : metrics[3]
	, includeRowCounts   : false
    , identifierMappings : {}
	}
	
    comparator.configuration = compareConfiguration
    const compareResults = await this.compare(targetConnectionName, targetConnectionName, task.taskName, comparator);	  
	this.metrics.recordFailed(compareResults.failed.length)
    this.metrics.recordTaskTimings([task.taskName,'COMPARE','',targetConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
    
	compareDBI.final()
	
	/*
    const filecompareRules = this.getCompareRules(targetDatabase,targetVersion,'file',0,testParameters)
    const fileCompare = await this.getDatabaseInterface('file',{},filecompareRules,null,identifierMappings)
    const fileCompareResults = await fileCompare.compareFiles(this.yadamuLoggger, sourceFile, copyFile1, copyFile2, metrics)
	*/
		
    const fileCompareConfiguration = {
	  source             : {
	    vendor           : targetDatabase
      , version          : targetVersion
      , schema           : targetSchema1.database ? targetSchema1 : targetSchema1.schema 
  	  }
	, target             : {
	    vendor           : 'file'
	  , version          : 0
	  , files            : [sourceFile, copyFile1, copyFile2]
	  }
    , parameters         : testParameters
	, metrics            : metrics
	, includeRowCounts   : false
	, identifierMappings : identifierMappings
	}
	
    stepStartTime = performance.now();
	const fileCompareResults = await this.compareFiles(sourceConnectionName,targetConnectionName,task.taskName,fileCompareConfiguration)
    stepElapsedTime = performance.now() - stepStartTime
    this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceDatabase,'',YadamuLibrary.stringifyDuration(stepElapsedTime)])

	const elapsedTime = performance.now() - taskStartTime
    this.yadamu.LOGGER.qa([this.OPERATION_NAME,`${test.parser === 'SQL' ? 'SQL' : 'CLARINET'}`].concat(this.operationsList),`Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
    this.metrics.recordTaskTimings([task.taskName,'TOTAL','','file',targetConnectionName,YadamuLibrary.stringifyDuration(elapsedTime)])
    this.reportTimings(this.metrics.timings)
    return

  }
  
  async import(task,configuration,test,targetConnectionName,parameters) {
    
    const sourceConnectionName = test.source
        
    const sourceConnection = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnection = this.getConnection(configuration.connections,targetConnectionName)

    const sourceDatabase =  YadamuLibrary.getVendorName(sourceConnection);
    const targetDatabase =  YadamuLibrary.getVendorName(targetConnection);

    const targetSchema  = this.getSourceMapping(targetDatabase,task); 
    
    const sourceParameters  = Object.assign({},parameters)

    this.yadamu.MACROS = Object.assign(this.yadamu.MACROS, {
      connection           : targetConnectionName
    , location             : this.configuration.tasks.datasetLocation[task.vendor]
    , mode                 : this.yadamu.MODE
    , operation            : this.OPERATION
    , task                 : task.taskName 
    , vendor               : targetDatabase
    , importPath           : this.IMPORT_PATH
    , exportPath           : this.EXPORT_PATH
    , sourceConnection     : sourceConnectionName 
    , targetconnection     : targetConnectionName
    })

    let importFile

    const filename = task.hasOwnProperty('schemaPrefix') ? `${task.schemaPrefix}_${task.file}` : task.file
    const directory = this.test.parameters?.SOURCE_DIRECTORY || this.configuration.parameters?.SOURCE_DIRECTORY || this.test.parameters?.DIRECTORY || this.configuration.parameters?.DIRECTORY ||''
    const importDirectory = YadamuLibrary.macroSubstitions(path.join(directory,this.IMPORT_PATH),this.yadamu.MACROS)
    
    sourceParameters.SOURCE_DIRECTORY = importDirectory
    if ( sourceDatabase === "file" ) {
      sourceParameters.FILE = filename
      sourceParameters.FROM_USER = 'YADAMU'
    }
    else {
      this.setUser(sourceParameters,'FROM_USER', sourceDatabase, this.getSourceMapping(targetDatabase, task))
    }
    
    const fileReader = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,null)

    const targetParameters  = Object.assign({},parameters)
    this.setUser(targetParameters,'TO_USER',targetDatabase,targetSchema)
    
    if (targetConnection[targetDatabase].hasOwnProperty('directory')) {
      let directory = this.test.parameters?.DIRECTORY || this.configuration.parameters?.DIRECTORY || targetConnection[targetDatabase].directory
      // EXPORT_PATH is the location where the files are to be located in the target 
      directory = YadamuLibrary.macroSubstitions(path.join(directory,this.EXPORT_PATH),this.yadamu.MACROS)
      targetParameters.DIRECTORY = directory
    }
    
    const targetDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,this.RECREATE_SCHEMA)
    const taskStartTime = performance.now();
    let stepStartTime = taskStartTime
    
    let metrics
    if (test.parser === 'SQL') {
      targetDBI.setParameters({"FILE" : filename})
      metrics = await this.yadamu.uploadData(targetDBI)
    }
    else {
      metrics = await this.yadamu.pumpData(fileReader,targetDBI)
    }
    
    let stepElapsedTime = performance.now() - stepStartTime
    let identifierMappings = targetDBI.IDENTIFIER_MAPPINGS
    const sourceDescription = this.getDescription(sourceDatabase,fileReader)  
    const targetDescription = this.getDescription(targetConnectionName, targetDBI)
    this.metrics.recordTaskTimings([task.taskName,this.OPERATION_NAME,targetDBI.MODE,sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    
    if ((this.VERIFY_OPERATION === true) && (this.yadamu.MODE !== 'DDL_ONLY')) {

      const compareDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,false,identifierMappings)

      const compareConfiguration = {
	    target             : {
    	  vendor           : targetDatabase
	    , schema           : compareDBI.getSchema(targetSchema)
	    }
      , parameters         : targetParameters
      , metrics            : metrics
      }

      const comparator = await compareDBI.getComparator(compareConfiguration)
	  stepStartTime = performance.now()
      await comparator.reportRowCounts() 
      stepElapsedTime = performance.now() - stepStartTime 
      this.metrics.recordTaskTimings([task.taskName,'COUNT','',targetConnectionName,'',YadamuLibrary.stringifyDuration(stepElapsedTime)])
	  compareDBI.final()
    }       

    const elapsedTime = performance.now() - taskStartTime
    this.printResults(this.OPERATION_NAME,sourceDescription,targetDescription,elapsedTime)
    this.metrics.recordTaskTimings([task.taskName,'TOTAL','',sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(elapsedTime)])
    this.reportTimings(this.metrics.timings)
    return sourceDatabase === 'file' ? importDirectory : path.dirname(fileReader.CONTROL_FILE_PATH)
      
  }
 
  async export(task,configuration,test,targetConnectionName,parameters) {
      
    const metrics = []
    // this.metrics.newTask();
    
    const sourceConnectionName = test.source
    
    const sourceConnection = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnection = this.getConnection(configuration.connections,targetConnectionName)

    const sourceDatabase =  YadamuLibrary.getVendorName(sourceConnection);
    const targetDatabase =  YadamuLibrary.getVendorName(targetConnection);
    
    const sourceParameters  = Object.assign({},parameters)
    this.setUser(sourceParameters,'FROM_USER', sourceDatabase, this.getSourceMapping(sourceDatabase,task))

    const targetParameters  = Object.assign({},parameters)
    this.setUser(targetParameters,'TO_USER', targetDatabase, this.getSourceMapping(targetDatabase, task))

    this.yadamu.MACROS = Object.assign(this.yadamu.MACROS, {
      connection           : sourceConnectionName
    , location             : this.configuration.tasks.datasetLocation[task.vendor]
    , mode                 : this.yadamu.MODE
    , operation            : this.OPERATION
    , task                 : task.taskName 
    , vendor               : sourceDatabase
    , sourceConnection     : sourceConnectionName 
    , targetconnection     : targetConnectionName
    , importPath           : this.IMPORT_PATH
    , exportPath           : this.EXPORT_PATH
    })

    const sourceDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)

    const filename = task.hasOwnProperty('schemaPrefix') ? `${task.schemaPrefix}_${task.file}` : task.file
    const directory = this.test.parameters?.TARGET_DIRECTORY || this.configuration.parameters?.TARGET_DIRECTORY || this.test.parameters?.DIRECTORY || this.configuration.parameters?.DIRECTORY || ''
    const exportDirectory = YadamuLibrary.macroSubstitions(path.join(directory,this.EXPORT_PATH),this.yadamu.MACROS)    

    if (targetDatabase === "file") {
      targetParameters.FILE =  filename
      targetParameters.TARGET_DIRECTORY = exportDirectory               
      targetParameters.TO_USER = 'YADAMU'
    }
    else {
    }
    
    const fileWriter = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,this.RECREATE_SCHEMA)

    const taskStartTime = performance.now();
    let stepStartTime = taskStartTime;
    metrics.push(await this.yadamu.pumpData(sourceDBI,fileWriter));
    let stepElapsedTime = performance.now() - stepStartTime
    let sourceDescription = this.getDescription(sourceConnectionName,sourceDBI)  
    const targetDescription = this.getDescription(sourceDatabase,fileWriter)  
    this.printResults(this.OPERATION_NAME,sourceDescription,targetDescription,stepElapsedTime)
    this.metrics.recordTaskTimings([task.taskName,this.OPERATION_NAME,fileWriter.MODE,sourceConnectionName,targetDatabase,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
      return;
    }

    const sourceVersion = sourceDBI.DATABASE_VERSION;
        
    if (this.VERIFY_OPERATION === true) {
      sourceDescription = targetDescription
      const sourceParameters  = Object.assign({},parameters)
      if (targetDatabase === "file") { 
        sourceParameters.FILE = filename
        sourceParameters.SOURCE_DIRECTORY = exportDirectory
      } 
      else { 
       sourceParameters.FILE = exportDirectory
       this.setUser(sourceParameters,'FROM_USER',sourceDatabase, task.source)
      }
      const fileReader = await this.getDatabaseInterface(targetDatabase,targetConnection,sourceParameters,null)
      
      const targetParameters  = Object.assign({},parameters)
      const sourceSchema = this.getSourceMapping(sourceDatabase,task)
      const targetSchema = this.getTargetMapping(sourceDatabase,task,'1')
      this.setUser(targetParameters,'TO_USER', sourceDatabase, targetSchema, task.vendor)
      const targetDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,targetParameters,this.RECREATE_SCHEMA)

      stepStartTime = performance.now();
      metrics.push(await this.yadamu.pumpData(fileReader,targetDBI));
      stepElapsedTime = performance.now() - stepStartTime
      this.metrics.recordTaskTimings([task.taskName,'IMPORT',fileReader.MODE,targetDatabase,sourceConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
      if (metrics[metrics.length-1] instanceof Error) {
        this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
        return;
      }
      
      const taskElapsedTime =  performance.now() - taskStartTime
      const identifierMappings = targetDBI.IDENTIFIER_MAPPINGS
      this.metrics.recordTaskTimings([task.taskName,'TASK','',sourceConnectionName,targetDatabase,YadamuLibrary.stringifyDuration(taskElapsedTime)])

      const verifyDescription = this.getDescription(sourceConnectionName,targetDBI) 
      this.printResults('VERIFY',sourceDescription,verifyDescription,taskElapsedTime)
      
      if (this.yadamu.MODE !== 'DDL_ONLY') {

        // Report rows Imported and Compare Schemas..
		
		/*
        const compareRules = this.getCompareRules(sourceDatabase,sourceVersion,targetDatabase,0,parameters)
        const compareResults = await this.compareSchemas( sourceDatabase, sourceDatabase, sourceSchema, targetSchema, sourceConnection, parameters, compareRules, metrics[metrics.length-1], true, identifierMappings)
        this.printCompareResults(sourceConnectionName,targetConnectionName,task.taskName,compareResults)
		*/
		
		const compareDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,parameters,false,identifierMappings)
          
	    const compareConfiguration = {
	      source             : {
		    vendor           : sourceDatabase
	      , version          : sourceVersion
	      , schema           : compareDBI.getSchema(sourceSchema)
	    }
	    , target             : {
    		vendor           : sourceDatabase
	      ,	version          : sourceVersion
	      ,	schema           : compareDBI.getSchema(targetSchema)
	    }
        , parameters         : parameters
	    , metrics            : metrics[metrics.length-1]
	    , includeRowCounts   : true
	    , identifierMappings : identifierMappings
        }

		const comparator = await compareDBI.getComparator(compareConfiguration)
	
   	    const compareResults = await this.compare(sourceConnectionName, targetConnectionName, task.taskName, comparator);
		await compareDBI.final()
        this.metrics.recordFailed(compareResults.failed.length)
        this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
	  }
    }
      
    this.metrics.recordTaskTimings([task.taskName,'TOTAL','',sourceConnectionName,targetDatabase,YadamuLibrary.stringifyDuration(performance.now() - taskStartTime)])
    this.reportTimings(this.metrics.timings)
    return targetDatabase === 'file' ? exportDirectory : path.dirname(fileWriter.CONTROL_FILE_PATH)
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
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
      this.yadamu.LOGGER.writeDirect(`|`)
      row.forEach((col,idx) => {this.yadamu.LOGGER.writeDirect(` ${col.padStart(colSizes[idx])} |`)});
      this.yadamu.LOGGER.writeDirect(`\n`)
    })

    if (summary.length > 1) {
      this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
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
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
      this.yadamu.LOGGER.writeDirect(`|`)
      row.forEach((col,idx) => {this.yadamu.LOGGER.writeDirect(` ${col.padStart(colSizes[idx])} |`)});
      this.yadamu.LOGGER.writeDirect(`\n`)
      if (row[3] === '') {
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
    })


    if (summary.length > 1) {
      this.yadamu.LOGGER.writeDirect('\n') 
    }     
    
  }
  
  printSourceSummary(summary) {
    
    const colSizes = this.calculateColumnSizes(summary)      
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    });
   
    this.yadamu.LOGGER.writeDirect(`\n`)

    summary.filter((r,idx) => {return ((idx ===0) || ((r[2] !== '') && (r[3] === '')))}).forEach((row,idx) => {          
      if (idx < 2) {
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
      this.yadamu.LOGGER.writeDirect(`|`)
      row.forEach((col,idx) => {this.yadamu.LOGGER.writeDirect(` ${col.padStart(colSizes[idx])} |`)});
      this.yadamu.LOGGER.writeDirect(`\n`)
    })

    if (summary.length > 1) {
      this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }     
  }
  
  printFailedSummary() {
     
    if (YadamuLibrary.isEmpty(this.failedOperations)) return
      
    this.yadamu.LOGGER.writeDirect(`\n`)
      
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
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
      this.yadamu.LOGGER.writeDirect(`|`)
      row.forEach((col,idx) => {this.yadamu.LOGGER.writeDirect(` ${col.padStart(colSizes[idx])} |`)});
      this.yadamu.LOGGER.writeDirect(`\n`)
    })

    if (failed.length > 1) {
      this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
    }   
  }

  reportTimings(timings) {
    
    timings.unshift(['Data Set','Step','Mode','Source','Target','Elapsed Time'])
     
    const colSizes = this.calculateColumnSizes(timings)      
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    });

    this.yadamu.LOGGER.writeDirect(`\n`)
   
    timings.forEach((row,idx) => {          
      if (idx < 2) {
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
      this.yadamu.LOGGER.writeDirect(`|`)
      row.forEach((col,idx) => {this.yadamu.LOGGER.writeDirect(` ${col.padStart(colSizes[idx])} |`)});
      this.yadamu.LOGGER.writeDirect(`\n`)
    })

    if (timings.length > 1) {
      this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }     
  }

  async doTests(configuration) {
      
    this.yadamu.LOGGER.qa([`Environemnt`,process.arch,process.platform,process.version],`Running tests`);

    const sourceSummary = []
    const startTime = performance.now()
    
    const summary = [['End Time','Operation','Source','Target','Task','Results','Memory Usage','Elapsed Time']]
    try {    
      for (this.test of configuration.tests) {
        this.metrics.newTest()
        const startTime = performance.now()
        
        // Initialize Configuration Parameters with values from configuration file
        const testParameters = Object.assign({} , configuration.parameters || {})
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
            const targetConnection = configuration.connections[target]
            try {
              for (const task of this.test.tasks) {
                this.metrics.newTask();
                const startTime = performance.now()
                const subTasks = this.getTaskList(configuration,task)
                try {
                  for (const subTask of subTasks) {
                    this.metrics.newSubTask();
                    const startTime = performance.now()
                    try {
                      switch (this.OPERATION_NAME) {
                        case 'EXPORT':
                        case 'UNLOAD':
                          const exportPath = await this.export(subTask,configuration,this.test,target,testParameters)
                          targetDescription = 'file://' + exportPath
                          break;
                        case 'IMPORT':
                        case 'LOAD':
                          const importPath = await this.import(subTask,configuration,this.test,target,testParameters)
                          sourceDescription = 'file://' + importPath
                          break;
                        case 'FILEROUNDTRIP':
                        case 'LOADERROUNDTRIP':
                          await this.fileRoundtrip(subTask,configuration,this.test,target,testParameters)
                          break;
                        case 'FASTIMPORT':
                          await this.fastImport(subTask,configuration,this.test,target,testParameters)
                          break;
                        case 'DBROUNDTRIP':
                          await this.dbRoundtrip(subTask,configuration,this.test,target,testParameters)
                          break;
                        case 'LOSTCONNECTION':
                          await this.dbRoundtrip(subTask,configuration,this.test,target,testParameters)
                          break;
                        case 'COPY':
                          await this.copy(subTask,configuration,this.test,target,testParameters)
                          break;
                      }
                      // Report SubTask metrics and rollup to Task
                      const elapsedTime = performance.now() - startTime;
                      if (subTasks.length > 1 ) {
                        const elapsedTime = performance.now() - startTime;
                        this.yadamu.LOGGER.qa([this.OPERATION_NAME,`SUB-TASK`,sourceDescription,targetDescription,task,subTask.taskName],`${this.metrics.formatMetrics(this.metrics.subTask)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
                        // summary.push([new Date().toISOString(),this.OPERATION_NAME,sourceDescription,targetDescription,Object.values(this.metrics.subTask),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
                      }
                      this.metrics.aggregateTask()
                    } catch (e) {
                      this.yadamu.LOGGER.handleException([this.OPERATION_NAME,`SUB-TASK`,sourceDescription,targetDescription,typeof task === 'string' ? task : `Anonymous`],e);
                      this.metrics.aggregateTask()
                      throw (e)
                    }
                  } 
                  const elapsedTime = performance.now() - startTime;
                  this.yadamu.LOGGER.qa([this.OPERATION_NAME,`TASK`,sourceDescription,targetDescription,typeof task === 'string' ? task : `Anonymous`],`${this.metrics.formatMetrics(this.metrics.task)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
                  summary.push([new Date().toISOString(),this.OPERATION_NAME,sourceDescription,targetDescription,typeof task === 'string' ? task : `Anonymous`,Object.values(this.metrics.task),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
                  this.metrics.aggregateTarget()
                } catch (e) {
                  this.yadamu.LOGGER.handleException([this.OPERATION_NAME,`TASK`,sourceDescription,targetDescription,typeof task === 'string' ? task : `Anonymous`],e);
                  this.metrics.aggregateTarget()
                  throw (e)
                }
              }
              const elapsedTime = performance.now() - startTime;
              this.yadamu.LOGGER.qa([this.OPERATION_NAME,`TARGET`,sourceDescription,targetDescription],`${this.metrics.formatMetrics(this.metrics.target)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);     
              summary.push([new Date().toISOString(),this.OPERATION_NAME,sourceDescription,targetDescription,'',Object.values(this.metrics.target),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
              this.metrics.aggregateTest()
            } catch (e) {
              this.yadamu.LOGGER.handleException([this.OPERATION_NAME,`TARGET`,sourceDescription,targetDescription],e);
              this.metrics.aggregateTest()
              throw e
            }
          }        
          const elapsedTime = performance.now() - startTime;
          this.yadamu.LOGGER.qa([this.OPERATION_NAME,`TEST`,`${sourceDescription}`],`${this.metrics.formatMetrics(this.metrics.test)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
          summary.push([new Date().toISOString(),this.OPERATION_NAME,sourceDescription,'','',Object.values(this.metrics.test),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
          this.metrics.aggregateSuite()
        } catch (e) {
          this.yadamu.LOGGER.handleException([this.OPERATION_NAME,`TEST`,`${sourceDescription}`],e);
          this.metrics.aggregateSuite()
          throw e
        }      
      }
    } catch (e) {
    }

    const elapsedTime = performance.now() - startTime;
    summary.push([new Date().toISOString(),'','','','',Object.values(this.metrics.suite),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
    this.formatArrays(summary)
    this.printFailedSummary()
    this.printSourceSummary(summary)
    this.printTargetSummary(summary)
    // wtf.dump();
    return this.metrics.formatMetrics(this.metrics.suite)
  } 

}
 
export { YadamuQA as default}
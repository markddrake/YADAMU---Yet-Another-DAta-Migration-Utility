"use strict"    

const path = require('path')
const fs = require('fs');
const fsp = require('fs').promises;
const { performance } = require('perf_hooks');
const Transform = require('stream').Transform;

const util = require('util')
const stream = require('stream')
const pipeline = util.promisify(stream.pipeline);

const YadamuTest = require('./yadamuTest.js');
const LoaderDBI = require('../../../YADAMU/loader/node/loaderDBI.js');
const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const JSONParser = require('../../../YADAMU/file/node/jsonParser.js');
const {ConfigurationFileError} = require('../../../YADAMU/common/yadamuException.js');
const YadamuDefaults = require('./yadamuDefaults.json')

class YadamuQA {

  get VERIFY_OPERATION()                          { return this.test.hasOwnProperty('verifyOperation') ? this.test.verifyOperation : this.configuration.hasOwnProperty('verifyOperation') ? this.configuration.verifyOperation : false }
  get RECREATE_SCHEMA()                           { return this.test.hasOwnProperty('recreateSchema') ? this.test.recreateSchema : this.configuration.hasOwnProperty('recreateSchema') ? this.configuration.recreateSchema : false }
  get CREATE_DIRECTORY()                          { return this.test.hasOwnProperty('createDirectory') ? this.test.createDirectory : this.configuration.hasOwnProperty('createDirectory') ? this.configuration.createDirectory : false }
  get DIRECTORY ()                                { return this.test.hasOwnProperty('directory') ? this.test.directory : this.configuration.hasOwnProperty('directory') ? this.configuration.directory : '' }
  get EXPORT_PATH()                               { return this.test.hasOwnProperty('exportPath') ? this.test.exportPath : this.configuration.hasOwnProperty('exportPath') ? this.configuration.exportPath : '' }
  get IMPORT_PATH()                               { return this.test.hasOwnProperty('importPath') ? this.test.importPath : this.configuration.hasOwnProperty('importPath') ? this.configuration.importPath : '' }
  get OPERATION()                                 { return this.test.hasOwnProperty('operation') ? this.test.operation : this.configuration.operation}
  get OPERATION_NAME()                            { return this.OPERATION.toUpperCase() }

  get KILL_CONNECTION()                           { return this.test.hasOwnProperty('kill') ? this.test.kill : this.configuration.hasOwnProperty('kill') ? this.configuration.kill : false }
  get KILL_READER()                               { return this.KILL_CONNECTION.process  === 'READER' }
  get KILL_WRITER()                               { return this.KILL_CONNECTION.process  === 'WRITER' }
  get KILL_WORKER()                               { return this.KILL_CONNECTION.worker }
  get KILL_DELAY()                                { return this.KILL_CONNECTION.delay }

  constructor(configuration,activeConnections) {
      
    this.configuration = configuration
    this.yadamu = new YadamuTest(configuration.parameters,activeConnections)
    this.metrics = this.yadamu.testMetrics;
    
    this.expandedTaskList = []
    this.operationsList = []
    this.failedOperations = {}
    
  }
  
  async initialize() {
	await this.yadamu.initialize()
  }
  
  async getDatabaseInterface(driver,testConnection,testParameters,recreateSchema,identifierMappings) {
    
	let dbi = undefined
	// Start with specified by the test - values specified for the test will override the QA defaults
	const parameters = Object.assign({}, testParameters || {})
    await this.yadamu.reset(parameters);
    
    if (YadamuTest.QA_DRIVER_MAPPINGS.hasOwnProperty(driver)) { 
      const DBI = require(YadamuTest.QA_DRIVER_MAPPINGS[driver]);
      dbi = new DBI(this.yadamu);
    }   
    else {   
      const err = new ConfigurationFileError(`[${this.constructor.name}.getDatabaseInterface()]: Unsupported database vendor "${driver}".`);  
      throw err
    }

    const connectionProperties = typeof testConnection === 'object' ? Object.assign({},testConnection) : testConnection
    dbi.setConnectionProperties(connectionProperties);
    dbi.setParameters(parameters);
    
    this.yadamu.IDENTIFIER_MAPPINGS = identifierMappings 
    
    dbi.setIdentifierMappings(identifierMappings);
    dbi.configureTest(recreateSchema);
 
    if (this.KILL_CONNECTION) {
      this.configureTermination(dbi,parameters)
    }

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
    const schemaInfo = Object.assign({}, operation.source); 
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
  
  getDescription(vendor,connectionName,parameters,key) {
     
    // MsSQL     : "Database"."Owner"
    // Snowflake : "Database"."Schema"
    // Default   : "Schema"

    switch (vendor) {
      case "mssql":
      case "snowflake":     
        return `${connectionName}://"${parameters.YADAMU_DATABASE}"."${parameters[key]}"`
      case "file":      
        return `${connectionName}://"${YadamuLibrary.macroSubstitions(parameters.FILE,this.yadamu.MACROS).split(path.posix.sep).join(path.sep)}"`
      case "loader":        
        const rootPath = (parameters.ROOT_FOLDER && (parameters.ROOT_FOLDER.length > 0)) ? `${YadamuLibrary.macroSubstitions(parameters.ROOT_FOLDER,this.yadamu.MACROS).split(path.posix.sep).join(path.sep)}${path.sep}` : ''
        return `${connectionName}://"${rootPath}${parameters[key]}"`
      default:
        return `${connectionName}://"${parameters[key]}"`
    }
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
        parameters.YADAMU_DATABASE = schemaInfo.database ? schemaInfo.database : database.toUpperCase()
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
  
  getDefaultValue(parameter,defaults,sourceVendor, sourceVersion, targetVendor, targetVersion) {
      
    const parameterDefaults = defaults[parameter]
    const sourceVersionKey = sourceVendor + "#" + sourceVersion;
    const targetVersionKey = targetVendor + "#" + targetVersion;

    switch (true) {
      case ((parameterDefaults[sourceVersionKey] !== undefined) && (parameterDefaults[sourceVersionKey][targetVersionKey] !== undefined)):
        return { [parameter] : parameterDefaults[sourceVersionKey][targetVersionKey]}
      case ((parameterDefaults[sourceVersionKey] !== undefined) && (parameterDefaults[sourceVersionKey][targetVendor] !== undefined)):
        return { [parameter] : parameterDefaults[sourceVersionKey][targetVendor]}
      case ((parameterDefaults[sourceVersionKey] !== undefined) && (parameterDefaults[sourceVersionKey].default !== undefined)):
        return { [parameter] : parameterDefaults[sourceVersionKey].default}
      case ((parameterDefaults[sourceVendor] !== undefined) && (parameterDefaults[sourceVendor][targetVersionKey] !== undefined)):
        return { [parameter] : parameterDefaults[sourceVendor][targetVersionKey]}
      case ((parameterDefaults[sourceVendor] !== undefined) && (parameterDefaults[sourceVendor][targetVendor] !== undefined)):
        return { [parameter] : parameterDefaults[sourceVendor][targetVendor]}
      case ((parameterDefaults[sourceVendor] !== undefined) && (parameterDefaults[sourceVendor].default !== undefined)):
        return { [parameter] : parameterDefaults[sourceVendor].default}
      default:
        return { [parameter] : parameterDefaults.default}
    }
  } 
  
  getCompareRules(sourceVendor,sourceVersion,targetVendor,targetVersion,targetParameters) {
	  
	const compareRules = {
	  MODE             : this.yadamu.MODE
	, TABLES           : targetParameters.TABLES || []
    }

	Object.assign(compareRules,YadamuTest.COMPARE_RULES[targetVendor] || {})
	
	let versionSpecificKey = targetVendor + "#" + targetVersion;
    Object.assign(compareRules, YadamuTest.COMPARE_RULES[versionSpecificKey] || {})
  
	Object.assign(compareRules, this.getDefaultValue('DOUBLE_PRECISION',YadamuTest.COMPARE_RULES,sourceVendor,sourceVersion,targetVendor,targetVersion))
    Object.assign(compareRules, this.getDefaultValue('SPATIAL_PRECISION',YadamuTest.COMPARE_RULES,sourceVendor,sourceVersion,targetVendor,targetVersion))
    Object.assign(compareRules, this.getDefaultValue('ORDERED_JSON',YadamuTest.COMPARE_RULES,sourceVendor,sourceVersion,targetVendor,targetVersion))
	Object.assign(compareRules, this.getDefaultValue('SERIALIZED_JSON',YadamuTest.COMPARE_RULES,sourceVendor,sourceVersion,targetVendor,targetVersion))
	Object.assign(compareRules, this.getDefaultValue('EMPTY_STRING_IS_NULL',YadamuTest.COMPARE_RULES,sourceVendor,sourceVersion,targetVendor,targetVersion))
	Object.assign(compareRules, this.getDefaultValue('OBJECTS_COMPARISSON_RULE',YadamuTest.COMPARE_RULES,sourceVendor,sourceVersion,targetVendor,targetVersion))
    Object.assign(compareRules, this.getDefaultValue('INFINITY_IS_NULL',YadamuTest.COMPARE_RULES,sourceVendor,sourceVersion,targetVendor,targetVersion))
    
	compareRules.INFINITY_IS_NULL = compareRules.INFINITY_IS_NULL && (targetParameters.INFINITY_MANAGEMENT === 'NULLIFY')
	
	if (YadamuTest.COMPARE_RULES.TIMESTAMP_PRECISION[sourceVendor] > YadamuTest.COMPARE_RULES.TIMESTAMP_PRECISION[targetVendor]) {
	  compareRules.TIMESTAMP_PRECISION = YadamuTest.COMPARE_RULES.TIMESTAMP_PRECISION[targetVendor]
    }
	
    compareRules.XML_COMPARISSON_RULE = null
	versionSpecificKey = targetVendor + "#" + targetVersion;
	let xmlCompareRule =  YadamuTest.COMPARE_RULES.XML_COMPARISSON_RULE[versionSpecificKey] ||  YadamuTest.COMPARE_RULES.XML_COMPARISSON_RULE[targetVendor]
	if (xmlCompareRule) {
	  versionSpecificKey = sourceVendor + "#" + sourceVersion;
	  xmlCompareRule = xmlCompareRule[versionSpecificKey] || xmlCompareRule[sourceVendor]
	  if (xmlCompareRule) {
		compareRules.XML_COMPARISSON_RULE = (typeof xmlCompareRule === 'object') ? xmlCompareRule[targetParameters.XML_STORAGE_MODEL] || null : xmlCompareRule
	  }
	}
	    
	// Object.assign(compareRules, testParameters)
    
	if (compareRules.DOUBLE_PRECISION !== null) {
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Double precision limited to ${compareRules.DOUBLE_PRECISION} digits`);
    }
	
    if (compareRules.SPATIAL_PRECISION !== 18) {
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Spatial precision limited to ${compareRules.SPATIAL_PRECISION} digits`);
    }
	
    if (compareRules.EMPTY_STRING_IS_NULL === true) {
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Empty Strings treated as NULL`);
    }
	
    if (compareRules.INFINITY_IS_NULL === true) {
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Infinity, -Infinity and NaN treated as NULL`);
    }
	
    if (compareRules.XML_COMPARISSON_RULE !== null) {
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Target XML storage model: "${targetParameters.XML_STORAGE_MODEL || 'XML'}". Using comparission rule "${compareRules.XML_COMPARISSON_RULE}".`);
    }
	
    if (compareRules.TIMESTAMP_PRECISION) {
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Timestamp precision limited to ${compareRules.TIMESTAMP_PRECISION} digits`);
    }
	
    if (compareRules.ORDERED_JSON === true) {
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Using "Ordered JSON" when performing JSON comparisons.`);
    }

    if (compareRules.SERIALIZED_JSON === true) {
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Target does not support JSON data. Using JSON Parser when comparing JSON values.`);
    }

    if (compareRules.OBJECTS_COMPARISSON_RULE !== null) {
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Comapring Oracle Objects using ${compareRules.OBJECTS_COMPARISSON_RULE}.`);
    }

    return compareRules;
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
  
  async reportRowCounts(rowCounts,metrics,parameters,identifierMappings) {

    rowCounts.forEach((row,idx) => {          
      const unmappedTableName = this.unmapTableName(row[1],identifierMappings)
	  const tableMetrics = metrics[unmappedTableName]
	  const rowCounts = tableMetrics ? [(tableMetrics.rowCount + tableMetrics.rowsSkipped), tableMetrics?.rowsSkipped, tableMetrics?.rowCount ] : [-1,-1,-1]
      row.push(...rowCounts)
    })   

    const colSizes = [32, 48, 14, 14, 14, 14, 14]
      
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    });
   
    this.yadamu.LOGGER.writeDirect(`\n`)

    rowCounts.sort().forEach((row,idx) => {          
      if (idx === 0) {
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamu.LOGGER.writeDirect(`|`
                                     + ` ${'TARGET SCHEMA'.padStart(colSizes[0])} |` 
                                     + ` ${'TABLE_NAME'.padStart(colSizes[1])} |`
                                     + ` ${'ROWS READ'.padStart(colSizes[2])} |`
                                     + ` ${'SKIPPED'.padStart(colSizes[3])} |`
                                     + ` ${'WRITTEN'.padStart(colSizes[4])} |`
                                     + ` ${'COUNT'.padStart(colSizes[5])} |`
                                     + ` ${'DELTA'.padStart(colSizes[6])} |`
                           + '\n');
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamu.LOGGER.writeDirect(`|`
                                     + ` ${row[0].padStart(colSizes[0])} |`
                                     + ` ${row[1].padStart(colSizes[1])} |`
                                     + ` ${row[3].toString().padStart(colSizes[2])} |` 
                                     + ` ${row[4].toString().padStart(colSizes[3])} |` 
                                     + ` ${row[5].toString().padStart(colSizes[4])} |` 
                                     + ` ${row[2].toString().padStart(colSizes[5])} |` 
                                     + ` ${(row[5] - row[2]).toString().padStart(colSizes[6])} |`
                           + '\n');
      }
      else {
        this.yadamu.LOGGER.writeDirect(`|`
                                     + ` ${''.padStart(colSizes[0])} |`
                                     + ` ${row[1].padStart(colSizes[1])} |`
                                     + ` ${row[3].toString().padStart(colSizes[2])} |` 
                                     + ` ${row[4].toString().padStart(colSizes[3])} |` 
                                     + ` ${row[5].toString().padStart(colSizes[4])} |` 
                                     + ` ${row[2].toString().padStart(colSizes[5])} |` 
                                     + ` ${(row[5] - row[2]).toString().padStart(colSizes[6])} |`
                           + '\n');         
      }
    })

    if (rowCounts.length > 0) {
      this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }     

  }

  async compareSchemas(sourceVendor,targetVendor,sourceSchema,targetSchema,connectionProperties,testParameters,rules,metrics,reportRowCounts,identifierMappings) {
      
    const compareDBI = await this.getDatabaseInterface(sourceVendor,connectionProperties,{},false,identifierMappings)
	
    try {
      compareDBI.setParameters(testParameters);
      await compareDBI.initialize();
      this.yadamu.activeConnections.add(compareDBI)
      if (reportRowCounts) {
        this.reportRowCounts(await compareDBI.getRowCounts(targetSchema),metrics,rules,identifierMappings) 
      } 
      const startTime = performance.now();
      const compareResults = await compareDBI.compareSchemas(sourceSchema,targetSchema,rules);
	  
      compareResults.elapsedTime = performance.now() - startTime;
      // this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Elapsed Time: ${YadamuLibrary.stringifyDuration(compareResults.elapsedTime)}s`);

      await compareDBI.releasePrimaryConnection()
      await compareDBI.finalize();
      this.yadamu.activeConnections.delete(compareDBI)

      compareResults.successful.forEach((row,idx) => {          
        // const tableName = (rules.TABLE_MATCHING === 'INSENSITIVE') ? row[2].toLowerCase() : row[2];
        const mappedTableName = metrics.hasOwnProperty(row[2]) ? row[2] : compareDBI.getMappedTableName(row[2],identifierMappings)
        const tableMetrics = (metrics[mappedTableName] === undefined) ? { elapsedTime : 'N/A', throughput : "-1ms" } : metrics[mappedTableName]
        row.push(tableMetrics.elapsedTime,tableMetrics.throughput)
      })     

 	  compareResults.failed = compareResults.failed.filter((row) => {return rules.TABLES === undefined || rules.TABLES.length === 0 || rules.TABLES.includes(row[2])})
    
      if (rules.TABLE_MATCHING === 'INSENSITIVE') {
        Object.keys(metrics).forEach((tableName)  => {
          if (tableName !== tableName.toLowerCase()) {
            metrics[tableName.toLowerCase()] = Object.assign({}, metrics[tableName])
            delete metrics[tableName]
          }
        })
      }
      
      compareResults.isFileBased = (compareDBI instanceof LoaderDBI)
      return compareResults;

    } catch (e) {
      this.yadamu.LOGGER.logException([`COMPARE`],e)
      await compareDBI.abort();
      this.yadamu.activeConnections.delete(compareDBI)
      throw e
    } 
  }
  
  printCompareResults(sourceConnectionName,targetConnectionName,testId,results) {
    
    let colSizes = [12, 32, 32, 48, 14, 14, 14]
    
    let seperatorSize = (colSizes.length *3) - 1
    colSizes.forEach((size)  => {
      seperatorSize += size;
    });
    
    this.yadamu.LOGGER.writeDirect(`\n`)
    results.successful.sort().forEach((row,idx) => {
      if (idx === 0) {
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamu.LOGGER.writeDirect(`|`
                                     + ` ${'RESULT'.padEnd(colSizes[0])} |`
                                     + ` ${'SOURCE SCHEMA'.padStart(colSizes[1])} |`
                                     + ` ${'TARGET SCHEMA'.padStart(colSizes[2])} |` 
                                     + ` ${(results.isFileBased ? 'FILE_NAME' : 'TABLE_NAME').padStart(colSizes[3])} |`
                                     + ` ${(results.isFileBased ? 'BYTES' : 'ROWS').padStart(colSizes[4])} |`
                                     + ` ${'ELAPSED TIME'.padStart(colSizes[5])} |`
                                     + ` ${'THROUGHPUT'.padStart(colSizes[6])} |`
                           + '\n');
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamu.LOGGER.writeDirect(`|`
                                     + ` ${'SUCCESSFUL'.padEnd(colSizes[0])} |`
                                     + ` ${row[0].padStart(colSizes[1])} |`
                                     + ` ${row[1].padStart(colSizes[2])} |`)
      }
      else {
        this.yadamu.LOGGER.writeDirect(`|`
                                     + ` ${''.padEnd(colSizes[0])} |`
                                     + ` ${''.padStart(colSizes[1])} |`
                                     + ` ${''.padStart(colSizes[2])} |` )
      }

      this.yadamu.LOGGER.writeDirect(` ${row[2].padStart(colSizes[3])} |` 
                                   + ` ${row[3].toString().padStart(colSizes[4])} |` 
                                   + ` ${YadamuLibrary.stringifyDuration(parseInt(row[4])).padStart(colSizes[5])} |` 
                                   + ` ${(row[5] === 'NaN/s' ? '' : row[5]).padStart(colSizes[6])} |` 
                         + '\n');
    })
        
    if (results.successful.length > 0) {
      this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }

    colSizes = [12, 32, 32, 48, 14, 14, 32, 32, 72]
      
    seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
   });
   
    const notesIdx = colSizes.length-2
    const lineSize = colSizes[notesIdx+1]
	
    results.failed.forEach((row,idx) => {
      const lines = []
      if ((row[notesIdx] !== null) && ((row[notesIdx].length > lineSize) || (row[notesIdx].indexOf('\r\n') > -1))) {
        const blocks = row[notesIdx].split('\r\n')
        for (const block of blocks) {
          const words = block.split(' ')
          let line = ''
          for (let word of words) {
            if (line.length > 0) {
              word = ' ' + word
            }
            if (line.length + word.length < lineSize) {
              line = line + word
            }
            else {
              if (line.length > 0) {
                // Push Line and start new line
                lines.push(line)
                word = word.substring(1)
              }
              while (word.length > lineSize) {
                if (word[lineSize-1] === '-') {
                  lines.push(word.substring(0,lineSize))
                  word = word.substring(lineSize)
                }  
                else {
                  lines.push(word.substring(0,lineSize-1) + "-")
                  word = word.substring(lineSize-1)
                }
              }
              line = word
            }
          }
          if (line.length > 0) {
             lines.push(line)
          }
        }
        row[7] = lines.shift()
      } 
      if (idx === 0) {
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamu.LOGGER.writeDirect(`|`
                                     + ` ${'RESULT'.padEnd(colSizes[0])} |`
                                     + ` ${'SOURCE SCHEMA'.padStart(colSizes[1])} |`
                                     + ` ${'TARGET SCHEMA'.padStart(colSizes[2])} |` 
                                     + ` ${(results.isFileBased ? 'FILE_NAME' : 'TABLE_NAME').padStart(colSizes[3])} |`
                                     + ` ${(results.isFileBased ? 'SOURCE BYTES' : 'SOURCE ROWS').padStart(colSizes[4])} |`
                                     + ` ${(results.isFileBased ? 'TARGET BYTES' : 'TARGET ROWS').padStart(colSizes[5])} |`
                                     + ` ${(results.isFileBased ? 'SOURCE CHECKSUM' : 'MISSING ROWS').padStart(colSizes[6])} |`
                                     + ` ${(results.isFileBased ? 'TARGET CHECKSUM' : 'EXTRA ROWS').padStart(colSizes[7])} |`
                                     + ` ${'NOTES'.padEnd(colSizes[8])} |`
                           + '\n');
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamu.LOGGER.writeDirect(`|`
                                     + ` ${'FAILED'.padEnd(colSizes[0])} |`
                                     + ` ${row[0].padStart(colSizes[1])} |`
                                     + ` ${row[1].padStart(colSizes[2])} |`) 
      }
      else {
        this.yadamu.LOGGER.writeDirect(`|`
                                     + ` ${''.padEnd(colSizes[0])} |`
                                     + ` ${''.padStart(colSizes[1])} |`
                                     + ` ${''.padStart(colSizes[2])} |`)
      }
                
      this.yadamu.LOGGER.writeDirect(` ${row[2].padStart(colSizes[3])} |` 
                                   + ` ${row[3].toString().padStart(colSizes[4])} |` 
                                   + ` ${row[4].toString().padStart(colSizes[5])} |` 
                                   + ` ${row[5].toString().padStart(colSizes[6])} |` 
                                   + ` ${row[6].toString().padStart(colSizes[7])} |` 
                                   + ` ${(row[7] !== null ? row[7] :  '').padEnd(colSizes[8])} |` 
                         + '\n');

                               
      lines.forEach((line) => {
        this.yadamu.LOGGER.writeDirect(`|`
                                     + ` ${''.padEnd(colSizes[0])} |`
                                     + ` ${''.padStart(colSizes[1])} |`
                                     + ` ${''.padStart(colSizes[2])} |`
                                     + ` ${''.padStart(colSizes[3])} |` 
                                     + ` ${''.padStart(colSizes[4])} |` 
                                     + ` ${''.padStart(colSizes[5])} |` 
                                     + ` ${''.padStart(colSizes[6])} |` 
                                     + ` ${''.padStart(colSizes[7])} |` 
                                     + ` ${line.padEnd(colSizes[8])} |`
                                    + '\n');
      })          

    })
      
    if (results.failed.length > 0) {
      this.failedOperations[sourceConnectionName] = Object.assign({},this.failedOperations[sourceConnectionName])
      this.failedOperations[sourceConnectionName][targetConnectionName] = Object.assign({},this.failedOperations[sourceConnectionName][targetConnectionName])
      results.failed.forEach((failed,idx) => {
        this.failedOperations[sourceConnectionName][targetConnectionName][testId] = Object.assign({},this.failedOperations[sourceConnectionName][targetConnectionName][testId])
        this.failedOperations[sourceConnectionName][targetConnectionName][testId][failed[2]] = results.failed[idx]
      })
      this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
    
  }
    
  propogateTableMatching(sourceDB,targetDB) {
    if (sourceDB.parameters.TABLE_MATCHING && !targetDB.parameters.TABLE_MATCHING) {
      targetDB.parameters.TABLE_MATCHING = sourceDB.parameters.TABLE_MATCHING
    }
    else {
      if (targetDB.parameters.TABLE_MATCING && !sourceDB.parameters.TABLE_MATCHING) {
        sourceDB.parameters.TABLE_MATCHING = targetDB.parameters.TABLE_MATCHING
      }
    }
    return sourceDB.parameters.TABLE_MATCHING
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
  
  setMongoStripID(sourceDatabase,targetDatabase,parameters) {
	 
	if (sourceDatabase === 'mongodb') {
       parameters.MONGO_STRIP_ID = parameters.MONGO_STRIP_ID || false
	}
	
	if (targetDatabase === 'mongodb') {
      parameters.MONGO_STRIP_ID = parameters.MONGO_STRIP_ID || true
	}
  
  }
  
  async dbRoundtrip(task,configuration,test,targetConnectionName,parameters) {
  /*
  **
  ** QA Test 
  **
  ** 1. Recreate targetSchema in sourceDB
  ** 2. Clone sourceSchema in sourceDB to targetSchema in sourceDB using DDL_ONLY mode copy
  ** 3. Recreate targetSchema in targetDB.
  ** 4. Copy sourceSchema in sourceDB to targetSchema in targetDB using DATA_ONLY copy
  ** 5. Copy targetSchema in targetDB to targetSchema in sourceDB using DATA_ONLY copy
  ** 6. Compare sourceSchema in sourceDB with targetSchema in sourceDB
  **
  */
              
    const metrics = []
    const operationsList = []
    let identifierMappings = {}
	let outboundParameters
    
    const sourceConnectionName = test.source

    const sourceConnectionInfo = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnectionInfo = this.getConnection(configuration.connections,targetConnectionName)

    const sourceDatabase =  Object.keys(sourceConnectionInfo)[0];
    const targetDatabase =  Object.keys(targetConnectionInfo)[0];
    this.setMongoStripID(sourceDatabase,targetDatabase,parameters);
	
    const sourceConnection = sourceConnectionInfo[sourceDatabase]
    const targetConnection = targetConnectionInfo[targetDatabase]

    const sourceSchema  = this.getSourceMapping(sourceDatabase,task);
    const targetSchema  = this.getTargetMapping(targetDatabase,task,'1');
    const compareSchema = this.getTargetMapping(sourceDatabase,task,'1');
        
    const sourceParameters  = Object.assign({},parameters)
    const compareParameters = Object.assign({},parameters)
    
    this.setUser(sourceParameters,'FROM_USER',sourceDatabase, sourceSchema)
    this.setUser(compareParameters,'TO_USER', sourceDatabase, compareSchema)
    
    const sourceDescription = this.getDescription(sourceDatabase,sourceConnectionName,sourceParameters,'FROM_USER')  
    const compareDescription = this.getDescription(sourceDatabase,sourceConnectionName,compareParameters,'TO_USER')  

    // If the source connection and target connection reference the same server perform a single DDL_AND_DATA copy between the source schema and target schema

    const taskMode = this.yadamu.MODE;
    
    /*
    **
    ** Two modes of operation
    **
    ** Source Connection and Target Connection are identical: 
    **
    ** Performa a single operation to copies the schema and optionally, data between the source schema and the compare schema in the source database. A DATA_ONLY mode operation is mapped to a DATA_AND_DDL operation.
    ** 
    ** Source Connection and Target Connection are different: 
    ** Fist use a DDL_ONLY mode copy operation to replicate the data structures from the source schema to the compare schemas. Then perform a DATA_ONLY mode copy operation to copy the data from source schema to the 
    ** target schema, followed by a second DATA_ONLY mode copy operation to copy the data from the target schema to the compare schema
    **
    */
    
    const mode = (sourceConnection === targetConnection) ? (taskMode === 'DATA_ONLY' ? 'DDL_AND_DATA' : taskMode ) : 'DDL_ONLY'

    sourceParameters.MODE = mode;
    compareParameters.MODE = mode;


    // Clone the Source Schema to the CompareSchema
    
    this.yadamu.MACROS = Object.assign(this.yadamu.MACROS, {
      connection           : targetConnectionName
    , location             : this.configuration.tasks.datasetLocation[task.vendor]
    , mode                 : this.yadamu.MODE
    , operation            : this.OPERATION
    , task                 : task.taskName 
    , vendor               : targetDatabase
    , sourceConnection     : sourceConnectionName 
    , targetconnection     : targetConnectionName
    })

    // Do not apply IDENTIFIER MAPPINGS during a 'CLONE' operation
	
    delete compareParameters.IDENTIFIER_MAPPING_FILE
    
    let sourceDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)
    let compareDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,compareParameters,this.RECREATE_SCHEMA) 
    const taskStartTime = performance.now();
    let stepStartTime = taskStartTime
    metrics.push(await this.yadamu.pumpData(sourceDBI,compareDBI));

    let stepElapsedTime = performance.now() - stepStartTime
    let sourceVersion = sourceDBI.DB_VERSION;
    let targetVersion = compareDBI.DB_VERSION;

    this.metrics.recordTaskTimings([task.taskName,'COPY',compareParameters.MODE,sourceConnectionName,sourceConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      const compareRules = this.getCompareRules(sourceDatabase,sourceVersion,targetDatabase,targetVersion,compareParameters)
      const compareResults = await this.compareSchemas(sourceDatabase, targetDatabase, sourceSchema, compareSchema, sourceConnection, parameters, compareRules, this.yadamu.metrics, false, undefined)
      this.printCompareResults(sourceConnectionName,targetConnectionName,task.taskName,compareResults)
      this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
	  this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
      return
    }
    
    operationsList.push(sourceDescription)
    operationsList.push(compareDescription)
    
    this.printResults(this.OPERATION_NAME,sourceDescription,compareDescription,stepElapsedTime) 
    
    // Test the current value of MODE. 
    // If MODE is set to DDL_ONLY the first copy operation cloned the structure of the source schema into the compare schema.
    // If MODE = DDL_AND_DATA the first copy completed the operation. 
    
    if (sourceConnection !== targetConnection) {
      
      // Run two more COPY operations. 
      // The first copies the DATA from source to the target. 
      
      operationsList.length = 0
      const targetParameters  = Object.assign({},parameters)
      this.setUser(targetParameters,'TO_USER', targetDatabase, targetSchema)
      sourceParameters.MODE = "DATA_ONLY"
      targetParameters.MODE = "DATA_ONLY"
      compareParameters.MODE = "DATA_ONLY"
      compareParameters.IDENTIFIER_MAPPING_FILE = parameters.IDENTIFIER_MAPPING_FILE

      const targetDescription = this.getDescription(targetDatabase,targetConnectionName,targetParameters,'TO_USER')  
 
      sourceDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)
      let targetDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,this.RECREATE_SCHEMA) 
      this.propogateTableMatching(sourceDBI,targetDBI);
      let stepStartTime = performance.now();
      metrics.push(await this.yadamu.pumpData(sourceDBI,targetDBI));
	  // console.log(targetDBI.parameters)
      stepElapsedTime = performance.now() - stepStartTime
      this.metrics.recordTaskTimings([task.taskName,'COPY',targetDBI.MODE,sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
      if (metrics[metrics.length-1] instanceof Error) {
        const compareRules = this.getCompareRules(sourceDatabase,sourceVersion,targetDatabase,targetVersion,compareParameters)
        const compareResults = await this.compareSchemas( sourceDatabase, targetDatabase, sourceSchema, compareSchema, sourceConnection, parameters, compareRules, this.yadamu.metrics, false, undefined)
        this.printCompareResults(sourceConnectionName,targetConnectionName,task.taskName,compareResults)
        this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
        this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
        return;
      }
      
	  // Preserve the complete set of parameters used to drive the outbound copy operation.
      identifierMappings = targetDBI.getIdentifierMappings()
      targetVersion = targetDBI.DB_VERSION
	  outboundParameters = targetDBI.parameters;

      operationsList.push(sourceDescription)
      operationsList.push(targetDescription)
      this.printResults(this.OPERATION_NAME,sourceDescription,targetDescription,stepElapsedTime)
      
      // The second copies the DATA from source to the target. 
      
      this.setUser(sourceParameters,'FROM_USER', targetDatabase, targetSchema)
      
      // Apply the table mappings used by target of the the first copy operation to the source of the second copy operation.
      
      targetDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,sourceParameters,false,{})
      compareDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,compareParameters,false,this.reverseIdentifierMappings(identifierMappings))
      
      stepStartTime = performance.now();
      metrics.push(await this.yadamu.pumpData(targetDBI,compareDBI));
      stepElapsedTime = performance.now() - stepStartTime
      this.metrics.recordTaskTimings([task.taskName,'COPY',compareDBI.MODE,targetConnectionName,sourceConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
      if (metrics[metrics.length-1] instanceof Error) {
        const compareRules = this.getCompareRules(sourceDatabase,sourceVersion,targetDatabase,targetVersion,compareParameters)
        const compareResults = await this.compareSchemas( sourceDatabase, targetDatabase, sourceSchema, compareSchema, sourceConnection, parameters, compareRules,  this.yadamu.metrics, false, {})
        this.printCompareResults(sourceConnectionName,targetConnectionName,task.taskName,compareResults)
        this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
        this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
        return;
      }
      
      operationsList.push(compareDescription)
      this.printResults(this.OPERATION_NAME,targetDescription,compareDescription,stepElapsedTime)
     
      const taskElapsedTime =  performance.now() - taskStartTime
      this.metrics.recordTaskTimings([task.taskName,'TASK','',sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(taskElapsedTime)])

    }    
    
    this.fixupMetrics(metrics);   
    if (this.yadamu.MODE !== 'DDL_ONLY') {
      const compareRules = this.getCompareRules(sourceDatabase,sourceVersion,targetDatabase,targetVersion,compareParameters)
      const compareResults = await this.compareSchemas( sourceDatabase, targetDatabase, sourceSchema, compareSchema, sourceConnection, parameters, compareRules, metrics[metrics.length-1], false, identifierMappings)
      this.printCompareResults(sourceConnectionName,targetConnectionName,task.taskName,compareResults)
      this.metrics.recordFailed(compareResults.failed.length)
      this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
      const elapsedTime =  performance.now() - taskStartTime
      this.metrics.recordTaskTimings([task.taskName,'TOTAL','',sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(elapsedTime)])
      this.dbRoundtripResults(operationsList,elapsedTime)
    }
    const elapsedTime =  performance.now() - taskStartTime
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

    const sourceConnectionInfo = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnectionInfo = this.getConnection(configuration.connections,targetConnectionName)

    const sourceDatabase =  Object.keys(sourceConnectionInfo)[0];
    const targetDatabase =  Object.keys(targetConnectionInfo)[0];

    const sourceConnection = sourceConnectionInfo[sourceDatabase]
    const targetConnection = targetConnectionInfo[targetDatabase]
    
    /*
    **
    ** Source Directory is the combination of the Connection's directory parameter and the Tasks's directory parameter. 
    ** Target Directory is the combination of the Tests' directory parameter with the Task's directory parameter.
    **
    */

    this.yadamu.MACROS = Object.assign(this.yadamu.MACROS, {
      connection       : targetConnectionName
    , location         : this.configuration.tasks.datasetLocation[task.vendor]
    , mode             : this.yadamu.MODE
    , operation        : this.OPERATION
    , task             : task.taskName 
    , vendor           : targetDatabase
    , sourceConnection : sourceConnectionName 
    , targetconnection : targetConnectionName
    })
    
    const importFile = YadamuLibrary.macroSubstitions(path.join(sourceConnection.directory,this.IMPORT_PATH,(task.hasOwnProperty('schemaPrefix') ? `${task.schemaPrefix}_${task.file}` : task.file)),this.yadamu.MACROS)

    this.yadamu.MACROS = Object.assign(this.yadamu.MACROS,{importPath: this.IMPORT_PATH })
    
    const exportDirectory = YadamuLibrary.macroSubstitions(path.normalize(this.EXPORT_PATH),this.yadamu.MACROS)
    if (this.CREATE_DIRECTORY) {
      await fsp.mkdir(exportDirectory, { recursive: true });
    }
    
    const sourcePathComponents = path.parse(task.file);
    const filename1 = sourcePathComponents.name + ".1" + sourcePathComponents.ext
    const filename2 = sourcePathComponents.name + ".2" + sourcePathComponents.ext
    const file1 = path.join(exportDirectory,filename1)
    const file2 = path.join(exportDirectory,filename2)

    const sourceSchema = this.getSourceMapping(targetDatabase,task)
    const targetSchema1  = this.getTargetMapping(targetDatabase,task,'1')
    const targetSchema2  = this.getTargetMapping(targetDatabase,task,'2');

    const taskStartTime = performance.now();

    // Source File to Target Schema #1
    
    let sourceParameters  = Object.assign({},parameters)
    sourceParameters.FILE = importFile
    let fileReader = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,null)
    let sourceDescription = this.getDescription(sourceDatabase,'file',sourceParameters,'FILE')
    
    let targetParameters  = Object.assign({},parameters)
    this.setUser(targetParameters,'TO_USER',targetDatabase, targetSchema1)
    let targetDescription = this.getDescription(targetDatabase,targetConnectionName,targetParameters,'TO_USER')
    let targetDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,this.RECREATE_SCHEMA)
    const targetDBVersion = targetDBI.DB_VERSION;
    
    let sourceAndTargetMatch
    let stepStartTime = taskStartTime   
    if (test.parser === 'SQL') {
      targetDBI.setParameters({"FILE" : importFile})
      metrics.push(await this.yadamu.uploadData(targetDBI))
      const parser = new YadamuExportParser(this.yadamu.LOGGER)
      await parser.parse(importFile,'systemInformation');
      const systemInformation = parser.getTarget()
      sourceAndTargetMatch = ((targetDBI.DATABASE_VENDOR === systemInformation.vendor) && (targetDBI.DB_VERSION === systemInformation.databaseVersion))
    }
    else {
      let results = await this.yadamu.pumpData(fileReader,targetDBI) 
      metrics.push(results)
      if (!(results instanceof Error)) {
        sourceAndTargetMatch = ((targetDBI.DATABASE_VENDOR === targetDBI.systemInformation.vendor) && (targetDBI.DB_VERSION === targetDBI.systemInformation.databaseVersion))
      }
    }
    let stepElapsedTime = performance.now() - stepStartTime
    
    this.metrics.recordTaskTimings([task.taskName,'IMPORT',targetDBI.MODE,sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
      return;
    }
    
    let targetVersion = targetDBI.DB_VERSION
    let identifierMappings = targetDBI.getIdentifierMappings();
    this.printResults(this.OPERATION_NAME,sourceDescription,targetDescription,stepElapsedTime)
    sourceDescription = targetDescription

    sourceParameters  = Object.assign({},parameters)
    this.setUser(sourceParameters,'FROM_USER',targetDatabase, targetSchema1)
    
    let sourceDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,sourceParameters,null,{})

    targetParameters  = Object.assign({},parameters)
    targetParameters.FILE = file1;
    let fileWriter = await this.getDatabaseInterface(sourceDatabase,sourceConnection,targetParameters,null,{})
    targetDescription = this.getDescription(sourceDatabase,'file',targetParameters,'FILE')  

    stepStartTime = performance.now();
    metrics.push(await this.yadamu.pumpData(sourceDBI,fileWriter))
    
    stepElapsedTime = performance.now() - stepStartTime
    this.metrics.recordTaskTimings([task.taskName,'EXPORT',fileWriter.MODE,targetConnectionName,sourceDatabase,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
      return;
    }
    
    this.printResults(this.OPERATION_NAME,sourceDescription,targetDescription,stepElapsedTime)
    sourceDescription = targetDescription
    
    // File#1 to Target Schema #2

    sourceParameters  = Object.assign({},parameters)
    sourceParameters.FILE = file1;
    fileReader = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,null,{})
    
    targetParameters  = Object.assign({},parameters)
    this.setUser(targetParameters,'TO_USER',targetDatabase, targetSchema2)
    targetDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,this.RECREATE_SCHEMA,{})
    targetDescription = this.getDescription(targetDatabase,targetConnectionName,targetParameters,'TO_USER')
    
    stepStartTime = performance.now();
    
    if (test.parser === 'SQL') {
      targetDBI.setParameters({"FILE" : file1})
      metrics.push(await this.yadamu.uploadData(targetDBI))
    }
    else {
      metrics.push(await this.yadamu.pumpData(fileReader,targetDBI))
    }
    stepElapsedTime = performance.now() - stepStartTime 
    this.metrics.recordTaskTimings([task.taskName,'EXPORT',targetDBI.MODE,sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
      return;
    }
    
    targetVersion = targetDBI.DB_VERSION
    this.printResults(this.OPERATION_NAME,sourceDescription,targetDescription,stepElapsedTime)
    sourceDescription = targetDescription

    // Target Schema #2 to File#2
    
    sourceParameters  = Object.assign({},parameters)
    this.setUser(sourceParameters,'FROM_USER',targetDatabase, targetSchema2)
    sourceDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,sourceParameters,false,{})

    targetParameters  = Object.assign({},parameters)
    targetParameters.FILE = file2;
    fileWriter = await this.getDatabaseInterface(sourceDatabase,sourceConnection,targetParameters,null,{})
    targetDescription = this.getDescription(sourceDatabase,'file',targetParameters,'FILE')

    stepStartTime = performance.now();
    metrics.push(await this.yadamu.pumpData(sourceDBI,fileWriter))
    stepElapsedTime = performance.now() - stepStartTime
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
    
    let compareDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,{},false)
    await compareDBI.initialize();
    this.yadamu.activeConnections.add(compareDBI);
	
    stepStartTime = performance.now();
    this.reportRowCounts(await compareDBI.getRowCounts(targetSchema1),metrics[0],parameters,identifierMappings) 
    stepElapsedTime = performance.now() - stepStartTime 
    this.metrics.recordTaskTimings([task.taskName,'COUNT','',targetConnectionName,'',YadamuLibrary.stringifyDuration(stepElapsedTime)])

    stepStartTime = performance.now();
    this.reportRowCounts(await compareDBI.getRowCounts(targetSchema2),metrics[2],parameters,identifierMappings) 
    stepElapsedTime = performance.now() - stepStartTime 
    this.metrics.recordTaskTimings([task.taskName,'COUNT','',targetConnectionName,'',YadamuLibrary.stringifyDuration(stepElapsedTime)])
   
    await compareDBI.releasePrimaryConnection()
    await compareDBI.finalize();
	this.yadamu.activeConnections.delete(compareDBI);
	
    
    // If the content of the export file originated in the target database compare the imported schema with the source schema.

    // if (task.source.directory.indexOf(targetConnectionName) === 0) {
        
    if (sourceAndTargetMatch) {
      const testParameters = {} // parameters ? Object.assign({},parameters) : {}
      const compareRules = this.getCompareRules(targetDatabase,targetVersion,targetDatabase,targetVersion,testParameters)
      const compareResults = await this.compareSchemas( targetDatabase, targetDatabase, sourceSchema, targetSchema1, targetConnection, parameters, compareRules, metrics[1],false)
      this.metrics.recordFailed(compareResults.failed.length)
      this.metrics.recordTaskTimings([task.taskName,'COMPARE','',targetConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
      this.printCompareResults('File',targetConnectionName,task.taskName,compareResults)
    }

    // Compare Target Schema #1 and Target Schema #2

    const testParameters = parameters ? Object.assign({},parameters) : {}
    const compareRules = this.getCompareRules(targetDatabase,targetVersion,targetDatabase,targetVersion,testParameters)
    const compareResults = await this.compareSchemas( targetDatabase, targetDatabase, targetSchema1, targetSchema2, targetConnection, parameters, compareRules, metrics[3],false,{})
    this.metrics.recordFailed(compareResults.failed.length)
    this.metrics.recordTaskTimings([task.taskName,'COMPARE','',targetConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
    this.printCompareResults('File',targetConnectionName,task.taskName,compareResults)
    
    stepStartTime = performance.now();
    const filecompareRules = this.getCompareRules(targetDatabase,targetVersion,'file',0,testParameters)
    const fileCompare = await this.getDatabaseInterface('file',{},filecompareRules,null,identifierMappings)
    const fileCompareResults = await fileCompare.compareFiles(this.yadamuLoggger, importFile, file1, file2, metrics)

    if (fileCompareResults.length > 0) {
      this.failedOperations[sourceConnectionName] = Object.assign({},this.failedOperations[sourceConnectionName])
      this.failedOperations[sourceConnectionName][targetConnectionName] = Object.assign({},this.failedOperations[sourceConnectionName][targetConnectionName])
      fileCompareResults.forEach((failed,idx) => {
        this.failedOperations[sourceConnectionName][targetConnectionName][task.taskName] = Object.assign({},this.failedOperations[sourceConnectionName][targetConnectionName][task.taskName])
        this.failedOperations[sourceConnectionName][targetConnectionName][task.taskName][failed[2]] = failed
      })
    }
    
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
        
    const sourceConnectionInfo = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnectionInfo = this.getConnection(configuration.connections,targetConnectionName)

    const sourceDatabase =  Object.keys(sourceConnectionInfo)[0];
    const targetDatabase =  Object.keys(targetConnectionInfo)[0];

    const sourceConnection = sourceConnectionInfo[sourceDatabase]
    const targetConnection = targetConnectionInfo[targetDatabase]

    const targetSchema  = this.getSourceMapping(targetDatabase,task); 
    
    const sourceParameters  = Object.assign({},parameters)

    this.yadamu.MACROS = Object.assign(this.yadamu.MACROS, {
      connection           : targetConnectionName
    , location             : this.configuration.tasks.datasetLocation[task.vendor]
    , mode                 : this.yadamu.MODE
    , operation            : this.OPERATION
    , task                 : task.taskName 
    , vendor               : targetDatabase
    , sourceConnection     : sourceConnectionName 
    , targetconnection     : targetConnectionName
    })

    let importFile
    let sourceDescription
    if ( sourceDatabase === "file" ) {
      importFile = YadamuLibrary.macroSubstitions(path.join(sourceConnection.directory,this.IMPORT_PATH,(task.hasOwnProperty('schemaPrefix') ? `${task.schemaPrefix}_${task.file}` : task.file)),this.yadamu.MACROS)
      sourceParameters.FILE = importFile
      sourceDescription = this.getDescription(sourceDatabase,sourceDatabase, sourceParameters,'FILE')  
    }
    else {
      this.setUser(sourceParameters,'FROM_USER', sourceDatabase, this.getSourceMapping(targetDatabase, task))
      sourceDescription = this.getDescription(sourceDatabase,sourceDatabase, sourceParameters,'FROM_USER')
    }

    const fileReader = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,null)

    const targetParameters  = Object.assign({},parameters)
    this.setUser(targetParameters,'TO_USER',targetDatabase,targetSchema)
    
    const targetDescription = this.getDescription(targetDatabase,targetConnectionName, targetParameters,'TO_USER')
    
    const targetDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,this.RECREATE_SCHEMA)
    const taskStartTime = performance.now();
    let stepStartTime = taskStartTime
    
    let metrics
    if (test.parser === 'SQL') {
      targetDBI.setParameters({"FILE" : importFile})
      metrics = await this.yadamu.uploadData(targetDBI)
    }
    else {
      metrics = await this.yadamu.pumpData(fileReader,targetDBI)
    }
    
    let stepElapsedTime = performance.now() - stepStartTime
    this.metrics.recordTaskTimings([task.taskName,this.OPERATION_NAME,targetDBI.MODE,sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    
    if ((this.VERIFY_OPERATION === true) && (this.yadamu.MODE !== 'DDL_ONLY')) {
      const compareDBI = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,false)
      await compareDBI.initialize();
      stepStartTime = performance.now()
      this.reportRowCounts(await compareDBI.getRowCounts(targetSchema),metrics,parameters) 
      stepElapsedTime = performance.now() - stepStartTime 
      await compareDBI.releasePrimaryConnection()
      await compareDBI.finalize();
      this.metrics.recordTaskTimings([task.taskName,'COUNT','',targetConnectionName,'',YadamuLibrary.stringifyDuration(stepElapsedTime)])
    }       

    const elapsedTime = performance.now() - taskStartTime
    this.printResults(this.OPERATION_NAME,sourceDescription,targetDescription,elapsedTime)
    this.metrics.recordTaskTimings([task.taskName,'TOTAL','',sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(elapsedTime)])
    this.reportTimings(this.metrics.timings)
    return path.dirname(sourceDatabase === 'file' ? sourceParameters.FILE : path.dirname(fileReader.controlFilePath))
      
  }
 
  async export(task,configuration,test,targetConnectionName,parameters) {
      
    const metrics = []
    // this.metrics.newTask();
    
    const sourceConnectionName = test.source
    
    const sourceConnectionInfo = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnectionInfo = this.getConnection(configuration.connections,targetConnectionName)

    const sourceDatabase =  Object.keys(sourceConnectionInfo)[0];
    const targetDatabase =  Object.keys(targetConnectionInfo)[0];
    
    const sourceConnection = sourceConnectionInfo[sourceDatabase]
    const targetConnection = targetConnectionInfo[targetDatabase]
 
    const sourceParameters  = Object.assign({},parameters)
    this.setUser(sourceParameters,'FROM_USER', sourceDatabase, this.getSourceMapping(sourceDatabase,task))
    const sourceDBI = await this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)
    
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
    })

    let sourceDescription = this.getDescription(sourceDatabase,sourceConnectionName,sourceParameters,'FROM_USER')  

    let exportFile
    let targetDescription
    if ( targetDatabase === "file" ) {
      const exportDirectory = YadamuLibrary.macroSubstitions(path.join(targetConnection.directory,this.EXPORT_PATH),this.yadamu.MACROS)
      if (this.CREATE_DIRECTORY) {
        await fsp.mkdir(exportDirectory, { recursive: true });
      }
      exportFile = path.join(exportDirectory,(task.hasOwnProperty('schemaPrefix') ? `${task.schemaPrefix}_${task.file}` : task.file))
      targetParameters.FILE = exportFile
      targetDescription = this.getDescription(targetDatabase,targetDatabase,targetParameters,'FILE')
    }
    else {
      targetDescription = this.getDescription(targetDatabase,targetDatabase,targetParameters,'FROM_USER')
    }
    
    const fileWriter = await this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,null)

    const taskStartTime = performance.now();
    let stepStartTime = taskStartTime;
    metrics.push(await this.yadamu.pumpData(sourceDBI,fileWriter));
    let stepElapsedTime = performance.now() - stepStartTime
    this.printResults(this.OPERATION_NAME,sourceDescription,targetDescription,stepElapsedTime)
    this.metrics.recordTaskTimings([task.taskName,this.OPERATION_NAME,fileWriter.MODE,sourceConnectionName,targetDatabase,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      this.metrics.recordError(this.yadamu.LOGGER.getMetrics(true))
      return;
    }

    const sourceVersion = sourceDBI.DB_VERSION;
        
    if (this.VERIFY_OPERATION === true) {
      sourceDescription = targetDescription
      const sourceParameters  = Object.assign({},parameters)
      if (targetDatabase === "file") { 
        sourceParameters.FILE = exportFile
      } 
      else { 
       sourceParameters.FILE = exportDirectory
       this.setUser(sourceParameters,'FROM_USER',sourceDatabase, task.source)
      }
      
      const fileReader = await this.getDatabaseInterface(targetDatabase,sourceConnection,sourceParameters,null)
      
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
      const identifierMappings = targetDBI.getIdentifierMappings();
      this.metrics.recordTaskTimings([task.taskName,'TASK','',sourceConnectionName,targetDatabase,YadamuLibrary.stringifyDuration(taskElapsedTime)])

      const verifyDescription = this.getDescription(sourceDatabase,sourceConnectionName,targetParameters,'TO_USER') 
      this.printResults('VERIFY',sourceDescription,verifyDescription,taskElapsedTime)
      
      if (this.yadamu.MODE !== 'DDL_ONLY') {
        // Report rows Imported and Compare Schemas..
        const compareRules = this.getCompareRules(sourceDatabase,sourceVersion,targetDatabase,0,parameters)
        const compareResults = await this.compareSchemas( sourceDatabase, sourceDatabase, sourceSchema, targetSchema, sourceConnection, parameters, compareRules, metrics[metrics.length-1], true, identifierMappings)
        this.metrics.recordFailed(compareResults.failed.length)
        this.metrics.recordTaskTimings([task.taskName,'COMPARE','',sourceConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
        this.printCompareResults(sourceConnectionName,targetConnectionName,task.taskName,compareResults)
      }
    }
      
    this.metrics.recordTaskTimings([task.taskName,'TOTAL','',sourceConnectionName,targetDatabase,YadamuLibrary.stringifyDuration(performance.now() - taskStartTime)])
    this.reportTimings(this.metrics.timings)
    return path.dirname(targetDatabase === 'file' ? targetParameters.FILE : path.dirname(fileWriter.controlFilePath))
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
  
  formatArray(v) {
    
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
                        case 'DBROUNDTRIP':
                          await this.dbRoundtrip(subTask,configuration,this.test,target,testParameters)
                          break;
                        case 'LOSTCONNECTION':
                          await this.dbRoundtrip(subTask,configuration,this.test,target,testParameters)
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
          this.yadamu.LOGGER.handleException([[this.OPERATION_NAME],`TEST`,`${sourceDescription}`],e);
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
    return this.metrics.formatMetrics(this.metrics.suite)
  } 

  configureTermination(dbi,parameters) {

    if ((this.KILL_READER && parameters.FROM_USER) || (this.KILL_WRITER && parameters.TO_USER)) {
      dbi.configureTermination(this.KILL_CONNECTION)
    }       
  }	

}

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
      
  async _transform(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
        case this.targetName:
          this.jsonParser.pause()
          this.jsonParser.end();
          this.target = obj[this.targetName]
          this.destroy()
        default:
          callback();
      }
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._transform()`],e);
      callback(e);
    }
  }
  
  getTarget() {
    return this.target
  }
}
 
module.exports = YadamuQA
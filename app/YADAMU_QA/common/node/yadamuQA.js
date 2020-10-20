"use strict"    

const path = require('path')
const fs = require('fs');
const { performance } = require('perf_hooks');


const YadamuTest = require('./yadamuTest.js');
const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const {ConfigurationFileError} = require('../../../YADAMU/common/yadamuError.js');

class YadamuQA {
	
  constructor(configuration) {
	this.configuration = configuration

    this.yadamu = new YadamuTest('TEST');

	this.expandedTaskList = []
	this.operationsList = []
	this.failedOperations = {}
  }
  
  getDatabaseInterface(driver,testConnection,testParameters,recreateSchema,tableMappings) {
    
    let dbi = undefined
    const parameters = testParameters ? Object.assign({},testParameters) : {}
	this.yadamu.reset(parameters);
    
    if (YadamuTest.YADAMU_DRIVERS.hasOwnProperty(driver)) { 
	  const DBI = require(YadamuTest.YADAMU_DRIVERS[driver]);
	  dbi = new DBI(this.yadamu);
    }	
    else {   
      const err = new ConfigurationFileError(`[${this.constructor.name}.getDatabaseInterface()]: Unsupported database vendor "${driver}".`);  
	  throw err
    }

	
    const connectionProperties = typeof testConnection === 'object' ? Object.assign({},testConnection) : testConnection
    dbi.setConnectionProperties(connectionProperties);
    dbi.setParameters(parameters);
	dbi.setTableMappings(tableMappings);
	dbi.configureTest(recreateSchema);
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
 
  getSourceSchema(vendor,schemaInfo,operation) {

    /*
	** 
	** Transformations
	** 
	** SchemaInfo              Vendor      Owner  Result
    ** ===============================================================================
    ** [database,owner]:       MsSQL              schemaInfo
	** [database,owner]:       Snowflake          {database : schemaInfo.database, schema: schemaInfo.owner}
	** [database,owner]:       Mongo       dbo    {databsase: `"${operation.schemaPrefix}_${database}"`}
	** [database,owner]:       Mongo       other  {databsase: `"${operation.schemaPrefix}_${owner}"`}
	** [database,owner]:       other       dbo    {schema: `"${operation.schemaPrefix}_${database}"`}
	** [database,owner]:       other       other  {schema: `"${operation.schemaPrefix}_${owner}"`}
	** [database,schema]:      other              TBD 
	** [database]:             MsQL               {database: `"${operation.schemaPrefix}_${database}"`, owner: "dbo"}
	** [database]:             Snowflake          TBD 
	** [database]:             Mongo              schemaInfo
	** [database]:             other              {schema : `schemaInfo.database}
	** [Schema]:               MsSQL       n/a    {database: `"${operation.schemaPrefix}_${schema}"`, owner: "dbo"}
	** [Schema]:               Snowflake   n/a    TBD
	** [Schema]:               Mongo       n/a    {database : schemaInfo.schema}
	** [Schema]:               other       n/a    schemaInfo
	** 
	*/

	schemaInfo = Object.assign({}, schemaInfo);
		
    switch (true) {
	  case (schemaInfo.hasOwnProperty('database') && schemaInfo.hasOwnProperty('owner')) : 
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
	  case (schemaInfo.hasOwnProperty('database') && schemaInfo.hasOwnProperty('schema')) :
	    // Snowflake style schema informaton
		return schemaInfo;
	    break;
      case (schemaInfo.hasOwnProperty('database') && !schemaInfo.hasOwnProperty('schema')):
	    // Mongo 
		switch (vendor) {
		  case 'mssql':
        	const database = this.getPrefixedSchema(operation.schemaPrefix,schemaInfo.database) 
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
        	const database = this.getPrefixedSchema(operation.schemaPrefix,schemaInfo.schema) 
		    return {"database": database, "owner" : "dbo"}
			break;
		  case 'snowflake':
		    return {database : operation.rdbms.toUpperCase(), schema : schemaInfo.schema}
			break;
		  case 'mongo':
		    return {"database" : schemaInfo.schema};
			break;
	      default:
		    return schemaInfo
		}
	}
  }
  
  getTargetSchema(vendor,schemaInfo,operation) {
      
    /*
	** 
	** Transformations
	** 
	** SchemaInfo              Vendor      Owner  Result
    ** ===============================================================================
    ** [database,owner]:       MsSQL       dbo    {database: `"${operation.schemaPrefix}_${owner}"`, owner: "dbo"|
    ** [database,owner]:       MsSQL       other  {database: `"${operation.schemaPrefix}_${database}"`, owner: "dbo"|
	** [database,owner]:       Snowflake          {database : schemaInfo.database, schema: schemaInfo.owner}
	** [database,owner]:       Mongo       dbo    {databsase: `"${operation.schemaPrefix}_${database}"`}
	** [database,owner]:       Mongo       other  {databsase: `"${operation.schemaPrefix}_${owner}"`}
	** [database,owner]:       other       dbo    {schema: `"${operation.schemaPrefix}_${database}"`}
	** [database,owner]:       other       other  {schema: `"${operation.schemaPrefix}_${owner}"`}
	** [database,schema]:      other              TBD 
	** [database]:             MsQL               {database: `"${operation.schemaPrefix}_${database}"`, owner: "dbo"}
	** [database]:             Snowflake          TBD 
	** [database]:             Mongo              schemaInfo
	** [database]:             other              {schema : `schemaInfo.database}
	** [Schema]:               MsSQL       n/a    {database: `"${operation.schemaPrefix}_${schema}"`, owner: "dbo"}
	** [Schema]:               Snowflake   n/a    TBD
	** [Schema]:               Mongo       n/a    {database : schemaInfo.schema}
	** [Schema]:               other       n/a    schemaInfo
	** 
	*/
    
	schemaInfo = Object.assign({}, schemaInfo);
    switch (true) {
	  case (schemaInfo.hasOwnProperty('database') && schemaInfo.hasOwnProperty('owner')) : 
	    // MsSQL style schema information
		let schema
		let database
		switch (vendor) {
	      // Cannot drop a schema in MsSQL. Map database.schema to prefix+schema.dbo
		  case 'mssql':
            database = schemaInfo.owner === 'dbo' ? schemaInfo.database : schemaInfo.owner
	    	database = this.getPrefixedSchema(operation.schemaPrefix,database) 
		    return {"database" : database, "owner" : "dbo"}
			break;
		  case 'snowflake':
		    return {database: schemaInfo.database, schema: schemaInfo.owner};
			break;
		  case 'mongo':
        	database = schemaInfo.owner === 'dbo' ? schemaInfo.database : schemaInfo.owner
	    	database = this.getPrefixedSchema(operation.schemaPrefix,database) 
		    return { "database" : database }
			break;
	      default:
        	schema = schemaInfo.owner === 'dbo' ? schemaInfo.database : schemaInfo.owner
	    	schema = this.getPrefixedSchema(operation.schemaPrefix,schema) 
		    return { "schema" : schema }
		}
		break;
	  case (schemaInfo.hasOwnProperty('database') && schemaInfo.hasOwnProperty('schema')) :
	    // Snowflake style schema informaton
		return schemaInfo;
	    break;
      case (schemaInfo.hasOwnProperty('database') && !schemaInfo.hasOwnProperty('schema')):
	    // Mongo 
		switch (vendor) {
		  case 'mssql':
        	const database = this.getPrefixedSchema(operation.schemaPrefix,schemaInfo.database) 
		    return {"database": database, "owner" : "dbo"}
			break;
		  case 'snowflake':
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
        	const database = this.getPrefixedSchema(operation.schemaPrefix,schemaInfo.schema) 
		    return {"database": database, "owner" : "dbo"}
			break;
		  case 'snowflake':
		    return {database : schemaInfo.database ? schemaInfo.database.toUpperCase() : operation.rdbms.toUpperCase(), schema : schemaInfo.schema}
			break;
		  case 'mongo':
		    return {"database" : schemaInfo.schema};
			break;
	      default:
		    return schemaInfo
		}
	} 
  }
  
  getDescription(vendor,schemaInfo,schemaPrefix) {
	  
	// MsSQL     : "Database"."Owner"
	// Snowflake : "Database"."Schema"
	// Default   : "Schema"

	switch (vendor) {
	  case "mssql":
	    return schemaInfo.database === undefined ? `"${schemaInfo.schema}"."dbo"` : `"${schemaInfo.database}"."${schemaInfo.owner}"` 
	  case "snowflake":	    
	    return schemaInfo.database === undefined ? `"${schemaInfo.schema}"."public"` : `"${schemaInfo.database}"."${schemaInfo.schema}"` 
	  case "file":
	    return `"${schemaInfo.file}"`;
      default:
	    return schemaInfo.database === undefined ? schemaInfo.schema : (schemaInfo.owner === 'dbo' ? `"${schemaInfo.database}"` : `"${this.getPrefixedSchema(schemaPrefix,schemaInfo.owner)}"` )
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
	    return `file://"${parameters.FILE}"`;
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
	    parameters.YADAMU_DATABASE = schemaInfo.database ? schemaInfo.database : database
		parameters[key] = schemaInfo.schema ? schemaInfo.schema : schemaInfo.owner
		// ### Force Database name to uppercase otherwise queries against INFORMATION_SCHEMA fail
	    parameters.YADAMU_DATABASE = parameters.YADAMU_DATABASE.toUpperCase();
	    break;
      default:
	    parameters[key] = schemaInfo.schema !== undefined ? schemaInfo.schema : (schemaInfo.owner === 'dbo' ? schemaInfo.database : schemaInfo.owner)
    }
	return parameters

  }
  
  fixupMetrics(metrics) {
    // If operations failed metrics may be undefined. If so replace with empty object to prevent errors when reporting
    metrics.forEach((t,i) => {
      if ((t === undefined) || (t === null)) {
        metrics[i] = {}
      }
    })
  }  
  
  formatMetrics(metrics) {
	return `Errors: ${metrics.errors}. Warnings: ${metrics.warnings}. Failed: ${metrics.failed}.`
  }

  adjustMetrics(metrics) {
	  
	/*
    ** If the copy operations generated errors or warnings the summary message generated at the end of the operation causes the metrics maintained by logger to be incremented by 1.
	** Adjust the metrics obtained from the logger to account for this.
	**
	*/
	
	if (metrics.errors > 1) {
	  metrics.errors--
	}
    else {
	  if (metrics.warnings > 1) {
		metrics.warnings--
	  }
	}
	return metrics;
  }
  
  printResults(operation,sourceDescription,targetDescription,elapsedTime) {
	
    this.operationsList.push(sourceDescription);

	const metrics = this.yadamu.LOGGER.getMetrics(true)
	const results = this.adjustMetrics(Object.assign({},metrics))
    // this.resetMetrics(metrics)	

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
      this.yadamu.LOGGER.qa([operation,`COPY`,`${sourceDescription}`,`${targetDescription}`],`${this.formatMetrics(results)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
    }

	return results;
  
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
  
  getCompareParameters(sourceVendor,sourceVersion,targetVendor,targetVersion,testParameters) {
		
    const compareParameters = Object.assign({}, testParameters)
    
    Object.assign(compareParameters, this.getDefaultValue('SPATIAL_PRECISION',YadamuTest.TEST_DEFAULTS,sourceVendor,sourceVersion,targetVendor,targetVersion,testParameters))
    Object.assign(compareParameters, this.getDefaultValue('XSL_TRANSFORMATION',YadamuTest.TEST_DEFAULTS,sourceVendor,sourceVersion,targetVendor,targetVersion,testParameters))
    Object.assign(compareParameters, this.getDefaultValue('ORDERED_JSON',YadamuTest.TEST_DEFAULTS,sourceVendor,sourceVersion,targetVendor,targetVersion,testParameters))
   
    let versionSpecificKey = sourceVendor + "#" + sourceVersion;
    Object.assign(compareParameters, YadamuTest.TEST_DEFAULTS[sourceVendor])
    Object.assign(compareParameters, YadamuTest.TEST_DEFAULTS[versionSpecificKey] ? YadamuTest.TEST_DEFAULTS[versionSpecificKey] : {})
 
    versionSpecificKey = targetVendor + "#" + targetVersion;
   
    Object.assign(compareParameters, YadamuTest.TEST_DEFAULTS[targetVendor])
    Object.assign(compareParameters, YadamuTest.TEST_DEFAULTS[versionSpecificKey] ? YadamuTest.TEST_DEFAULTS[versionSpecificKey] : {})

    return compareParameters;
  }
  
  async reportRowCounts(rowCounts,metrics,parameters,tableMappings) {
	  	  
    rowCounts.forEach((row,idx) => {          
	  let tableName = row[1]
	  if (tableMappings && tableMappings.hasOwnProperty(tableName)) {
		tableName = tableMappings[tableName].tableName
      }
      tableName = (parameters.TABLE_MATCHING === 'INSENSITIVE') ? tableName.toLowerCase() : tableName;
      const tableMetrics = (metrics[tableName] === undefined) ? { rowCount : -1 } : metrics[tableName]
      row.push(tableMetrics.rowCount)
    })	 

    const colSizes = [32, 48, 14, 14, 14]
      
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
                                     + ` ${'ROWS'.padStart(colSizes[2])} |`
                                     + ` ${'ROWS IMPORTED'.padStart(colSizes[3])} |`
                                     + ` ${'DELTA'.padStart(colSizes[4])} |`
                           + '\n');
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamu.LOGGER.writeDirect(`|`
                                     + ` ${row[0].padStart(colSizes[0])} |`
                                     + ` ${row[1].padStart(colSizes[1])} |`
                                     + ` ${row[2].toString().padStart(colSizes[2])} |` 
                                     + ` ${row[3].toString().padStart(colSizes[3])} |` 
                                     + ` ${(row[3] - row[2]).toString().padStart(colSizes[4])} |`
                           + '\n');
      }
      else {
        this.yadamu.LOGGER.writeDirect(`|`
                                     + ` ${''.padStart(colSizes[0])} |`
                                     + ` ${row[1].padStart(colSizes[1])} |`
                                     + ` ${row[2].toString().padStart(colSizes[2])} |` 
                                     + ` ${row[3].toString().padStart(colSizes[3])} |` 
                                     + ` ${(row[3] - row[2]).toString().padStart(colSizes[4])} |`
                           + '\n');         
      }
    })

    if (rowCounts.length > 0) {
      this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }     

  }

  async compareSchemas(sourceVendor,connectionProperties,parameters,sourceSchema,targetVendor,targetSchema,metrics,reportRowCounts,tableMappings,stepMetrics) {
	  
	// Make sure compare operations do not get terminated during KILL tests  
	  
    delete parameters.KILL_READER_AFTER
	delete parameters.KILL_WRITER_AFTER

    const compareDBI = this.getDatabaseInterface(sourceVendor,connectionProperties,parameters,false,tableMappings)

    if (compareDBI.parameters.SPATIAL_PRECISION !== 18) {
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Spatial precision limited to ${compareDBI.parameters.SPATIAL_PRECISION} digits`);
    }
    if (compareDBI.parameters.EMPTY_STRING_IS_NULL === true) {
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Empty Strings treated as NULL`);
    }
    if (compareDBI.parameters.STRIP_XML_DECLARATION === true) {
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`XML Declartion ignored when comparing XML content`);
    }
    if (compareDBI.parameters.TIMESTAMP_PRECISION && (compareDBI.parameters.TIMESTAMP_PRECISION < 9)){
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Timestamp precision limited to ${compareDBI.parameters.TIMESTAMP_PRECISION} digits`);
    }
    if (compareDBI.parameters.XSL_TRANSFORMATION !== null) {
      this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`XSL Transformation ${compareDBI.parameters.XSL_TRANSFORMATION} applied when performing XML comparisons.`);
    }

	try {
      await compareDBI.initialize();
	  // Always use source schema mapping logic for export..
	  const sourceSchemaInfo = this.getSourceSchema(sourceVendor,sourceSchema,{});
	  const targetSchemaInfo = this.getSourceSchema(sourceVendor,targetSchema,{});
      if (reportRowCounts) {
        this.reportRowCounts(await compareDBI.getRowCounts(targetSchemaInfo),metrics,parameters,tableMappings) 
      }	
	  const startTime = performance.now();
	  const compareResults = await compareDBI.compareSchemas(sourceSchemaInfo,targetSchemaInfo);
	  compareResults.elapsedTime = performance.now() - startTime;
      // this.yadamu.LOGGER.qa([`COMPARE`,`${sourceVendor}`,`${targetVendor}`],`Elapsed Time: ${YadamuLibrary.stringifyDuration(compareResults.elapsedTime)}s`);
	  await compareDBI.releasePrimaryConnection()
      await compareDBI.finalize();

      compareResults.successful.forEach((row,idx) => {          
        const tableName = (parameters.TABLE_MATCHING === 'INSENSITIVE') ? row[2].toLowerCase() : row[2];
        const tableMetrics = (metrics[tableName] === undefined) ? { elapsedTime : 'N/A', throughput : "-1ms" } : metrics[tableName]
        row.push(tableMetrics.elapsedTime,tableMetrics.throughput)
      })	 
	
      if (parameters.TABLE_MATCHING === 'INSENSITIVE') {
        Object.keys(metrics).forEach((tableName)  => {
          if (tableName !== tableName.toLowerCase()) {
            metrics[tableName.toLowerCase()] = Object.assign({}, metrics[tableName])
            delete metrics[tableName]
          }
        })
      }

	  return compareResults;

    } catch (e) {
      this.yadamu.LOGGER.logException([`COMPARE`],e)
      await compareDBI.abort();
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
                                     + ` ${'TABLE_NAME'.padStart(colSizes[3])} |`
                                     + ` ${'ROWS'.padStart(colSizes[4])} |`
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
7   });
   
    const notesIdx = colSizes.length-2
    const lineSize = colSizes[notesIdx+1]

    results.failed.forEach((row,idx) => {
      const lines = []
      if ((row[notesIdx] !== null) && ((row[notesIdx].length > lineSize) || (row[notesIdx].indexOf('\r\n') > -1))) {
        const blocks = row[7].split('\r\n')
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
                                     + ` ${'TABLE_NAME'.padStart(colSizes[3])} |`
                                     + ` ${'SOURCE ROWS'.padStart(colSizes[4])} |`
                                     + ` ${'TARGET ROWS'.padStart(colSizes[5])} |`
                                     + ` ${'MISSING ROWS'.padStart(colSizes[6])} |`
                                     + ` ${'EXTRA ROWS'.padStart(colSizes[7])} |`
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
	
	// stepMetrics[stepMetrics.length-1].push(YadamuLibrary.stringifyDuration(results.elapsedTime))

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

  taskSummary(stepMetrics) {
	
	stepMetrics.unshift(['Data Set','Step','Mode','Source','Target','Elapsed Time'])
	 
	const colSizes = this.calculateColumnSizes(stepMetrics)      
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach((size)  => {
      seperatorSize += size;
    });

    this.yadamu.LOGGER.writeDirect(`\n`)
   
    stepMetrics.forEach((row,idx) => {          
      if (idx < 2) {
        this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      }
      this.yadamu.LOGGER.writeDirect(`|`)
	  row.forEach((col,idx) => {this.yadamu.LOGGER.writeDirect(` ${col.padStart(colSizes[idx])} |`)});
      this.yadamu.LOGGER.writeDirect(`\n`)
    })

    if (stepMetrics.length > 1) {
      this.yadamu.LOGGER.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }     
  }
  
  dbRoundtripResults(operationsList,elapsedTime,metrics) {
	
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
      this.yadamu.LOGGER.qa([`DBROUNDTRIP`,`STEP`].concat(operationsList),`${this.formatMetrics(metrics)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
    }
  
    this.taskSummary(metrics.stepMetrics)
    
  }

  async dbRoundtrip(task,configuration,test,targetConnectionName,parameters,operation) {
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
              
   /*
   **
   ** If source vendor and target vendor are the same test consists of a single operation. The operation does a DDL_AND_DATA mode copy of the data from the source schema to the target schema.
   **
   **
   ** If source vendor and target vendor are difference the test consists of two, or possiblly three operations.
   **
   ** If clone mode = true 
   ** 
   */
            
   /*
   **    Ensure that the table structure of the target is a direct clone of the source.
   **
   **    If source = target then both database are managed by the same vendor. The test consists of a single operation. The operation is a direct clone of the source schema into the target schema. MODE is DDL_AND_DLL. 
   ** 
   **    If source != target then we are copying data from the source vendor to the target vendor and back to the source vendor.
   **
   **       The first operation is a direct clone from the source schema to the target schema. The mode is DDL_ONLY. This clones the source schemas structure in the target schema.
   **       The second operations is a copy from the source vendor to the target vendor. The mode is DDL_AND_DATA.
   **       The third operation is a copy from the target vendor back to the source vendor using the schema created in the first operation. 
   **
   */
    const metrics = []
	const stepMetrics = []
	const operationsList = []
    let tableMappings = {}
	const taskMetrics = this.initializeMetrics();
	
	const sourceConnectionName = test.source

    const sourceConnectionInfo = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnectionInfo = this.getConnection(configuration.connections,targetConnectionName)

    const sourceDatabase =  Object.keys(sourceConnectionInfo)[0];
    const targetDatabase =  Object.keys(targetConnectionInfo)[0];

    const sourceConnection = sourceConnectionInfo[sourceDatabase]
	const targetConnection = targetConnectionInfo[targetDatabase]

    const sourceSchema  = this.getSourceSchema(sourceDatabase,operation.source,operation);
    const targetSchema  = this.getTargetSchema(targetDatabase,operation.target,operation);
    const compareSchema = this.getTargetSchema(sourceDatabase,operation.target,operation);
	    
    const sourceParameters  = Object.assign({},parameters)
    const compareParameters = Object.assign({},parameters)
	
    this.setUser(sourceParameters,'FROM_USER',sourceDatabase, sourceSchema)
    this.setUser(compareParameters,'TO_USER', sourceDatabase, compareSchema)
	
	const sourceDescription = this.getDescription(sourceDatabase,sourceConnectionName,sourceParameters,'FROM_USER')  
	const compareDescription = this.getDescription(sourceDatabase,sourceConnectionName,compareParameters,'TO_USER')  

	// If the source connection and target connection reference the same server perform a single DDL_AND_DATA copy between the source schema and target schema.
  	
	const mode =  (sourceConnection === targetConnection) ? "DDL_AND_DATA" : "DDL_ONLY"
    sourceParameters.MODE = mode;
    compareParameters.MODE = mode;

	// Clone the Source Schema to the CompareSchema
	  
	let sourceDBI = this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)
	let compareDBI = this.getDatabaseInterface(sourceDatabase,sourceConnection,compareParameters,true) 
    const taskStartTime = performance.now();
	let stepStartTime = taskStartTime
    metrics.push(await this.yadamu.pumpData(sourceDBI,compareDBI));
    let stepElapsedTime = performance.now() - stepStartTime
    stepMetrics.push([task,'COPY',compareParameters.MODE,sourceConnectionName,sourceConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      taskMetrics.errors++
      return taskMetrics;
    }
    
    operationsList.push(sourceDescription)
    operationsList.push(compareDescription)
    let sourceVersion = sourceDBI.DB_VERSION;
    let targetVersion = compareDBI.DB_VERSION;

	let results = this.printResults(configuration.operation.toUpperCase(),sourceDescription,compareDescription,stepElapsedTime)	
	
	this.aggregateMetrics(taskMetrics,results);
	
	// Test the current value of MODE. 
	// If MODE is set to DDL_ONLY the first copy operation cloned the structure of the source schema into the compare schema.
	// If MODE = DDL_AND_DATA the first copy completed the operation. 
	
	if (mode === 'DDL_ONLY') {
	  
	  // Run two more COPY operations. 
	  // The first copies the DATA from source to the target. 
	  
	  operationsList.length = 0
      const targetParameters  = Object.assign({},parameters)
      this.setUser(targetParameters,'TO_USER', targetDatabase, targetSchema)
      sourceParameters.MODE = "DATA_ONLY"
  	  targetParameters.MODE = "DATA_ONLY"
	  compareParameters.MODE = "DATA_ONLY"
      const targetDescription = this.getDescription(targetDatabase,targetConnectionName,targetParameters,'TO_USER')  
 
	  sourceDBI = this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)
      let targetDBI = this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,true) 
      this.propogateTableMatching(sourceDBI,targetDBI);
      let stepStartTime = performance.now();
      metrics.push(await this.yadamu.pumpData(sourceDBI,targetDBI));
      stepElapsedTime = performance.now() - stepStartTime
      stepMetrics.push([task,'COPY',targetDBI.MODE,sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
	  if (metrics[metrics.length-1] instanceof Error) {
        taskMetrics.errors++
        return taskMetrics;
      }
      
	  tableMappings = targetDBI.getTableMappings()
      targetVersion = targetDBI.DB_VERSION

      operationsList.push(sourceDescription)
      operationsList.push(targetDescription)
      results = this.printResults(configuration.operation.toUpperCase(),sourceDescription,targetDescription,stepElapsedTime)
  	  this.aggregateMetrics(taskMetrics,results);

	  // The second copies the DATA from source to the target. 
	  
  	  this.setUser(sourceParameters,'FROM_USER', targetDatabase, targetSchema)
  	  
	  // Make sure that we don't terminate the READER or WRITER when copying data back.
	  
	  delete sourceParameters.KILL_READER_AFTER
	  delete sourceParameters.KILL_WRITER_AFTER
	  delete compareParameters.KILL_READER_AFTER
	  delete compareParameters.KILL_WRITER_AFTER
	  
	  // Apply any table mappings used by target of the the first copy operation to the source of the second copy operation.
	  
	  targetDBI = this.getDatabaseInterface(targetDatabase,targetConnection,sourceParameters,false,tableMappings)
      compareDBI = this.getDatabaseInterface(sourceDatabase,sourceConnection,compareParameters,false)
      
      stepStartTime = performance.now();
      metrics.push(await this.yadamu.pumpData(targetDBI,compareDBI));
      stepElapsedTime = performance.now() - stepStartTime
      stepMetrics.push([task,'COPY',compareDBI.MODE,targetConnectionName,sourceConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
      if (metrics[metrics.length-1] instanceof Error) {
        taskMetrics.errors++
        return taskMetrics;
      }
      
      operationsList.push(compareDescription)
      results = this.printResults(configuration.operation.toUpperCase(),targetDescription,compareDescription,stepElapsedTime)
  	  this.aggregateMetrics(taskMetrics,results);

      const taskElapsedTime =  performance.now() - taskStartTime
	  stepMetrics.push([task,'TASK','',sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(taskElapsedTime)])

    }    
	
    
    this.fixupMetrics(metrics);   
    const compareParms = this.getCompareParameters(sourceDatabase,sourceVersion,targetDatabase,targetVersion,compareParameters)
	const compareResults = await this.compareSchemas(sourceDatabase, sourceConnection, compareParms, sourceSchema, targetDatabase, compareSchema, metrics[metrics.length-1], false, undefined)
    taskMetrics.failed += compareResults.failed.length;
	stepMetrics.push([task,'COMPARE','',sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
	this.printCompareResults(sourceConnectionName,targetConnectionName,task,compareResults)
	const elapsedTime =  performance.now() - taskStartTime
	stepMetrics.push([task,'TOTAL','',sourceConnectionName,targetConnectionName,YadamuLibrary.stringifyDuration(elapsedTime)])
    taskMetrics.stepMetrics = stepMetrics
    this.dbRoundtripResults(operationsList,elapsedTime,taskMetrics)
	return taskMetrics;
	
  }
  
  supportsDDLGeneration(instance) {
	do { 
	  if (Object.getOwnPropertyDescriptor(instance,'getDDLOperations')  !== null) return instance.constructor.name;
	} while ((instance = Object.getPrototypeOf(instance)))
  }
  
  async fileRoundtrip(task,mode,configuration,test,targetConnectionName,parameters,operation) {
  
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
  
    const fileInterface = mode === 'FILEROUNDTRIP' ? 'file' : 'loader'

    const metrics = []
	const stepMetrics = []
	const taskMetrics = this.initializeMetrics();
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
	
	const sourceFile = path.join(sourceConnection.directory,operation.source.directory,operation.source.file).replace(/%connection%/g,targetConnectionName).replace(/%vendor%/g,targetDatabase).replace(/%mode%/g,this.yadamu.MODE);		
	const connectionPath = YadamuTest.TEST_DEFAULTS.DDL_SUPPORTED.includes(targetDatabase) ? path.join(targetConnectionName,this.yadamu.MODE) : targetConnectionName
    const targetDirectory = path.normalize(test.directory.replace(/%sourceDirectory%/g,operation.source.directory).replace(/%connection%/g,connectionPath).replace(/%vendor%/g,targetDatabase).replace(/%mode%/g,this.yadamu.MODE));
	fs.mkdirSync(targetDirectory,{ recursive : true })
	
	const sourcePathComponents = path.parse(operation.source.file);
	const filename1 = sourcePathComponents.name + ".1" + sourcePathComponents.ext
	const filename2 = sourcePathComponents.name + ".2" + sourcePathComponents.ext
	const file1 = path.join(targetDirectory,filename1)
	const file2 = path.join(targetDirectory,filename2)

    const sourceSchema = operation.target;
	const targetSchema1  = this.getTargetSchema(targetDatabase,operation.target,operation);
	const targetSchema2  = this.getTargetSchema(targetDatabase,operation.target,operation);

	switch (targetDatabase) {
	  case "mssql" :
	    switch (targetSchema1.owner) {
		   case "dbo":
		     targetSchema1.database = targetSchema1.database + "1";
			 targetSchema2.database = targetSchema2.database + "2"
		     break;
		   default:
		     targetSchema1.owner = targetSchema1.owner + "1";
			 targetSchema2.owner = targetSchema2.owner + "2"
        }
		break;
      default:
        targetSchema1.schema = targetSchema1.schema + "1";
	    targetSchema2.schema = targetSchema2.schema + "2" 
    }

	const taskStartTime = performance.now();

	// Source File to Target Schema #1
	
	let sourceParameters  = Object.assign({},parameters)
	sourceParameters.FILE = sourceFile
	let fileReader = this.getDatabaseInterface(fileInterface,sourceConnection,sourceParameters,true)
    let sourceDescription = this.getDescription(sourceDatabase,'file',sourceParameters,'FILE')
	
    let targetParameters  = Object.assign({},parameters)
    this.setUser(targetParameters,'TO_USER',targetDatabase, targetSchema1)
    let targetDescription = this.getDescription(targetDatabase,targetConnectionName,targetParameters,'TO_USER')
	let targetDBI = this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,true)
	
	let stepStartTime = taskStartTime	
	if (test.parser === 'SQL') {
	  targetDBI.setParameters({"FILE" : sourceFile})
	  metrics.push(await this.yadamu.uploadData(targetDBI))
	}
	else {
  	  metrics.push(await this.yadamu.pumpData(fileReader,targetDBI))
    }
    let stepElapsedTime = performance.now() - stepStartTime
	
	
	stepMetrics.push([task,'IMPORT',targetDBI.MODE,sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      taskMetrics.errors++
      return taskMetrics;
    }
    
    let targetVersion = targetDBI.DB_VERSION
	let tableMappings = targetDBI.getTableMappings();
    let counters = this.printResults(configuration.operation.toUpperCase(),sourceDescription,targetDescription,stepElapsedTime)
    this.aggregateMetrics(taskMetrics,counters);
	sourceDescription = targetDescription

	sourceParameters  = Object.assign({},parameters)
    this.setUser(sourceParameters,'FROM_USER',targetDatabase, targetSchema1)
	// Use the the mappings generted during the import to drive the export

    let sourceDBI = this.getDatabaseInterface(targetDatabase,targetConnection,sourceParameters,false,tableMappings)

	targetParameters  = Object.assign({},parameters)
	targetParameters.FILE = file1;
  	let fileWriter = this.getDatabaseInterface(fileInterface,sourceConnection,targetParameters,true)
    targetDescription = this.getDescription(sourceDatabase,sourceConnectionName,targetParameters,'FILE')  

	stepStartTime = performance.now();
  	metrics.push(await this.yadamu.pumpData(sourceDBI,fileWriter))
	
	stepElapsedTime = performance.now() - stepStartTime
    stepMetrics.push([task,'EXPORT',fileWriter.MODE,targetConnectionName,sourceDatabase,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      taskMetrics.errors++
      return taskMetrics;
    }
	
    counters = this.printResults(configuration.operation.toUpperCase(),sourceDescription,targetDescription,stepElapsedTime)
    this.aggregateMetrics(taskMetrics,counters);
	sourceDescription = targetDescription
	
	// File#1 to Target Schema #2

	sourceParameters  = Object.assign({},parameters)
	sourceParameters.FILE = file1;
    fileReader = this.getDatabaseInterface(fileInterface,sourceConnection,sourceParameters,true)
    
    targetParameters  = Object.assign({},parameters)
    this.setUser(targetParameters,'TO_USER',targetDatabase, targetSchema2)
	targetDBI = this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,true)
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
    stepMetrics.push([task,'EXPORT',targetDBI.MODE,sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      taskMetrics.errors++
      return taskMetrics;
    }
	
	targetVersion = targetDBI.DB_VERSION
	tableMappings = targetDBI.getTableMappings();
	counters = this.printResults(configuration.operation.toUpperCase(),sourceDescription,targetDescription,stepElapsedTime)
    this.aggregateMetrics(taskMetrics,counters);
	sourceDescription = targetDescription

	// Target Schema #2 to File#2
	
	sourceParameters  = Object.assign({},parameters)
    this.setUser(sourceParameters,'FROM_USER',targetDatabase, targetSchema2)
	sourceDBI = this.getDatabaseInterface(targetDatabase,targetConnection,sourceParameters,false)
	// Use the mappings generted during the import to drive the export
    sourceDBI.setTableMappings(tableMappings)

	targetParameters  = Object.assign({},parameters)
	targetParameters.FILE = file2;
    fileWriter = this.getDatabaseInterface(fileInterface,sourceConnection,targetParameters,true)
    targetDescription = this.getDescription(targetDatabase,targetConnectionName,targetParameters,'FILE')

	stepStartTime = performance.now();
  	metrics.push(await this.yadamu.pumpData(sourceDBI,fileWriter))
    stepElapsedTime = performance.now() - stepStartTime
    stepMetrics.push([task,'IMPORT',fileWriter.MODE,targetConnectionName,sourceDatabase,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      taskMetrics.errors++
      return taskMetrics;
    }

    counters = this.printResults(configuration.operation.toUpperCase(),sourceDescription,targetDescription,stepElapsedTime)
    this.aggregateMetrics(taskMetrics,counters);
	this.operationsList.push(targetDescription);
    this.fixupMetrics(metrics);   

	const taskElapsedTime =  performance.now() - taskStartTime
    stepMetrics.push([task,'TASK','',sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(taskElapsedTime)])
   
	// Compare Results

	
	/*
	**
	** Import #1: Array Size Vs Rows Imported
	** Import #2: Array Size Vs Rows Imported
	**
	*/
	
	let compareDBI = this.getDatabaseInterface(targetDatabase,targetConnection,{},false)
    await compareDBI.initialize();
	
    stepStartTime = performance.now();
	this.reportRowCounts(await compareDBI.getRowCounts(targetSchema1),metrics[0],parameters,targetDBI.inverseTableMappings) 
	stepElapsedTime = performance.now() - stepStartTime 
	stepMetrics.push([task,'COUNT','',targetConnectionName,'',YadamuLibrary.stringifyDuration(stepElapsedTime)])

    stepStartTime = performance.now();
	this.reportRowCounts(await compareDBI.getRowCounts(targetSchema2),metrics[2],parameters,targetDBI.inverseTableMappings) 
	stepElapsedTime = performance.now() - stepStartTime 
	stepMetrics.push([task,'COUNT','',targetConnectionName,'',YadamuLibrary.stringifyDuration(stepElapsedTime)])
   
    await compareDBI.releasePrimaryConnection()
    await compareDBI.finalize();
	
	// If the content of the export file originated in the target database compare the imported schema with the source schema.
		
    if (operation.source.directory.indexOf(targetConnectionName) === 0) {
      const testParameters = {} // parameters ? Object.assign({},parameters) : {}
	  const compareParms = this.getCompareParameters(targetDatabase,targetVersion,targetDatabase,targetVersion,testParameters)
	  const compareResults = await this.compareSchemas(targetDatabase, targetConnection, compareParms, operation.target, targetDatabase, targetSchema1, metrics[1],false,targetDBI.inverseTableMappings)
	  taskMetrics.failed += compareResults.failed.length;
	  stepMetrics.push([task,'COMPARE','',targetConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
	  this.printCompareResults('File',targetConnectionName,task,compareResults)
    }

    // Compare Target Schema #1 and Target Schema #2

	const testParameters = parameters ? Object.assign({},parameters) : {}
	const compareParams = this.getCompareParameters(targetDatabase,targetVersion,targetDatabase,targetVersion,testParameters)
	const compareResults = await this.compareSchemas(targetDatabase, targetConnection, compareParams, targetSchema1, targetDatabase, targetSchema2, metrics[3],false,targetDBI.inverseTableMappings)
	taskMetrics.failed += compareResults.failed.length;
	stepMetrics.push([task,'COMPARE','',targetConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
	this.printCompareResults('File',targetConnectionName,task,compareResults)
    
	stepStartTime = performance.now();
	const fileCompareParms = this.getCompareParameters(targetDatabase,targetVersion,'file',0,testParameters)
    const fileCompare = this.getDatabaseInterface('file',{},fileCompareParms,false,tableMappings)
	const fileCompareResults = await fileCompare.compareFiles(this.yadamuLoggger, sourceFile, file1, file2, metrics)

	if (fileCompareResults.length > 0) {
	  this.failedOperations[sourceConnectionName] = Object.assign({},this.failedOperations[sourceConnectionName])
	  this.failedOperations[sourceConnectionName][targetConnectionName] = Object.assign({},this.failedOperations[sourceConnectionName][targetConnectionName])
	  fileCompareResults.forEach((failed,idx) => {
	    this.failedOperations[sourceConnectionName][targetConnectionName][task] = Object.assign({},this.failedOperations[sourceConnectionName][targetConnectionName][task])
        this.failedOperations[sourceConnectionName][targetConnectionName][task][failed[2]] = failed
	  })
	}
	
    stepElapsedTime = performance.now() - stepStartTime
    stepMetrics.push([task,'COMPARE','',sourceDatabase,'',YadamuLibrary.stringifyDuration(stepElapsedTime)])

    const elapsedTime = performance.now() - taskStartTime
    this.yadamu.LOGGER.qa([mode,`${test.parser === 'SQL' ? 'SQL' : 'CLARINET'}`].concat(this.operationsList),`Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
    stepMetrics.push([task,'TOTAL','','file',targetConnectionName,YadamuLibrary.stringifyDuration(elapsedTime)])
    this.taskSummary(stepMetrics)
	taskMetrics.stepMetrics = stepMetrics
	return taskMetrics

  }

  async import(task,mode,configuration,test,targetConnectionName,parameters,operation) {
	
	const stepMetrics = []
    const fileInterface = mode === 'IMPORT' ? 'file' : 'loader'

	const taskMetrics = this.initializeMetrics();
    const sourceConnectionName = test.source
		
    const sourceConnectionInfo = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnectionInfo = this.getConnection(configuration.connections,targetConnectionName)

    const sourceDatabase =  Object.keys(sourceConnectionInfo)[0];
    const targetDatabase =  Object.keys(targetConnectionInfo)[0];

    const sourceConnection = sourceConnectionInfo[sourceDatabase]
	const targetConnection = targetConnectionInfo[targetDatabase]

    const targetSchema  = this.getTargetSchema(targetDatabase,operation.target,operation); 
	
     
	const sourceParameters  = Object.assign({},parameters)
    const sourceFile = path.join(sourceConnection.directory.replace(/%connection%/g,targetConnectionName).replace(/%vendor%/g,targetDatabase).replace(/%mode%/g,this.yadamu.MODE),operation.source.file)
	sourceParameters.FILE = sourceFile

    const fileReader = this.getDatabaseInterface(fileInterface,sourceConnection,sourceParameters,true)
    
    const targetParameters  = Object.assign({},parameters)
    this.setUser(targetParameters,'TO_USER',targetDatabase,targetSchema)
	
    const sourceDescription = this.getDescription(sourceDatabase,'file', sourceParameters,'FILE')  
    const targetDescription = this.getDescription(targetDatabase,targetConnectionName, targetParameters,'TO_USER')
    
	const targetDBI = this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,(operation.recreateTarget || test.recreateSchemas))
    const taskStartTime = performance.now();
	let stepStartTime = taskStartTime
	
	let metrics
	if (test.parser === 'SQL') {
	  targetDBI.setParameters({"FILE" : sourceFile})
	  metrics = await this.yadamu.uploadData(targetDBI)
	}
	else {
  	  metrics = await this.yadamu.pumpData(fileReader,targetDBI)
    }
	
    let stepElapsedTime = performance.now() - stepStartTime
    stepMetrics.push([task,'IMPORT',targetDBI.MODE,sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
	
	if ((operation.hasOwnProperty('verify')) && (this.yadamu.MODE !== 'DDL_ONLY')) {
	  const compareDBI = this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,false)
      await compareDBI.initialize();
	  stepStartTime = performance.now()
	  this.reportRowCounts(await compareDBI.getRowCounts(targetSchema),metrics,parameters,targetDBI.inverseTableMappings) 
	  stepElapsedTime = performance.now() - stepStartTime 
	  await compareDBI.releasePrimaryConnection()
      await compareDBI.finalize();
      stepMetrics.push([task,'COUNT','',targetConnectionName,'',YadamuLibrary.stringifyDuration(stepElapsedTime)])
    }		

    const elapsedTime = performance.now() - taskStartTime
    const counters = this.printResults(mode,sourceDescription,targetDescription,elapsedTime)
	stepMetrics.push([task,'TOTAL','',sourceDatabase,targetConnectionName,YadamuLibrary.stringifyDuration(elapsedTime)])
	this.taskSummary(stepMetrics)
    counters.path = path.dirname(sourceFile);
	counters.stepMetrics = stepMetrics
	return counters
      
  }
 
  async export(task,mode,configuration,test,targetConnectionName,parameters,operation) {
      
    const fileInterface = mode === 'EXPORT' ? 'file' : 'loader'

	let counters
    const metrics = []
	const stepMetrics = []
	const taskMetrics = this.initializeMetrics();
	
    
	const sourceConnectionName = test.source
	
    const sourceConnectionInfo = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnectionInfo = this.getConnection(configuration.connections,targetConnectionName)

    const sourceDatabase =  Object.keys(sourceConnectionInfo)[0];
    const targetDatabase =  Object.keys(targetConnectionInfo)[0];

    const sourceConnection = sourceConnectionInfo[sourceDatabase]
	const targetConnection = targetConnectionInfo[targetDatabase]
 
    const sourceParameters  = Object.assign({},parameters)
    this.setUser(sourceParameters,'FROM_USER',sourceDatabase, operation.source, operation.rdbms)
    const sourceDBI = this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)
    
	const targetParameters  = Object.assign({},parameters)
    this.setUser(targetParameters,'TO_USER',sourceDatabase, operation.source, operation.rdbms)
    
    const targetDirectory = targetConnection.directory.replace(/%connection%/g,sourceConnectionName).replace(/%vendor%/g,sourceDatabase).replace(/%mode%/g,this.yadamu.MODE)
    const targetFile =  fileInterface === "file" ? path.join(targetDirectory,operation.target.file) : targetDirectory
	targetParameters.FILE = targetFile

    let sourceDescription = this.getDescription(sourceDatabase,sourceConnectionName,sourceParameters,'FROM_USER')  
    let targetDescription = this.getDescription(targetDatabase,'file',targetParameters,'FILE')

    const fileWriter = this.getDatabaseInterface(fileInterface,targetConnection,targetParameters,true)

	const taskStartTime = performance.now();
	let stepStartTime = taskStartTime;
  	metrics.push(await this.yadamu.pumpData(sourceDBI,fileWriter));
	let stepElapsedTime = performance.now() - stepStartTime
    this.printResults(configuration.operation.toUpperCase(),sourceDescription,targetDescription,stepElapsedTime)
    stepMetrics.push([task,'EXPORT',fileWriter.MODE,sourceConnectionName,targetDatabase,YadamuLibrary.stringifyDuration(stepElapsedTime)])
    if (metrics[metrics.length-1] instanceof Error) {
      taskMetrics.errors++
      return taskMetrics;
    }

    const sourceVersion = sourceDBI.DB_VERSION;
		
	if (operation.hasOwnProperty('verify')) {
	  sourceDescription = targetDescription
      const sourceParameters  = Object.assign({},parameters)
      if (fileInterface === "file") { 
        sourceParameters.FILE = targetFile
      } 
      else { 
       sourceParameters.FILE = targetDirectory
       this.setUser(sourceParameters,'FROM_USER',sourceDatabase, operation.source)
      }
      
      const fileReader = this.getDatabaseInterface(fileInterface,sourceConnection,sourceParameters,true)
	  
  	  const targetParameters  = Object.assign({},parameters)
      this.setUser(targetParameters,'TO_USER', sourceDatabase, operation.verify, operation.rdbms)
	  const targetDBI = this.getDatabaseInterface(sourceDatabase,sourceConnection,targetParameters,true)

      stepStartTime = performance.now();
  	  metrics.push(await this.yadamu.pumpData(fileReader,targetDBI));
      stepElapsedTime = performance.now() - stepStartTime
      stepMetrics.push([task,'IMPORT',fileReader.MODE,targetDatabase,sourceConnectionName,YadamuLibrary.stringifyDuration(stepElapsedTime)])
      if (metrics[metrics.length-1] instanceof Error) {
        taskMetrics.errors++
        return taskMetrics;
      }
      
      const taskElapsedTime =  performance.now() - taskStartTime
   	  stepMetrics.push([task,'TASK','',sourceConnectionName,targetDatabase,YadamuLibrary.stringifyDuration(taskElapsedTime)])

	  
	  const verifyDescription = this.getDescription(sourceDatabase,sourceConnectionName,targetParameters,'TO_USER') 
      counters  = this.printResults('VERIFY',sourceDescription,verifyDescription,taskElapsedTime)
      this.aggregateMetrics(taskMetrics,counters);
      if (this.yadamu.MODE !== 'DDL_ONLY') {
		// Report rows Imported and Compare Schemas..
        const compareParms = this.getCompareParameters(sourceDatabase,sourceVersion,targetDatabase,0,parameters)
        const compareResults = await this.compareSchemas(sourceDatabase, sourceConnection, compareParms, operation.source, sourceDatabase, operation.verify, metrics[metrics.length-1],true,undefined)
	    taskMetrics.failed += compareResults.failed.length;
	    stepMetrics.push([task,'COMPARE','',sourceConnectionName,'',YadamuLibrary.stringifyDuration(compareResults.elapsedTime)])
	    this.printCompareResults(sourceConnectionName,targetConnectionName,task,compareResults)
      } 
    }
      
    stepMetrics.push([task,'TOTAL','',sourceConnectionName,targetDatabase,YadamuLibrary.stringifyDuration(performance.now() - taskStartTime)])
    this.taskSummary(stepMetrics)
	taskMetrics.path = path.dirname(targetFile);
	taskMetrics.stepMetrics = stepMetrics
    return taskMetrics
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

  reverseOperation(operation) {
    const reversedOperation = Object.assign({},operation);
	reversedOperation.source = operation.target;
	reversedOperation.target = operation.source;
	return reversedOperation
  }
  
  initializeMetrics() {
    return {
	  errors    : 0
	, warnings  : 0
	, failed    : 0
	}
  }
	
  resetMetrics(metrics) {
	// Preserve the object by using Object.assign to copy zeroed properties from an new metrics instance.
    Object.assign(metrics,this.initializeMetrics());
  }
	
  aggregateMetrics(cummulative, metrics, retainMetrics) {
	cummulative.errors+= metrics.errors;
	cummulative.warnings+= metrics.warnings;
	cummulative.failed+= metrics.failed;
	if (!retainMetrics) this.resetMetrics(metrics)
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

	  
  async doTests(configuration) {

    this.yadamu.LOGGER.qa([`Environemnt`,process.arch,process.platform,process.version],`Running tests`);

    const startTime = performance.now()
    let mode = configuration.operation.toUpperCase()
    
	const operationMetrics = this.initializeMetrics()
    const taskMetrics = this.initializeMetrics()
    const targetMetrics = this.initializeMetrics()
    const sourceMetrics = this.initializeMetrics()
	const sourceSummary = []
	const testMetrics = this.initializeMetrics()

    const summary = [['End Time','Test','Source','Target','Results','Memory Usage','ElapsedTime']]
	    
    try {
      for (const test of configuration.tests) {
        // Initialize constructor parameters with values from configuration file
        const testParameters = Object.assign({} , configuration.parameters ? configuration.parameters : {})

        // Merge test specific parameters
        Object.assign(testParameters, test.parameters ? test.parameters : {})
        
		this.yadamu.loadParameters(testParameters);
        const startTime = performance.now()
  	    let sourceDescription = test.source;
	    const targets = test.target ? [test.target] : test.targets
	    const targetMetrics = this.initializeMetrics()
        for (const target of targets) {
		  let targetDescription = target
          const targetConnection = configuration.connections[target]
		  const startTime = performance.now()		      
	      for (const task of test.tasks) {
         	let metrics = this.initializeMetrics()
	        try {
		      const operations = this.getTaskList(configuration,task)
		      const startTime = performance.now()
			  for (const operation of operations) {
                mode = (test.operation ? test.operation : configuration.operation).toUpperCase()
			    switch (mode) {
 		          case 'EXPORT':
                  case 'UNLOAD':
				    metrics = await this.export(operation.taskName,mode,configuration,test,target,testParameters,test.reverseOperations ? this.reverseOperation(operation) : operation)
					targetDescription = 'file://' + metrics.path
					delete metrics.path
  				    break;
 		          case 'IMPORT':
                  case 'LOAD':
    		        metrics = await this.import(operation.taskName,mode,configuration,test,target,testParameters,test.reverseOperations ? this.reverseOperation(operation) : operation)
					sourceDescription = 'file://' + metrics.path
					delete metrics.path
				    break;
			      case 'FILEROUNDTRIP':
                  case 'LOADERROUNDTRIP':
			        metrics = await this.fileRoundtrip(operation.taskName,mode,configuration,test,target,testParameters,test.reverseOperations ? this.reverseOperation(operation) : operation)
			        break;
			      case 'DBROUNDTRIP':
  			        metrics = await this.dbRoundtrip(operation.taskName,configuration,test,target,testParameters,test.reverseOperations ? this.reverseOperation(operation) : operation)
			        break;
			      case 'LOSTCONNECTION':
			        metrics = await this.dbRoundtrip(operation.taskName,configuration,test,target,testParameters,test.reverseOperations ? this.reverseOperation(operation) : operation)
			        break;
			    }
				this.aggregateMetrics(operationMetrics,metrics)
			  }
              const elapsedTime = performance.now() - startTime;
              this.yadamu.LOGGER.qa([mode,`TASK`,`${sourceDescription}`,`${targetDescription}`,`${typeof task === 'string' ? task : 'Anonymous'}`],`${this.formatMetrics(operationMetrics)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
			  if (operations.length > 1 ) {
				summary.push([new Date().toISOString(),mode,sourceDescription,targetDescription,Object.values(taskMetrics),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
			  }
			  this.aggregateMetrics(taskMetrics,operationMetrics)
		    } catch (e) {
  			  this.aggregateMetrics(operationMetrics,metrics)
			  this.aggregateMetrics(taskMetrics,operationMetrics)
  	          this.aggregateMetrics(targetMetrics,taskMetrics)
              this.aggregateMetrics(sourceMetrics,targetMetrics)
			  this.yadamu.LOGGER.logException([mode,`TASK`],e);
			  throw e
			}   
	      }
		  const elapsedTime = performance.now() - startTime;
          this.yadamu.LOGGER.qa([mode,`TARGET`,`${sourceDescription}`,`${targetDescription}`],`${this.formatMetrics(taskMetrics)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
		  summary.push([new Date().toISOString(),mode,sourceDescription,targetDescription,Object.values(taskMetrics),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
  	      this.aggregateMetrics(targetMetrics,taskMetrics)
        }		 
        const elapsedTime = performance.now() - startTime;
        this.yadamu.LOGGER.qa([mode,`SOURCE`,`${sourceDescription}`],`${this.formatMetrics(targetMetrics)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
		
        summary.push([new Date().toISOString(),mode,sourceDescription,'',Object.values(taskMetrics),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
		
        this.aggregateMetrics(sourceMetrics,targetMetrics)
      }
      const elapsedTime = performance.now() - startTime;
      this.yadamu.LOGGER.qa([mode,`SUCCESS`],`${this.formatMetrics(sourceMetrics)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
      summary.push([new Date().toISOString(),mode,'','',Object.values(taskMetrics),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
      this.aggregateMetrics(testMetrics,sourceMetrics)
	} catch (e) {
      const elapsedTime = performance.now() - startTime;
      this.yadamu.LOGGER.handleException([mode,`FAILED`],e)
      this.yadamu.LOGGER.error([mode,`FAILED`],`${this.formatMetrics(sourceMetrics)} Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
	  this.aggregateMetrics(testMetrics,sourceMetrics)
	}  
    const elapsedTime = performance.now() - startTime;
    summary.push([new Date().toISOString(),'','','',Object.values(taskMetrics),Object.values(process.memoryUsage()),YadamuLibrary.stringifyDuration(elapsedTime)])
	this.formatArrays(summary)
	this.printFailedSummary()
	this.printSourceSummary(summary)
	this.printTargetSummary(summary)
    return this.formatMetrics(testMetrics)
  } 
}
 
module.exports = YadamuQA
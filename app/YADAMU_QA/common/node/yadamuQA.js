"use strict"

const path = require('path')
const fs = require('fs');
const { performance } = require('perf_hooks');


const Yadamu = require('./yadamuTest.js');
const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const {ConfigurationFileError} = require('../../../YADAMU/common/yadamuError.js');
const FileDBI = require('../../../YADAMU/file/node/fileDBI.js');
const FileQA = require('../../file/node/fileQA.js');

class YadamuQA {
	
  constructor(configuration,logger) {
	this.configuration = configuration
	this.yadamuLogger = logger;
	this.expandedTaskList = []
    this.yadamu = new Yadamu('TEST');
	this.yadamuLogger = this.yadamu.getYadamuLogger();
	this.operationsList = []
  }
  
  getDatabaseInterface(driver,testConnection,testParameters,recreateSchema,tableMappings) {
    
    let dbi = undefined
    this.yadamu.reset();
    
    switch (driver) {
      case "oracle"  : 
        const OracleQA = require('../../oracle/node/oracleQA.js');
        dbi = new OracleQA(this.yadamu)
        break;
      case "postgres" :
        const PostgresQA = require('../../postgres/node/postgresQA.js');
        dbi = new PostgresQA(this.yadamu)
        break;
      case "mssql" :
        const MsSQLQA = require('../../mssql/node/mssqlQA.js');
        dbi = new MsSQLQA(this.yadamu)
        break;
      case "mysql" :
        const MySQLQA = require('../../mysql/node/mysqlQA.js');
        dbi = new MySQLQA(this.yadamu)
        break;
      case "mariadb" :
        const MariaQA = require('../../mariadb/node/mariadbQA.js');
        dbi = new MariaQA(this.yadamu)
        break;
      case "mongodb" :
        const MongoQA = require('../../mongodb/node/mongoQA.js');
        dbi = new MongoQA(this.yadamu)
        break;
      case "snowflake" :
        const SnowFlakeQA = require('../../snowflake/node/snowflakeQA.js');
        dbi = new SnowFlakeQA(this.yadamu)
        break;
      case "file" :
        dbi = new FileQA(this.yadamu)
        break;
      default:   
        const err = new ConfigurationFileError(`[${this.constructor.name}.getDatabaseInterface()]: Unsupported database vendor "${driver}".`);  
		throw err
      }
	  
      const connectionProperties = Object.assign({},testConnection)
      const parameters = testParameters ? Object.assign({},testParameters) : {}
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
  
  setUser(parameters,key,db,schemaInfo,operation) {
	  
	switch (db) {
      case 'mssql':
	    if (schemaInfo.schema) {
		  parameters.MSSQL_SCHEMA_DB = schemaInfo.schema
	      parameters[key] = 'dbo'
		}
		else {
		  parameters.MSSQL_SCHEMA_DB = schemaInfo.database
	      parameters[key] = schemaInfo.owner
		}
        break;
      case 'snowflake':
	    parameters.SNOWFLAKE_SCHEMA_DB = schemaInfo.database
	    parameters[key] = schemaInfo.schema;
		break;
      default:
	    parameters[key] = schemaInfo.schema !== undefined ? schemaInfo.schema : (schemaInfo.owner === 'dbo' ? schemaInfo.database : schemaInfo.owner)
		parameters[key] = operation ? this.getPrefixedSchema(operation.schemaPrefix,parameters[key]) : parameters[key]
    }
	
	return parameters

  }
  
  fixupTimings(timings) {
    // If operations failed timings may be undefined. If so replace with empty object to prevent errors when reporting
    timings.forEach(function (t,i) {
      if ((t === undefined) || (t === null)) {
        timings[i] = {}
      }
    },this)
  }  
  
  printResults(operation,sourceDescription,targetDescription,elapsedTime) {
    this.operationsList.push(sourceDescription);
  
    if (!this.yadamuLogger.loggingToConsole()) {
      
      const colSizes = [24,128,12]
      let seperatorSize = (colSizes.length * 3) - 1;
      colSizes.forEach(function(size) {
        seperatorSize += size;
      },this);
    
      this.yadamuLogger.writeDirect('\n+' + '-'.repeat(seperatorSize) + '+' + '\n') 
     
      this.yadamuLogger.writeDirect(`| ${'TIMESTAMP'.padEnd(colSizes[0])} |`
                                  + ` ${'OPERATION'.padEnd(colSizes[1])} |`
                                  + ` ${'ELASPED TIME'.padStart(colSizes[2])} |` 
                 
                 + '\n');
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      
      this.yadamuLogger.writeDirect(`| ${new Date().toISOString().padEnd(colSizes[0])} |`
                                  + ` ${(sourceDescription + ' --> ' + targetDescription).padEnd(colSizes[1])} |`
                                  + ` ${(elapsedTime.toString()+"ms").padStart(colSizes[2])} |` 
                 + '\n');
                 
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
    else {
      this.yadamuLogger.info([`${this.constructor.name}.${operation}()`,`COPY`],`Completed. Source:[${sourceDescription}]. Target:[${targetDescription}]. Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
    }
  
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
		
    const testDefaults = this.yadamu.getYadamuTestDefaults()
    const compareParameters = Object.assign({}, testParameters)
    
    Object.assign(compareParameters, this.getDefaultValue('SPATIAL_PRECISION',testDefaults,sourceVendor,sourceVersion,targetVendor,targetVersion,testParameters))
    Object.assign(compareParameters, this.getDefaultValue('XSL_TRANSFORMATION',testDefaults,sourceVendor,sourceVersion,targetVendor,targetVersion,testParameters))
    Object.assign(compareParameters, this.getDefaultValue('ORDERED_JSON',testDefaults,sourceVendor,sourceVersion,targetVendor,targetVersion,testParameters))
   
    let versionSpecificKey = sourceVendor + "#" + sourceVersion;
    Object.assign(compareParameters, testDefaults[sourceVendor])
    Object.assign(compareParameters, testDefaults[versionSpecificKey] ? testDefaults[versionSpecificKey] : {})
 
    versionSpecificKey = targetVendor + "#" + targetVersion;
   
    Object.assign(compareParameters, testDefaults[targetVendor])
    Object.assign(compareParameters, testDefaults[versionSpecificKey] ? testDefaults[versionSpecificKey] : {})
    
    return compareParameters;
  }
  
  async reportRowCounts(rowCounts,timings,parameters) {
	  
    rowCounts.forEach(function(row,idx) {          
      const tableName = (parameters.TABLE_MATCHING === 'INSENSITIVE') ? row[1].toLowerCase() : row[1];
      const tableTimings = (timings[tableName] === undefined) ? { rowCount : -1 } : timings[tableName]
      row.push(tableTimings.rowCount)
    },this)	 

    const colSizes = [32, 48, 14, 14, 14]
      
    let seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach(function(size) {
      seperatorSize += size;
    },this);
   
    rowCounts.sort().forEach(function(row,idx) {          
      if (idx === 0) {
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${'TARGET SCHEMA'.padStart(colSizes[0])} |` 
                                    + ` ${'TABLE_NAME'.padStart(colSizes[1])} |`
                                    + ` ${'ROWS'.padStart(colSizes[2])} |`
                                    + ` ${'ROWS IMPORTED'.padStart(colSizes[3])} |`
                                    + ` ${'DELTA'.padStart(colSizes[4])} |`
                           + '\n');
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${row[0].padStart(colSizes[0])} |`
                                    + ` ${row[1].padStart(colSizes[1])} |`
                                    + ` ${row[2].toString().padStart(colSizes[2])} |` 
                                    + ` ${row[3].toString().padStart(colSizes[3])} |` 
                                    + ` ${(row[3] - row[2]).toString().padStart(colSizes[4])} |`
                           + '\n');
      }
      else {
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${''.padStart(colSizes[0])} |`
                                    + ` ${row[1].padStart(colSizes[1])} |`
                                    + ` ${row[2].toString().padStart(colSizes[2])} |` 
                                    + ` ${row[3].toString().padStart(colSizes[3])} |` 
                                    + ` ${(row[3] - row[2]).toString().padStart(colSizes[4])} |`
                           + '\n');         
      }
    },this)

    if (rowCounts.length > 0) {
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }     

  }

  async compareSchemas(sourceVendor,connectionProperties,parameters,sourceSchema,targetVendor,targetSchema,timings,reportRowCounts,tableMappings) {
	  
    const compareDBI = this.getDatabaseInterface(sourceVendor,connectionProperties,parameters,false,tableMappings)

    if (compareDBI.parameters.SPATIAL_PRECISION !== 18) {
      this.yadamuLogger.info([`${this.constructor.name}.compareSchemas()`,`${sourceVendor}`,`${targetVendor}`],`Spatial precision limited to ${compareDBI.parameters.SPATIAL_PRECISION} digits`);
    }
    if (compareDBI.parameters.EMPTY_STRING_IS_NULL === true) {
      this.yadamuLogger.info([`${this.constructor.name}.compareSchemas()`,`${sourceVendor}`,`${targetVendor}`],`Empty Strings treated as NULL`);
    }
    if (compareDBI.parameters.STRIP_XML_DECLARATION === true) {
      this.yadamuLogger.info([`${this.constructor.name}.compareSchemas()`,`${sourceVendor}`,`${targetVendor}`],`XML Declartion ignored when comparing XML content`);
    }
    if (compareDBI.parameters.TIMESTAMP_PRECISION && (compareDBI.parameters.TIMESTAMP_PRECISION < 9)){
      this.yadamuLogger.info([`${this.constructor.name}.compareSchemas()`,`${sourceVendor}`,`${targetVendor}`],`Timestamp precision limited to ${compareDBI.parameters.TIMESTAMP_PRECISION} digits`);
    }
    if (compareDBI.parameters.XSL_TRANSFORMATION !== null) {
      this.yadamuLogger.info([`${this.constructor.name}.compareSchemas()`,`${sourceVendor}`,`${targetVendor}`],`XSL Transformation ${compareDBI.parameters.XSL_TRANSFORMATION} applied when performing XML comparisons.`);
    }
	
	let startTime
	let elapsedTime
	
	let report
	try {
      await compareDBI.initialize();
	  // Always use source schema mapping logic for export..
	  const sourceSchemaInfo = this.getSourceSchema(sourceVendor,sourceSchema,{});
	  const targetSchemaInfo = this.getSourceSchema(sourceVendor,targetSchema,{});
      if (reportRowCounts) {
        this.reportRowCounts(await compareDBI.getRowCounts(targetSchemaInfo),timings,parameters) 
      }	
	  startTime = performance.now();
	  report = await compareDBI.compareSchemas(sourceSchemaInfo,targetSchemaInfo);
	  elapsedTime = performance.now() - startTime;
      await compareDBI.finalize();
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}.compareSchemas()`],e)
      await compareDBI.abort();
      throw e
    } 
	
    if (parameters.TABLE_MATCHING === 'INSENSITIVE') {
      Object.keys(timings).forEach(function(tableName) {
        if (tableName !== tableName.toLowerCase()) {
        timings[tableName.toLowerCase()] = Object.assign({}, timings[tableName])
        delete timings[tableName]
      }
      },this)
    }

    report.successful.forEach(function(row,idx) {          
      const tableName = (parameters.TABLE_MATCHING === 'INSENSITIVE') ? row[2].toLowerCase() : row[2];
      const tableTimings = (timings[tableName] === undefined) ? { elapsedTime : 'N/A', throughput : "-1ms" } : timings[tableName]
      row.push(tableTimings.elapsedTime,tableTimings.throughput)
    },this)	 
	 	 
    const colSizes = [12, 32, 32, 48, 14, 14, 14, 14, 72]
    
    let seperatorSize = (colSizes.slice(0,7).length *3) - 1
    colSizes.slice(0,7).forEach(function(size) {
      seperatorSize += size;
    },this);
    
    report.successful.sort().forEach(function(row,idx) {
      if (idx === 0) {
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${'RESULT'.padEnd(colSizes[0])} |`
                                    + ` ${'SOURCE SCHEMA'.padStart(colSizes[1])} |`
                                    + ` ${'TARGET SCHEMA'.padStart(colSizes[2])} |` 
                                    + ` ${'TABLE_NAME'.padStart(colSizes[3])} |`
                                    + ` ${'ROWS'.padStart(colSizes[4])} |`
                                    + ` ${'ELAPSED TIME'.padStart(colSizes[5])} |`
                                    + ` ${'THROUGHPUT'.padStart(colSizes[6])} |`
                           + '\n');
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${'SUCCESSFUL'.padEnd(colSizes[0])} |`
                                    + ` ${row[0].padStart(colSizes[1])} |`
                                    + ` ${row[1].padStart(colSizes[2])} |`)
      }
      else {
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${''.padEnd(colSizes[0])} |`
                                    + ` ${''.padStart(colSizes[1])} |`
                                    + ` ${''.padStart(colSizes[2])} |` )
      }

      this.yadamuLogger.writeDirect(` ${row[2].padStart(colSizes[3])} |` 
                                  + ` ${row[3].toString().padStart(colSizes[4])} |` 
                                  + ` ${YadamuLibrary.stringifyDuration(parseInt(row[4])).padStart(colSizes[5])} |` 
                                  + ` ${(row[5] === 'NaN/s' ? '' : row[5]).padStart(colSizes[6])} |` 
                         + '\n');
    },this)
        
    if (report.successful.length > 0) {
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
      
    seperatorSize = (colSizes.length * 3) - 1;
    colSizes.forEach(function(size) {
      seperatorSize += size;
7   },this);
   
    const notesIdx = colSizes.length-2
    const lineSize = colSizes[notesIdx+1]
   
    report.failed.forEach(function(row,idx) {

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
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamuLogger.writeDirect(`|`
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
        this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${'FAILED'.padEnd(colSizes[0])} |`
                                    + ` ${row[0].padStart(colSizes[1])} |`
                                    + ` ${row[1].padStart(colSizes[2])} |`) 
      }
      else {
        this.yadamuLogger.writeDirect(`|`
                                    + ` ${''.padEnd(colSizes[0])} |`
                                    + ` ${''.padStart(colSizes[1])} |`
                                    + ` ${''.padStart(colSizes[2])} |`)
      }
                
      this.yadamuLogger.writeDirect(` ${row[2].padStart(colSizes[3])} |` 
                                  + ` ${row[3].toString().padStart(colSizes[4])} |` 
                                  + ` ${row[4].toString().padStart(colSizes[5])} |` 
                                  + ` ${row[5].toString().padStart(colSizes[6])} |` 
                                  + ` ${row[6].toString().padStart(colSizes[7])} |` 
                                  + ` ${(row[7] !== null ? row[7] :  '').padEnd(colSizes[8])} |` 
                         + '\n');

                               
      lines.forEach(function(line) {
        this.yadamuLogger.writeDirect(`|`
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
      },this)          

    },this)
      
    if (report.failed.length > 0) {
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
	
    this.yadamuLogger.info([`${this.constructor.name}.compareSchemas()`],` Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);

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
  
    if (!this.yadamuLogger.loggingToConsole()) {
      
      const colSizes = [24,128,12]
      let seperatorSize = (colSizes.length * 3) - 1;
      colSizes.forEach(function(size) {
        seperatorSize += size;
      },this);
    
      this.yadamuLogger.writeDirect('\n+' + '-'.repeat(seperatorSize) + '+' + '\n') 
     
      this.yadamuLogger.writeDirect(`| ${'TIMESTAMP'.padEnd(colSizes[0])} |`
                                  + ` ${'OPERATION'.padEnd(colSizes[1])} |`
                                  + ` ${'ELASPED TIME'.padStart(colSizes[2])} |`     
                         + '\n');
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
      
      this.yadamuLogger.writeDirect(`| ${new Date().toISOString().padEnd(colSizes[0])} |`
                                  + ` ${(operationsList[0] + ' --> ' + operationsList[operationsList.length-1]).padEnd(colSizes[1])} |`
                                  + ` ${(elapsedTime.toString()+"ms").padStart(colSizes[2])} |` 
                         + '\n');
                 
      this.yadamuLogger.writeDirect('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
    }
    else {
      this.yadamuLogger.info([`${this.constructor.name}.dbRoundtrip()`,`OPERATION`],`Completed: Source:[${operationsList[0]}] -->  ${(operationsList.length === 3 ? '[' + operationsList[1] + '] --> ' : '')}Target:${operationsList[operationsList.length-1]}]. Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
    }
  
  }

  async dbRoundtrip(configuration,test,targetConnectionName,parameters,operation) {

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
  
    const timings = []
    const operationsList = []
    let tableMappings = {}
	
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
	
	const sourceDescription = this.getDescription(sourceDatabase,sourceSchema, operation)  
    const targetDescription = this.getDescription(targetDatabase,targetSchema, operation)
	const compareDescription = this.getDescription(sourceDatabase,targetSchema)

    const sourceParameters  = Object.assign({},parameters)
    const compareParameters = Object.assign({},parameters)
	
    this.setUser(sourceParameters,'FROM_USER',sourceDatabase, sourceSchema)
    this.setUser(compareParameters,'TO_USER', sourceDatabase, targetSchema)
	
	// If the ssource connection and target connection reference the same server perform a single DDL_AND_DATA copy between the source schema and target schema.
  	
	const mode =  (sourceConnection === targetConnection) ? "DDL_AND_DATA" : "DDL_ONLY"
    sourceParameters.MODE = mode;
    compareParameters.MODE = mode;

	// Clone the Source Schema to the CompareSchema
	  
    let sourceDBI = this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)
    let compareDBI = this.getDatabaseInterface(sourceDatabase,sourceConnection,compareParameters,true) 
    const startTime = performance.now();
    timings.push(await this.yadamu.pumpData(sourceDBI,compareDBI));
    const elapsedTime = performance.now() - startTime	
    operationsList.push(`"${sourceConnectionName}"://${sourceDescription}`)
    operationsList.push(`"${sourceConnectionName}"://${compareDescription}`)
    let sourceVersion = sourceDBI.dbVersion;
    let targetVersion = compareDBI.dbVersion;

    this.printResults('dbRoundtrip',`"${sourceConnectionName}"://${sourceDescription}`,`"${sourceConnectionName}"://${compareDescription}`,elapsedTime)	
	
	// Test current value of MODE. If  the value is DDL_ONLY then the previous operation cloned the structure of the source schema into the compare schema.
	// Now copy the data from the source schema to the target schema  and then from the target schema to the compare schema
	
	if (mode === 'DDL_ONLY') {
	  operationsList.length = 0
      const targetParameters  = Object.assign({},parameters)
      this.setUser(targetParameters,'TO_USER', targetDatabase, targetSchema)
      sourceParameters.MODE = "DATA_ONLY"
  	  targetParameters.MODE = "DATA_ONLY"
	  compareParameters.MODE = "DATA_ONLY"
      const targetDescription = this.getDescription(targetDatabase,targetSchema, operation)
  	  
	  // Copy Source Schema to Target 
	  
      sourceDBI = this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)
      let targetDBI = this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,true) 
	  
      this.propogateTableMatching(sourceDBI,targetDBI);
      let startTime = performance.now();
      timings.push(await this.yadamu.pumpData(sourceDBI,targetDBI));
      let elapsedTime = performance.now() - startTime
      targetVersion = targetDBI.dbVersion
      tableMappings = targetDBI.reverseTableMappings();
      operationsList.push(`"${sourceConnectionName}"://${sourceDescription}`)
      operationsList.push(`"${targetConnectionName}"://${targetDescription}`)
      this.printResults('dbRoundtrip',`"${sourceConnectionName}"://${sourceDescription}`,`"${targetConnectionName}"://${targetDescription}`,elapsedTime)

      // Copy Target Schema to the Comparse Schema
	  
  	  this.setUser(sourceParameters,'FROM_USER', targetDatabase, targetSchema)
  	  
	  targetDBI = this.getDatabaseInterface(targetDatabase,targetConnection,sourceParameters,false)
      compareDBI = this.getDatabaseInterface(sourceDatabase,sourceConnection,compareParameters,false,tableMappings)
      startTime = performance.now();
      timings.push(await this.yadamu.pumpData(targetDBI,compareDBI));
      elapsedTime = performance.now() - startTime
      operationsList.push(`"${sourceConnectionName}"://${compareDescription}`)
      this.printResults('dbRoundtrip',`"${targetConnectionName}"://${targetDescription}`,`"${sourceConnectionName}"://${compareDescription}`,elapsedTime)

    }    
	
    const opsElapsedTime =  performance.now() - startTime

    this.fixupTimings(timings);   
    const testParameters = {} // parameters ? Object.assign({},parameters) : {}
	const compareParms = this.getCompareParameters(sourceDatabase,sourceVersion,targetDatabase,targetVersion,testParameters)
	// Compare requires Table Mappings in order to process timings correctly
	await this.compareSchemas(sourceDatabase, sourceConnection, compareParms, sourceSchema, targetDatabase, compareSchema, timings[timings.length-1], false, tableMappings)
    this.dbRoundtripResults(operationsList,opsElapsedTime)
	
  }
  
  supportsDDLGeneration(instance) {
	do { 
	  if (Object.getOwnPropertyDescriptor(instance,'getDDLOperations')  !== null) return instance.constructor.name;
	} while ((instance = Object.getPrototypeOf(instance)))
  }
  
  async fileRoundtrip(configuration,test,targetConnectionName,parameters,operation) {
  
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
  
    const timings = []
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
	
	const sourceFile = path.join(sourceConnection.directory,operation.source.directory,operation.source.file).replace('%connection%',targetConnectionName).replace('%vendor%',targetDatabase).replace('%mode%',parameters.MODE);		
	const connectionPath = this.yadamu.getYadamuTestDefaults().DDL_SUPPORTED.includes(targetDatabase) ? path.join(targetConnectionName,parameters.MODE) : targetConnectionName
    const targetDirectory = path.normalize(test.directory.replace('%sourceDirectory%',operation.source.directory).replace('%connection%',connectionPath).replace('%vendor%',targetDatabase).replace('%mode%',parameters.MODE));
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

	const opsStartTime = performance.now();

	// Source File to Target Schema #1
	
	let sourceParameters  = Object.assign({},parameters)
	sourceParameters.FILE = sourceFile
    let fileReader = new FileDBI(this.yadamu);
	fileReader.setParameters(sourceParameters);
    let sourceDescription = this.getDescription(sourceDatabase,operation.source)  

    let targetParameters  = Object.assign({},parameters)
    this.setUser(targetParameters,'TO_USER',targetDatabase, targetSchema1)
	let targetDBI = this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,true)

    let targetDescription = this.getDescription(targetDatabase,targetSchema1)
	
	let startTime = opsStartTime
	
	if (test.parser === 'SQL') {
	  targetDBI.setParameters({"FILE" : sourceFile})
	  timings.push(await this.yadamu.uploadData(targetDBI))
	}
	else {
  	  timings.push(await this.yadamu.pumpData(fileReader,targetDBI))
    }
	
    let elapsedTime = performance.now() - startTime
	let targetVersion = targetDBI.dbVersion
	let tableMappings = targetDBI.reverseTableMappings();
    this.printResults('fileRoundTrip',`file://${sourceFile}`,`"${targetConnectionName}"://${targetDescription}`,elapsedTime)
	
	// If the source schema for the export file came from the target database compare the imported schema with the source schema.
	
    let compareStartTime = performance.now();
    if (operation.source.directory.indexOf(targetConnectionName) === 0) {
      this.fixupTimings(timings);   
      const testParameters = {} // parameters ? Object.assign({},parameters) : {}
	  const compareParms = this.getCompareParameters(targetDatabase,targetVersion,targetDatabase,targetVersion,testParameters)
	  await this.compareSchemas(targetDatabase, targetConnection, compareParms, operation.target, targetDatabase, targetSchema1, timings[timings.length-1],false,tableMappings)
    }
	else {
	  const compareDBI = this.getDatabaseInterface(targetDatabase,targetConnection,{},false)
      await compareDBI.initialize();
	  this.reportRowCounts(await compareDBI.getRowCounts(targetSchema1),timings[timings.length-1],parameters) 
      await compareDBI.finalize();
    }	
    let compareElapsedTime = (performance.now() - compareStartTime);
	
	
	sourceParameters  = Object.assign({},parameters)
    this.setUser(sourceParameters,'FROM_USER',targetDatabase, targetSchema1)
	let sourceDBI = this.getDatabaseInterface(targetDatabase,targetConnection,sourceParameters,false)
    sourceDescription = this.getDescription(targetDatabase,targetSchema1)

	targetParameters  = Object.assign({},parameters)
	targetParameters.FILE = file1;
    let fileWriter = new FileDBI(this.yadamu);
	fileWriter.setParameters(targetParameters);
	fileWriter.setTableMappings(tableMappings)
    targetDescription = this.getDescription(sourceDatabase,{"file" : filename1})  

	startTime = performance.now();
  	timings.push(await this.yadamu.pumpData(sourceDBI,fileWriter))
    elapsedTime = performance.now() - startTime
    this.printResults('fileRoundTrip',`"${targetConnectionName}"://${sourceDescription}`,`file://${file1}`,elapsedTime)

	// File#1 to Target Schema #2
	
	sourceParameters  = Object.assign({},parameters)
	sourceParameters.FILE = file1;
    fileReader = new FileDBI(this.yadamu);
	fileReader.setParameters(sourceParameters);
    sourceDescription = this.getDescription(sourceDatabase,{"file" : filename1})  

    targetParameters  = Object.assign({},parameters)
    this.setUser(targetParameters,'TO_USER',targetDatabase, targetSchema2)
	targetDBI = this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,true)
    targetDescription = this.getDescription(targetDatabase,targetSchema2)
	
	startTime = performance.now();
	
  	if (test.parser === 'SQL') {
	  targetDBI.setParameters({"FILE" : file1})
	  timings.push(await this.yadamu.uploadData(targetDBI))
	}
	else {
  	  timings.push(await this.yadamu.pumpData(fileReader,targetDBI))
    }
	
    elapsedTime = performance.now() - startTime
	targetVersion = targetDBI.dbVersion
	tableMappings = targetDBI.reverseTableMappings();
    this.printResults('fileRoundTrip',`file://${file1}`,`"${targetConnectionName}"://${targetDescription}`,elapsedTime)

    // Compare Target Schema #1 and Target Schema #2
	
	compareStartTime = performance.now();
    this.fixupTimings(timings);   
    const testParameters = {} // parameters ? Object.assign({},parameters) : {}
	const compareParams = this.getCompareParameters(targetDatabase,targetVersion,targetDatabase,targetVersion,testParameters)
	await this.compareSchemas(targetDatabase, targetConnection, compareParams, targetSchema1, targetDatabase, targetSchema2, timings[timings.length-1],false,tableMappings)
	compareElapsedTime = compareElapsedTime + (performance.now() - compareStartTime);

	// Target Schema #2 to File#2
	
	sourceParameters  = Object.assign({},parameters)
    this.setUser(sourceParameters,'FROM_USER',targetDatabase, targetSchema2)
	sourceDBI = this.getDatabaseInterface(targetDatabase,targetConnection,sourceParameters,false)
    sourceDescription = this.getDescription(targetDatabase,targetSchema2)

	targetParameters  = Object.assign({},parameters)
	targetParameters.FILE = file2;
    fileWriter = new FileDBI(this.yadamu);
	fileWriter.setParameters(targetParameters);
	fileWriter.setTableMappings(tableMappings)
    targetDescription = this.getDescription(sourceDatabase,{"file" : filename2})  

	startTime = performance.now();
  	timings.push(await this.yadamu.pumpData(sourceDBI,fileWriter))
    elapsedTime = performance.now() - startTime
    this.printResults('fileRoundTrip',`"${targetConnectionName}"://${sourceDescription}`,`file://${file2}`,elapsedTime)
	
	this.operationsList.push(`file://${file2}`);
	
	const opsElapsedTime =  performance.now() - opsStartTime - compareElapsedTime
	
	const fileCompareParms = this.getCompareParameters(targetDatabase,targetVersion,'file',0,testParameters)
    const fileCompare = this.getDatabaseInterface('file',{},fileCompareParms,false,tableMappings)
	await fileCompare.compareFiles(this.yadamuLoggger,sourceFile,file1, file2, timings)

    this.yadamuLogger.info([`${this.constructor.name}.fileRoundtrip()`,`${test.parser === 'SQL' ? 'SQL' : 'CLARINET'}`],`Operation complete: [${this.operationsList[0]}] -->  [${this.operationsList[1]}] --> [${this.operationsList[2]}] --> [${this.operationsList[3]}]  --> [${this.operationsList[4]}]. Elapsed Time: ${YadamuLibrary.stringifyDuration(opsElapsedTime)}s.`);

  }

  async import(configuration,test,targetConnectionName,parameters,operation) {
	
	const sourceConnectionName = test.source
		
    const sourceConnectionInfo = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnectionInfo = this.getConnection(configuration.connections,targetConnectionName)

    const sourceDatabase =  Object.keys(sourceConnectionInfo)[0];
    const targetDatabase =  Object.keys(targetConnectionInfo)[0];

    const sourceConnection = sourceConnectionInfo[sourceDatabase]
	const targetConnection = targetConnectionInfo[targetDatabase]

    const targetSchema  = this.getTargetSchema(targetDatabase,operation.target,operation); 
    const sourceDescription = this.getDescription(sourceDatabase,operation.source,operation)  
    const targetDescription = this.getDescription(targetDatabase,targetSchema, operation)
     
	const sourceParameters  = Object.assign({},parameters)
    const sourceFile = path.join(sourceConnection.directory.replace('%connection%',targetConnectionName).replace('%vendor%',targetDatabase).replace('%mode%',parameters.MODE),operation.source.file)
	sourceParameters.FILE = sourceFile
    const fileReader = new FileDBI(this.yadamu);
	fileReader.setParameters(sourceParameters);

    const targetParameters  = Object.assign({},parameters)
    this.setUser(targetParameters,'TO_USER',targetDatabase, operation.target, operation)
	const targetDBI = this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,(operation.recreateTarget || test.recreateSchemas))
	const startTime = performance.now();
	
	let timings
	if (test.parser === 'SQL') {
	  targetDBI.setParameters({"FILE" : sourceFile})
	  timings = await this.yadamu.uploadData(targetDBI)
	}
	else {
  	  timings = await this.yadamu.pumpData(fileReader,targetDBI)
    }
	
    const opsElapsedTime = performance.now() - startTime
	const tableMappings = targetDBI.reverseTableMappings();
	
	if ((operation.hasOwnProperty('verify')) && (parameters.MODE !== 'DDL_ONLY')) {
	  const compareDBI = this.getDatabaseInterface(targetDatabase,targetConnection,targetParameters,false)
      await compareDBI.initialize();
	  this.reportRowCounts(await compareDBI.getRowCounts(targetSchema),timings,parameters) 
      await compareDBI.finalize();
    }		

    this.printResults('import',`"file://${sourceFile}`,`"${targetConnectionName}"://${targetDescription}`,opsElapsedTime)
	return path.dirname(sourceFile);
      
  }
 
  async export(configuration,test,targetConnectionName,parameters,operation) {
	
    const timings = []

	const sourceConnectionName = test.source
	
    const sourceConnectionInfo = this.getConnection(configuration.connections,sourceConnectionName)
    const targetConnectionInfo = this.getConnection(configuration.connections,targetConnectionName)

    const sourceDatabase =  Object.keys(sourceConnectionInfo)[0];
    const targetDatabase =  Object.keys(targetConnectionInfo)[0];

    const sourceConnection = sourceConnectionInfo[sourceDatabase]
	const targetConnection = targetConnectionInfo[targetDatabase]
 
    const sourceDescription = this.getDescription(sourceDatabase,operation.source)  
    const targetDescription = this.getDescription(targetDatabase,operation.target)

    const sourceParameters  = Object.assign({},parameters)
    this.setUser(sourceParameters,'FROM_USER',sourceDatabase, operation.source,operation)
    const sourceDBI = this.getDatabaseInterface(sourceDatabase,sourceConnection,sourceParameters,false)

	const targetParameters  = Object.assign({},parameters)
    const targetFile = path.join(targetConnection.directory.replace('%connection%',sourceConnectionName).replace('%vendor%',sourceDatabase).replace('%mode%',parameters.MODE),operation.target.file)
	targetParameters.FILE = targetFile
    const fileWriter = new FileDBI(this.yadamu);
	fileWriter.setParameters(targetParameters);
	
	const startTime = performance.now();
  	timings.push(await this.yadamu.pumpData(sourceDBI,fileWriter));
    const elapsedTime = performance.now() - startTime
    const sourceVersion = sourceDBI.dbVersion;
	const opsElapsedTime = performance.now() - startTime
		
	if (operation.verify) {
      const sourceParameters  = Object.assign({},parameters)
      sourceParameters.FILE = targetFile
	  const fileReader = new FileDBI(this.yadamu);
	  fileReader.setParameters(sourceParameters);
	  
  	  const targetParameters  = Object.assign({},parameters)
      this.setUser(targetParameters,'TO_USER', sourceDatabase, operation.verify, operation)
	  const targetDBI = this.getDatabaseInterface(sourceDatabase,sourceConnection,targetParameters,true)

      const startTime = performance.now();
  	  timings.push(await this.yadamu.pumpData(fileReader,targetDBI));
      const elapsedTime = performance.now() - startTime
	  const verifyDescription = this.getDescription(sourceDatabase,operation.verify) 
      this.printResults('verify',`file://${targetFile}`,`"${sourceConnectionName}"://${verifyDescription}`,elapsedTime)
      if (parameters.MODE !== 'DDL_ONLY') {
		// Report rows Imported and Compare Schemas..
        const compareParms = this.getCompareParameters(sourceDatabase,sourceVersion,targetDatabase,0,parameters)
  	    await this.compareSchemas(sourceDatabase, sourceConnection, compareParms, operation.source, sourceDatabase, operation.verify, timings[timings.length-1],true)
      } 
    }
      
    this.printResults('export',`"${sourceConnectionName}"://${sourceDescription}`,`file://${targetFile}`,opsElapsedTime)
    return path.dirname(targetFile);
  }
  
  getTaskList(configuration,task) {
	if (typeof task === 'string') {
	  if (this.expandedTaskList[task] === undefined) {
        this.expandedTaskList[task] = []
	    if (Array.isArray(configuration.tasks[task])) {
		  for (const subTask of configuration.tasks[task]) {
	        this.expandedTaskList[task] = this.expandedTaskList[task].concat(this.getTaskList(configuration,subTask))
		  }
	    }
	    else {
		  const taskList = configuration.tasks[task]
	 	  if (taskList === undefined) {
		    throw new ConfigurationFileError(`Named task "${task}" not defined. Valid values: "${Object.keys(configuration.tasks)}".`);
	      }
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
      return [task]
    }
  }

  reverseOperation(operation) {
    const reversedOperation = Object.assign({},operation);
	reversedOperation.source = operation.target;
	reversedOperation.target = operation.source;
	return reversedOperation
  }
	  
  async doTests(configuration) {

    const startTime = performance.now()
    let mode = configuration.operation.toUpperCase()

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
	    for (const target of targets) {
		  let targetDescription = target
          const targetConnection = configuration.connections[target]
		  const startTime = performance.now()
          for (const task of test.tasks) {
    	    try {
		      const operations = this.getTaskList(configuration,task)
		      const startTime = performance.now()
		      for (const operation of operations) {
                let mode = (test.operation ? test.operation : configuration.operation).toUpperCase()
			    switch (mode) {
 		          case 'EXPORT':
  		            targetDescription = 'file://' + await this.export(configuration,test,target,testParameters,test.reverseOperations ? this.reverseOperation(operation) : operation)
  				    break;
 		          case 'IMPORT':
    		        sourceDescription = 'file://' + await this.import(configuration,test,target,testParameters,test.reverseOperations ? this.reverseOperation(operation) : operation)
				    break;
			      case 'DBROUNDTRIP':
  			        await this.dbRoundtrip(configuration,test,target,testParameters,test.reverseOperations ? this.reverseOperation(operation) : operation)
			        break;
			      case 'FILEROUNDTRIP':
			        await this.fileRoundtrip(configuration,test,target,testParameters,test.reverseOperations ? this.reverseOperation(operation) : operation)
			        break;
			    }
			  }
              const elapsedTime = performance.now() - startTime;
              this.yadamuLogger.log([`TEST`,`TASK`,`Completed`],`Source:[${sourceDescription}]. Target:[${targetDescription}]. Task:[${task}]. Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
		    } catch (e) {
			  this.yadamuLogger.logException([`TEST`,mode],e);
			  throw e
			}   
	      }
		  const elapsedTime = performance.now() - startTime;
          this.yadamuLogger.log([`TEST`,mode,`TARGET`,`Completed`],`Source:[${sourceDescription}]. Target:[${targetDescription}]. Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
        }		 
        const elapsedTime = performance.now() - startTime;
        this.yadamuLogger.log([`TEST`,`Completed`],`Source:[${sourceDescription}]. Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
      }
      const elapsedTime = performance.now() - startTime;
      this.yadamuLogger.log([`TEST`,mode,`SUCCESS`],`Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
	} catch (e) {
      const elapsedTime = performance.now() - startTime;
      this.yadamuLogger.error([`TEST`,mode,`FAILED`],`Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s`);
	}  
  }
  
}
 
module.exports = YadamuQA
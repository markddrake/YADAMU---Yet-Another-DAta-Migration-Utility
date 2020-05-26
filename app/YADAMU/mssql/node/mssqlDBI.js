"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;
const { performance } = require('perf_hooks');

/* 
**
** Require Database Vendors API 
**
*/

const sql = require('mssql');

/*
**
** Unlike most Driver's which have a concept of a pool and connections MsSQL uses pool and request. 
** The pool which acts a requestProvider, providing request objects on demand.
** Each request is good for one operation.
**
** When working in parallel Master and Slave instances share the same Pool.
**
** Transactions are managed via a Transaction object. The transaction object owns a connection.
** Each instance of the DBI owns it's own Transaction object. 
** When operations need to be transactional the Transaction object becomes the requestProvider for the duration of the transaction.
**
*/


const YadamuLibrary = require('../../common/yadamuLibrary.js')
const {ConnectionError, MsSQLError} = require('../../common/yadamuError.js')
const YadamuDBI = require('../../common/yadamuDBI.js');
const DBParser = require('./dbParser.js');
const TableWriter = require('./tableWriter.js');
const StatementGenerator = require('./statementGenerator.js');
const StagingTable = require('./stagingTable.js');

const STAGING_TABLE =  { tableName : '#YADAMU_STAGING', columnName : 'DATA'}

const sqlSystemInformation = 
`select db_Name() "DATABASE_NAME", 
       current_user "CURRENT_USER", 
	   session_user "SESSION_USER", 
	   (
	     select
           SERVERPROPERTY('BuildClrVersion') AS "BuildClrVersion"
          ,SERVERPROPERTY('Collation') AS "Collation"
          ,SERVERPROPERTY('CollationID') AS "CollationID"
          ,SERVERPROPERTY('ComparisonStyle') AS "ComparisonStyle"
          ,SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS "ComputerNamePhysicalNetBIOS"
          ,SERVERPROPERTY('Edition') AS "Edition"
          ,SERVERPROPERTY('EditionID') AS "EditionID"
          ,SERVERPROPERTY('EngineEdition') AS "EngineEdition"
          ,SERVERPROPERTY('HadrManagerStatus') AS "HadrManagerStatus"
          ,SERVERPROPERTY('InstanceDefaultDataPath') AS "InstanceDefaultDataPath"
          ,SERVERPROPERTY('InstanceDefaultLogPath') AS "InstanceDefaultLogPath"
          ,SERVERPROPERTY('InstanceName') AS "InstanceName"
          ,SERVERPROPERTY('IsAdvancedAnalyticsInstalled') AS "IsAdvancedAnalyticsInstalled"
          ,SERVERPROPERTY('IsBigDataCluster') AS "IsBigDataCluster"
          ,SERVERPROPERTY('IsClustered') AS "IsClustered"
          ,SERVERPROPERTY('IsFullTextInstalled') AS "IsFullTextInstalled"
          ,SERVERPROPERTY('IsHadrEnabled') AS "IsHadrEnabled"
          ,SERVERPROPERTY('IsIntegratedSecurityOnly') AS "IsIntegratedSecurityOnly"
          ,SERVERPROPERTY('IsLocalDB') AS "IsLocalDB"
          ,SERVERPROPERTY('IsPolyBaseInstalled') AS "IsPolyBaseInstalled"
          ,SERVERPROPERTY('IsSingleUser') AS "IsSingleUser"
          ,SERVERPROPERTY('IsXTPSupported') AS "IsXTPSupported"
          ,SERVERPROPERTY('LCID') AS "LCID"
          ,SERVERPROPERTY('LicenseType') AS "LicenseType"
          ,SERVERPROPERTY('MachineName') AS "MachineName"
          ,SERVERPROPERTY('NumLicenses') AS "NumLicenses"
          ,SERVERPROPERTY('ProcessID') AS "ProcessID"
          ,SERVERPROPERTY('ProductBuild') AS "ProductBuild"
          ,SERVERPROPERTY('ProductBuildType') AS "ProductBuildType"
          ,SERVERPROPERTY('ProductLevel') AS "ProductLevel"
          ,SERVERPROPERTY('ProductMajorVersion') AS "ProductMajorVersion"
          ,SERVERPROPERTY('ProductMinorVersion') AS "ProductMinorVersion"
          ,SERVERPROPERTY('ProductUpdateLevel') AS "ProductUpdateLevel"
          ,SERVERPROPERTY('ProductUpdateReference') AS "ProductUpdateReference"
          ,SERVERPROPERTY('ProductVersion') AS "ProductVersion"
          ,SERVERPROPERTY('ResourceLastUpdateDateTime') AS "ResourceLastUpdateDateTime"
          ,SERVERPROPERTY('ResourceVersion') AS "ResourceVersion"
          ,SERVERPROPERTY('ServerName') AS "ServerName"
          ,SERVERPROPERTY('SqlCharSet') AS "SqlCharSet"
          ,SERVERPROPERTY('SqlCharSetName') AS "SqlCharSetName"
          ,SERVERPROPERTY('SqlSortOrder') AS "SqlSortOrder"
          ,SERVERPROPERTY('SqlSortOrderName') AS "SqlSortOrderName"
          ,SERVERPROPERTY('FilestreamShareName') AS "FilestreamShareName"
          ,SERVERPROPERTY('FilestreamConfiguredLevel') AS "FilestreamConfiguredLevel"
          ,SERVERPROPERTY('FilestreamEffectiveLevel') AS "FilestreamEffectiveLevel"
         FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
	   ) "SERVER_PROPERTIES",
	   (
	     select
           DATABASEPROPERTYEX(DB_NAME(),'Collation') AS "Collation"
          ,DATABASEPROPERTYEX(DB_NAME(),'ComparisonStyle') AS "ComparisonStyle"
          ,DATABASEPROPERTYEX(DB_NAME(),'Edition') AS "Edition"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAnsiNullDefault') AS "IsAnsiNullDefault"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAnsiNullsEnabled') AS "IsAnsiNullsEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAnsiPaddingEnabled') AS "IsAnsiPaddingEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAnsiWarningsEnabled') AS "IsAnsiWarningsEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsArithmeticAbortEnabled') AS "IsArithmeticAbortEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAutoClose') AS "IsAutoClose"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAutoCreateStatistics') AS "IsAutoCreateStatistics"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAutoCreateStatisticsIncremental') AS "IsAutoCreateStatisticsIncremental"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAutoShrink') AS "IsAutoShrink"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsAutoUpdateStatistics') AS "IsAutoUpdateStatistics"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsClone') AS "IsClone"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsCloseCursorsOnCommitEnabled') AS "IsCloseCursorsOnCommitEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsFulltextEnabled') AS "IsFulltextEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsInStandBy') AS "IsInStandBy"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsLocalCursorsDefault') AS "IsLocalCursorsDefault"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsMemoryOptimizedElevateToSnapshotEnabled') AS "IsMemoryOptimizedElevateToSnapshotEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsMergePublished') AS "IsMergePublished"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsNullConcat') AS "IsNullConcat"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsNumericRoundAbortEnabled') AS "IsNumericRoundAbortEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsParameterizationForced') AS "IsParameterizationForced"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsQuotedIdentifiersEnabled') AS "IsQuotedIdentifiersEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsPublished') AS "IsPublished"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsRecursiveTriggersEnabled') AS "IsRecursiveTriggersEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsSubscribed') AS "IsSubscribed"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsSyncWithBackup') AS "IsSyncWithBackup"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsTornPageDetectionEnabled') AS "IsTornPageDetectionEnabled"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsVerifiedClone') AS "IsVerifiedClone"
          ,DATABASEPROPERTYEX(DB_NAME(),'IsXTPSupported') AS "IsXTPSupported"
          ,DATABASEPROPERTYEX(DB_NAME(),'LastGoodCheckDbTime') AS "LastGoodCheckDbTime"
          ,DATABASEPROPERTYEX(DB_NAME(),'LCID') AS "LCID"
          ,DATABASEPROPERTYEX(DB_NAME(),'MaxSizeInBytes') AS "MaxSizeInBytes"
          ,DATABASEPROPERTYEX(DB_NAME(),'Recovery') AS "Recovery"
          ,DATABASEPROPERTYEX(DB_NAME(),'ServiceObjective') AS "ServiceObjective"
          ,DATABASEPROPERTYEX(DB_NAME(),'ServiceObjectiveId') AS "ServiceObjectiveId"
          ,DATABASEPROPERTYEX(DB_NAME(),'SQLSortOrder') AS "SQLSortOrder"
          ,DATABASEPROPERTYEX(DB_NAME(),'Status') AS "Status"
          ,DATABASEPROPERTYEX(DB_NAME(),'Updateability') AS "Updateability"
          ,DATABASEPROPERTYEX(DB_NAME(),'UserAccess') AS "UserAccess"
          ,DATABASEPROPERTYEX(DB_NAME(),'Version') AS "Version"
          FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
		) "DATABASE_PROPERTIES"`;
	   
const sqlCreateSavePoint = `SAVE TRANSACTION BulkInsert`;

const sqlRestoreSavePoint = `ROLLBACK TRANSACTION BulkInsert`;

class MsSQLDBI extends YadamuDBI {
    
  /*
  **
  ** Local methods 
  **
  */
  
  sqlTableInfo() {
     
    let spatialClause = `concat('"',"COLUMN_NAME",'".${this.spatialSerializer} "',"COLUMN_NAME",'"')`
   
    if (this.parameters.SPATIAL_MAKE_VALID === true) {
      spatialClause = `concat('case when "',"COLUMN_NAME",'".STIsValid = 0 then "',"COLUMN_NAME",'".makeValid().${this.spatialSerializer} else "',"COLUMN_NAME",'".${this.spatialSerializer} "',"COLUMN_NAME",'"')`
    }
      
    return `select t."TABLE_SCHEMA" "TABLE_SCHEMA"
                  ,t."TABLE_NAME"   "TABLE_NAME"
                  ,string_agg(concat('"',c."COLUMN_NAME",'"'),',') within group (order by "ORDINAL_POSITION") "COLUMN_LIST"
                  ,string_agg(concat('"',"DATA_TYPE",'"'),',') within group (order by "ORDINAL_POSITION") "DATA_TYPES"
                  ,string_agg(concat('"',"COLLATION_NAME",'"'),',') within group (order by "ORDINAL_POSITION") "COLLATION_NAMES"
                  ,string_agg(case
                                 when ("NUMERIC_PRECISION" is not null) and ("NUMERIC_SCALE" is not null) 
                                   then concat('"',"NUMERIC_PRECISION",',',"NUMERIC_SCALE",'"')
                                 when ("NUMERIC_PRECISION" is not null) 
                                   then concat('"',"NUMERIC_PRECISION",'"')
                                 when ("DATETIME_PRECISION" is not null)
                                   then concat('"',"DATETIME_PRECISION",'"')
                                 when ("CHARACTER_MAXIMUM_LENGTH" is not null)
                                   then concat('"',"CHARACTER_MAXIMUM_LENGTH",'"')
                                 else
                                   '""'
                               end
                              ,','
                             )
                    within group (order by "ORDINAL_POSITION") "SIZE_CONSTRAINTS"
                   ,concat('select ',string_agg(case 
                                                  when "DATA_TYPE" = 'hierarchyid' then
                                                    concat('cast("',"COLUMN_NAME",'" as NVARCHAR(4000)) "',"COLUMN_NAME",'"') 
                                                  when "DATA_TYPE" in ('geography','geometry') then
                                                    ${spatialClause}
                                                  when "DATA_TYPE" = 'datetime2' then
                                                    concat('convert(VARCHAR(33),"',"COLUMN_NAME",'",127) "',"COLUMN_NAME",'"') 
                                                  when "DATA_TYPE" = 'datetimeoffset' then
                                                    concat('convert(VARCHAR(33),"',"COLUMN_NAME",'",127) "',"COLUMN_NAME",'"') 
                                                  when "DATA_TYPE" = 'xml' then
                                                    concat('replace(replace(convert(NVARCHAR(MAX),"',"COLUMN_NAME",'"),''&#x0A;'',''\n''),''&#x20;'','' '') "',"COLUMN_NAME",'"') 
                                                  else 
                                                    concat('"',"COLUMN_NAME",'"') 
                                                end
                                               ,','
                                               ) 
                                     within group (order by "ORDINAL_POSITION")
                           ,' from "',t."TABLE_SCHEMA",'"."',t."TABLE_NAME",'"') "SQL_STATEMENT"
              from "INFORMATION_SCHEMA"."COLUMNS" c, "INFORMATION_SCHEMA"."TABLES" t
             where t."TABLE_NAME" = c."TABLE_NAME"
               and t."TABLE_SCHEMA" = c."TABLE_SCHEMA"
               and t."TABLE_TYPE" = 'BASE TABLE'
               and t."TABLE_SCHEMA" = @SCHEMA
             group by t."TABLE_SCHEMA", t."TABLE_NAME"`;  
  }    

  async testConnection(connectionProperties,parameters) {   
    try {
      this.setConnectionProperties(connectionProperties);
      this.setTargetDatabase();
      const connection = await sql.connect(this.connectionProperties);
      await sql.close();
	  super.setParameters(parameters)
	} catch (e) {
      await sql.close();
	  throw (e)
	} 
  }

  getArgNameList(args) {

	if (args !== undefined) {
      if (args.inputs) {
	    const argList = args.inputs.map(function(input) {
		  return `@${input.name}`
	    },this).join(',')
		return argList
	  }
	}
    return ''
  }		

  async configureConnection() {

    let statement = `SET QUOTED_IDENTIFIER ON`
	let results = await this.executeSQL(statement)
    
    statement = `select CONVERT(NVARCHAR(20),SERVERPROPERTY('ProductVersion')) "DATABASE_VERSION", CONVERT(NVARCHAR(32),DATABASEPROPERTYEX(DB_NAME(),'collation')) "DB_COLLATION"`
    results = await this.executeSQL(statement)
    this.dbVersion =  parseInt(results.recordsets[0][0].DATABASE_VERSION)
	this.dbCollation = results.recordsets[0][0].DB_COLLATION
	
    this.defaultCollation = this.dbVersion < 15 ? 'Latin1_General_100_CS_AS_SC' : 'Latin1_General_100_CS_AS_SC_UTF8';
  }
  
  setTargetDatabase() {  
    if ((this.parameters.MSSQL_SCHEMA_DB) && (this.parameters.MSSQL_SCHEMA_DB !== this.connectionProperties.database)) {
      this.connectionProperties.database = this.parameters.MSSQL_SCHEMA_DB
    }
  }
    
  getTransactionManager() {

    // this.yadamuLogger.trace([`${this.constructor.name}.getTransactionManager()`,this.getSlaveNumber()],``)

	this.transactionInProgress = false;
  	const transaction = new sql.Transaction(this.pool)
    return transaction
  }

  getRequest() {
	let stack
    const self = this
	try {
      const yadamuLogger = this.yadamuLogger	
	  stack = new Error().stack;
	  const request = new sql.Request(this.requestProvider)
      request.on('info',function(infoMsg){ 
        yadamuLogger.info([`${self.DATABASE_VENDOR}`,`MESSAGE`],`${infoMsg.message}`);
      })
	  return request;
	} catch (e) {
	  throw new MsSQLError(e,stack,`sql.Request(${this.requestProvider.constuctor.name})`);
    }
  }
  
  getRequestWithArgs(args) {
	 
	const request = this.getRequest();
	
	if (args !== undefined) {
      if (args.inputs) {
	    args.inputs.forEach(function(input) {
		  request.input(input.name,input.type,input.value)
	    },this)
	  }
	}
	
	return request;
  }
  
  async getPreparedStatement(sqlStatement, dataTypes, incomingSpatialFormat) {
	  
    // this.yadamuLogger.trace([`${this.constructor.name}.getPreparedStatement()`,this.getSlaveNumber()],sqlStatement);
	
	const spatialFormat = incomingSpatialFormat === undefined ? this.spatialFormat : incomingSpatialFormat
	let stack
	let statement
	try {
      stack = new Error().stack;
      statement = new sql.PreparedStatement(this.requestProvider)
      dataTypes.forEach(function (dataType,idx) {
        const column = 'C' + idx;
        switch (dataType.type) {
          case 'bit':
            statement.input(column,sql.Bit);
            break;
          case 'bigint':
            statement.input(column,sql.BigInt);
            break;
          case 'float':
            statement.input(column,sql.Float);
            break;
          case 'int':
            statement.input(column,sql.Int);
            break;
          case 'money':
            // statement.input(column,sql.Money);
            statement.input(column,sql.Decimal(19,4));
            break
          case 'decimal':
            // sql.Decimal ([precision], [scale])
            statement.input(column,sql.Decimal(dataType.length,dataType.scale));
            break;
          case 'smallint':
            statement.input(column,sql.SmallInt);
            break;
          case 'smallmoney':
            // statement.input(column,sql.SmallMoney);
            statement.input(column,sql.Decimal(10,4));
            break;
          case 'real':
            statement.input(column,sql.Real);
            break;
          case 'numeric':
            // sql.Numeric ([precision], [scale])
            statement.input(column,sql.Numeric(dataType.length,dataType.scale));
            break;
          case 'tinyint':
            statement.input(column,sql.TinyInt);
            break;
          case 'char':
            statement.input(column,sql.Char(dataType.length));
            break;
          case 'nchar':
            statement.input(column,sql.NChar(dataType.length));
            break;
          case 'text':
            statement.input(column,sql.Text);
            break;
          case 'ntext':
            statement.input(column,sql.NText);
            break;
          case 'varchar':
            statement.input(column,sql.VarChar(dataType.length));
            break;
          case 'nvarchar':
            statement.input(column,sql.NVarChar(dataType.length));
            break;
          case 'json':
            statement.input(column,sql.NVarChar(sql.MAX));
          case 'xml':
            // statement.input(column,sql.Xml);
            statement.input(column,sql.NVarChar(sql.MAX));
            break;
          case 'time':
            // sql.Time ([scale])
            // statement.input(column,sql.Time(dataType.length));
            statement.input(column,sql.VarChar(32));
            break;
          case 'date':
            // statement.input(column,sql.Date);
            statement.input(column,sql.VarChar(32));
            break;
          case 'datetime':
            // statement.input(column,sql.DateTime);
            statement.input(column,sql.VarChar(32));
            break;
          case 'datetime2':
            // sql.DateTime2 ([scale]
            // statement.input(column,sql.DateTime2());
            statement.input(column,sql.VarChar(32));
            break;
          case 'datetimeoffset':
            // sql.DateTimeOffset ([scale])
            // statement.input(column,sql.DateTimeOffset(dataType.length));
            statement.input(column,sql.VarChar(32));
            break;
          case 'smalldatetime':
            // statement.input(column,sql.SmallDateTime);
            statement.input(column,sql.VarChar(32));
            break;
          case 'uniqueidentifier':
            // statement.input(column,sql.UniqueIdentifier);
            // TypeError: parameter.type.validate is not a function
            statement.input(column,sql.Char(36));
            break;
          case 'variant':
            statement.input(column,sql.Variant);
            break;
          case 'binary':
            statement.input(column,sql.Binary);
            break;
          case 'varbinary':
  	        // Upload images as VarBinary(MAX). Convert data to Buffer. This enables bulk upload and avoids Collation issues...
            // sql.VarBinary ([length])
             statement.input(column,sql.VarBinary(dataType.length));
            break;
          case 'image':
            // statement.input(column,sql.Image);
            statement.input(column,sql.VarBinary(sql.MAX));
            break;
          case 'udt':
            statement.input(column,sql.UDT);
            break;
          case 'geography':
            // statement.input(column,sql.Geography)
	        // Upload Geography as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer.
	   	    switch (spatialFormat) {
	   		  case "WKB":
                case "EWKB":
                  statement.input(column,sql.VarBinary(sql.MAX));
	     		  break;
	     		default:
	   	          statement.input(column,sql.VarChar(sql.MAX));
	   	    }
            break;
          case 'geometry':
            // statement.input(column,sql.Geometry);
	   	    // Upload Geometry as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer.
	   	    switch (spatialFormat) {
	   		  case "WKB":
              case "EWKB":
                statement.input(column,sql.VarBinary(sql.MAX));
	   	  	    break;
	   		  default:
	   	        statement.input(column,sql.VarChar(sql.MAX));
	   	    }
            break;
          case 'hierarchyid':
            statement.input(column,sql.VarChar(4000));
            break;
          default:
            this.yadamuLogger.info([`${this.constructor.name}.createstatement()`],`Unmapped data type [${dataType.type}].`);
        }
      },this)
	  
	  stack = new Error().stack;
	  await statement.prepare(sqlStatement);
	  return statement;
	} catch (e) {
	  try {
        await statement.unprepare();
	  } catch (e) {}
	  throw new MsSQLError(e,stack,`sql.PreparedStatement(${sqlStatement}`);
    }
  }

  async createConnectionPool() {
	  
    // this.yadamuLogger.trace([this.DATABASE_VENDOR],`createConnectionPool()`)
  
	this.setTargetDatabase();
    this.logConnectionProperties();

	let stack
	let operation                                                                        
    const self = this
	try {
      const sqlStartTime = performance.now();
      stack = new Error().stack;
	  operation = 'sql.connectionPool()'
	  this.pool = new sql.ConnectionPool(this.connectionProperties)
      this.pool.on('error',(err, p) => {
		if (!self.reconnectInProgress) {
          self.yadamuLogger.logException([`${this.DATABASE_VENDOR}`,`sql.ConnectionPool.onError()`],err);
		}
        throw err
      })
	  
      stack = new Error().stack;
	  operation = 'sql.ConnectionPool.connect()'
	  await this.pool.connect();
      this.traceTiming(sqlStartTime,performance.now())
      this.requestProvider = this.pool;
	  this.transaction = this.getTransactionManager()
	  
	} catch (e) {
	  throw new MsSQLError(e,stack,operation);
    }		

	await this.configureConnection();
  }

  async getDatabaseConnectionImpl() {
	try {
   	  // this.yadamuLogger.trace(this.DATABASE_VENDOR,this.getSlaveNumber()],`getDatabaseConnectionImpl()`)
      await this.createConnectionPool();
	} catch (e) {
      const err = new ConnectionError(e,this.connectionProperties);
	  throw err
	}
  } 
  
  async closeConnection() {

	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getSlaveNumber()],`closeConnection({(this.preparedStatement !== undefined ),${this.transactionInProgress})`)

    if (this.preparedStatement !== undefined ) {
      await this.clearCachedStatement()
    }	
    if (this.transactionInProgress) {
      try {
        await this.rollbackTransaction()
      } catch (e) {
	    if (e.code && (e.code === 'ENOTBEGUN')) {
	      this.yadamuLogger.info([`${this.constructor.name}.sqlection()`],`Incosistent driver state. Transaction in Progress: ${this.transactionInProgress}. Rollback operation raised "${e.message}".`);
        }			
		else {
          throw e
		}
      }
	}  
  }
  
  async closePool() {
  	
  // this.yadamuLogger.trace([this.DATABASE_VENDOR],`closePool(${(this.pool !== undefined)})`)

	if (this.pool !== undefined) {
	  let stack
	  let psudeoSQL
      try {
		stack = new Error().stack
		psudeoSQL = 'MsSQL.Pool.close()'
        await this.pool.close();
		stack = new Error().stack
		psudeoSQL = 'MsSQL.close()'
		await sql.close();
		// Setting pool to undefined seems to cause Error: No connection is specified for that request if a new pool is created.. ### Makes no sense
	    // this.pool = undefined;
      } catch(e) {
	    // this.pool = undefined
	    throw new MsSQLError(e,stack,psudeoSQL)
	  }
    }
  }
 
  async reconnectImpl() {
    await this.pool.connect() 
    this.requestProvider = this.pool
    await this.executeSQL('select 1');
    this.transaction = this.getTransactionManager()
  }
  
  setConnectionProperties(connectionProperties) {
	if (Object.getOwnPropertyNames(connectionProperties).length > 0) {	  
      if (!connectionProperties.options) {
        connectionProperties.options = {}
	  }
      connectionProperties.options.abortTransactionOnError = false
      connectionProperties.options.enableArithAbort = true;
	}
    super.setConnectionProperties(connectionProperties)
  }
  
  async executeBatch(sqlStatment) {

    let attemptReconnect = this.attemptReconnection;

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(sqlStatment))
    }  

    let stack
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		stack = new Error().stack
		const request = await this.getRequest();
        const results = await request.batch(sqlStatment);  
        this.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = new MsSQLError(e,stack,sqlStatment)
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'BATCH OPERATION')
          continue;
        }
        throw cause
      }      
    } 
  }     

  async execute(procedure,args,output) {
     
    let attemptReconnect = this.attemptReconnection;
    const psuedoSQL = `SET @RESULTS = '{}';CALL ${procedure}(${this.getArgNameList(args)}); SELECT @RESULTS "${output}";`

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(psuedoSQL))
    }  

    let stack
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		stack = new Error().stack
		const request = await this.getRequestWithArgs(args);
		const results = await request.execute(procedure);
        this.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = new MsSQLError(e,stack,psuedoSQL);
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'EXECUTE')
          continue;
        }
        throw cause
      }      
    } 
  }
  
  async cachePreparedStatement(sqlStatement,dataTypes,spatialFormat) {
	 const statement = await this.getPreparedStatement(sqlStatement,dataTypes,spatialFormat)
	 this.preparedStatement = {
	   statement         : statement
	 , sqlStatement      : sqlStatement
	 , dataTypes         : dataTypes
	 }
  }
 
  async executeCachedStatement(args) {
	
    let attemptReconnect = this.attemptReconnection;

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(this.preparedStatement.sqlStatement))
    }  

    let stack
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		stack = new Error().stack
	    const results = await this.preparedStatement.statement.execute(args);
        this.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = new MsSQLError(e,stack,sqlStatement);
		if (attemptReconnect && cause.lostConnection()) {
	      this.preparedStatement === undefined;
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'PREPARED STATEMENT')
          this.cachePreparedStatement(this.preparedStatement.sqlStatement,this.preparedStatement.dataTypes);
          continue;
        }
		this.clearCachedStatement();
        throw cause
      }      
    } 
  }

  async clearCachedStatement() {
     // this.yadamuLogger.trace([`${this.constructor.name}.clearCachedStatement()`,this.getSlaveNumber()],this.preparedStatement.sqlStatement)
	 if (this.preparedStatement !== undefined) {
	   await this.preparedStatement.statement.unprepare()
	 }
     this.preparedStatement = undefined;
  }

  async executePreparedStatement(sqlStatement,dataTypes,args) {

    await this.cachePreparedStatement(sqlStatement,dataTypes)
    const results = await this.dbi.executeCachedStatement(args);
	await this.clearCachedStatement()
	return results;
	
  }
	
  async bulkInsert(bulkOperation) {
     
    let attemptReconnect = this.attemptReconnection;
	
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceComment(`Bulk Operation: ${bulkOperation.path}. Rows ${bulkOperation.rows.length}.`))
    }
   
    let stack
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		stack = new Error().stack
		const request = await this.getRequest();
        const results = await request.bulk(bulkOperation);
        this.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = new MsSQLError(e,stack,`BULK INSERT ${bulkOperation.path}. Rows${bulkOperation.rows.length}`)
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'BULK INSERT')
          continue;
        }
        throw cause
      }      
    } 
  }

  async executeSQL(sqlStatment,args,noReconnect) {

    let attemptReconnect = this.attemptReconnection;

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(sqlStatment))
    }  

	let stack
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		stack = new Error().stack
		const request = this.getRequestWithArgs(args)
        const results = await request.query(sqlStatment);  
        this.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = new  MsSQLError(e,stack,sqlStatment);
		if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'SQL')
          continue;
        }
        throw cause
      }      
    } 
  }     
 
  async executeDDLImpl(ddl) {
    
    await this.beginTransaction()     

    await this.createSchema(this.parameters.TO_USER);
    // Cannot use Promise.all with mssql Transaction class
    for (let ddlStatement of ddl) {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
      try {
		// May need to use executeBatch if we support SQL Server 2000.
        const results = await this.executeSQL(ddlStatement);
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}.executeDDL()`],e)
        this.yadamuLogger.writeDirect(`${ddlStatement}\n`)
      } 
    }

    await this.commitTransaction()      

  }
    
  async verifyDataLoad(request,tableSpec) {    
    const statement = `select ISJSON("${tableSpec.columnName}") "VALID_JSON" from "${tableSpec.tableName}"`;
    const results = await this.executeSQL(statement);  
    this.yadamuLogger.info([`${this.constructor.name}.verifyDataLoad()`],`: Upload succesful: ${results.recordsets[0][0].VALID_JSON === 1}. Elapsed time ${performance.now() - startTime}ms.`);
    return results;
  }
  
  async createSchema(schema) {
    
    if (schema !== 'dbo') {
      const createSchema = `if not exists (select 1 from sys.schemas where name = N'${schema}') exec('create schema "${schema}"')`;
      try {
		const results = await this.executeSQL(createSchema)
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}.createSchema()`],e)
      }
    }     
  }
  
  decomposeDataType(targetDataType) {
    const dataType = super.decomposeDataType(targetDataType);
    if (dataType.length === -1) {
      dataType.length = sql.MAX;
    }
    return dataType;
  }

  /*
  **
  ** Overridden Methods
  **
  */
   
  get DATABASE_VENDOR()    { return 'MSSQLSERVER' };
  get SOFTWARE_VENDOR()    { return 'Microsoft Corporation' };
  get SPATIAL_FORMAT()     { return this.spatialFormat };
  get DEFAULT_PARAMETERS() { return this.yadamu.getYadamuDefaults().mssql }

  constructor(yadamu) {
    
    super(yadamu,yadamu.getYadamuDefaults().mssql);
    this.requestProvider = undefined;
	this.transaction = undefined;
    this.pool = undefined;
	
	// Allow subclasses to access constants defined by the sql object. Redeclaring the SQL object in a subclass causes strange behavoir
	
	this.sql = sql
    
    sql.on('error',(err, p) => {
      this.yadamuLogger.logException([`${this.DATABASE_VENDOR}`,`mssql.onError()`],err);
      throw err
    })
    
  }

  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  setSpatialSerializer(spatialFormat) {      
    switch (spatialFormat) {
      case "WKB":
        this.spatialSerializer = "STAsBinary()";
        break;
      case "EWKB":
        this.spatialSerializer = "AsBinaryZM()";
        break;
      case "WKT":
        this.spatialSerializer = "STAsText()";
        break;
      case "EWKT":
        this.spatialSerializer = "AsTextZM()";
        break;
     default:
        this.spatialSerializer = "AsBinaryZM()";
    }  
    
  }   
  
  async initialize() {
    await super.initialize(true);   
    this.spatialFormat = this.parameters.SPATIAL_FORMAT ? this.parameters.SPATIAL_FORMAT : super.SPATIAL_FORMAT
    this.setSpatialSerializer(this.spatialFormat);
  }

  getConnectionProperties() {
    return {
      server          : this.parameters.HOSTNAME
    , user            : this.parameters.USERNAME
    , database        : this.parameters.DATABASE
    , password        : this.parameters.PASSWORD
    , port            : parseInt(this.parameters.PORT)
    , requestTimeout  : 2 * 60 * 60 * 10000
    , options         : {
        encrypt: false // Use this if you're on Windows Azure
      , abortTransactionOnError : false
      }
    }
  }

  /*
  **
  **  Gracefully close down the database connection and pool
  **
  */

  async finalize(poolOptions) {
	await super.finalize(poolOptions)
  }

  /*
  **
  **  Abort the database connection and pool.
  **
  */

  async abort() {
    await super.abort(true);
  }

  /*
  **
  ** Begin the current transaction
  **
  */
  
  async beginTransaction() {

    // this.yadamuLogger.trace([`${this.constructor.name}.beginTransaction()`,this.getSlaveNumber()],``)
    	  
    const psuedoSQL = 'begin transaction'
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(psuedoSQL));
    }
	  
	let stack
	try {
      const sqlStartTime = performance.now();
      stack = new Error().stack;
      await this.transaction.begin();
	  this.traceTiming(sqlStartTime,performance.now())
      this.requestProvider = this.transaction
	  super.beginTransaction()
	} catch (e) {
	  throw new MsSQLError(e,stack,'sql.Transaction.begin()');
    }
	
  }

  /*
  ** Commit the current transaction
  **
  **
  */
  
  async commitTransaction() {
	  
    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getSlaveNumber()],``)

    const psuedoSQL = 'commit transaction'
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(psuedoSQL));
    }
	  
	let stack
	try {
      const sqlStartTime = performance.now();
      stack = new Error().stack;
      await this.transaction.commit();
	  this.traceTiming(sqlStartTime,performance.now())
      this.requestProvider = this.pool
	  super.commitTransaction()
	} catch (e) {
	  throw new MsSQLError(e,stack,'sql.Transaction.commit()');
    }
	
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.rollbackTransaction()`,this.getSlaveNumber()],``)

	this.checkConnectionState(cause)

	// If rollbackTransaction was invoked due to encounterng an error and the rollback operation results in a second exception being raised, log the exception raised by the rollback operation and throw the original error.
	// Note the underlying error is not thrown unless the rollback itself fails. This makes sure that the underlying error is not swallowed if the rollback operation fails.
		
    const psuedoSQL = 'rollback transaction'
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(psuedoSQL));
    }
	  
	let stack
	try {
      const sqlStartTime = performance.now();
      stack = new Error().stack;
      await this.transaction.rollback();
	  this.traceTiming(sqlStartTime,performance.now())
      this.requestProvider = this.pool
	  super.rollbackTransaction()
	} catch (e) {
	  let newIssue = new MsSQLError(e,stack,'sql.Transaction.rollback()')
	  this.checkCause(cause,newIssue)
    }	
	
  }
  
  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getSlaveNumber()],``)

    await this.executeSQL(sqlCreateSavePoint);
    super.createSavePoint()
  }
  
  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getSlaveNumber()],``)

    this.checkConnectionState(cause)
	
	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.
	
   	try {
      await this.executeSQL(sqlRestoreSavePoint);
      super.restoreSavePoint()
	} catch (newIssue) {
	  this.checkCause(cause,newIssue)
	}
	
  }
  
  /*
  **
  ** The following methods are used by JSON_TABLE() style import operations  
  **
  */

  /*
  **
  **  Upload a JSON File to the server. Optionally return a handle that can be used to process the file
  **
  */
  
  async uploadFile(importFilePath) {
    
    const stagingTable = new StagingTable(this,STAGING_TABLE,importFilePath,this.status); 
    let results = await stagingTable.uploadFile()
    // results = await this.verifyDataLoad(this.generateRequest(),STAGING_TABLE);
  }
  
  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */


  async processFile(hndl) {
	 
	 const args = { 
	         inputs: [{
				name: 'TARGET_DATABASE', type: sql.VarChar,  value: this.parameters.TO_USER
			 },{
				name: 'DB_COLLATION',    type: sql.VarChar,  value: this.dbCollation  
		     }]
	       }	

     let results = await this.execute('sp_IMPORT_JSON',args,'')		              
	 results = results.recordset;
     const log = JSON.parse(results[0][Object.keys(results[0])[0]])
     super.processLog(log,'OPENJSON',this.status, this.yadamuLogger)
     return log
  }
  
  /*
  **
  ** The following methods are used by the YADAMU DBReader class
  **
  */
  
  /*
  **
  **  Generate the SystemInformation object for an Export operation
  **
  */
  
  async getSystemInformation() {     
  
    const results = await this.executeSQL(sqlSystemInformation)
    const sysInfo =  results.recordsets[0][0];
    const serverProperties = JSON.parse(sysInfo.SERVER_PROPERTIES)	
	const dbProperties = JSON.parse(sysInfo.DATABASE_PROPERTIES)	
	
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()                      
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT
     ,schema             : this.parameters.FROM_USER
     ,exportVersion      : this.EXPORT_VERSION
	 ,sessionUser        : sysInfo.SESSION_USER
	 ,currentUser        : sysInfo.CURRENT_USER
     ,dbName             : sysInfo.DATABASE_NAME
     ,databaseVersion    : serverProperties.ProductVersion
     ,softwareVendor     : this.SOFTWARE_VENDOR
     ,hostname           : serverProperties.MachineName
     ,nodeClient         : {
        version          : process.version
       ,architecture     : process.arch
       ,platform         : process.platform
      }
    ,serverProperties    : serverProperties
	,databaseProperties  : dbProperties
    }
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */

  async getDDLOperations() {
    return undefined
  }
   
  async getSchemaInfo(schemaKey) {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceComment(`@SCHEMA="${this.parameters[schemaKey]}"`))
    }
      
    const statement = this.sqlTableInfo()
    const results = await this.executeSQL(statement, { inputs: [{name: "SCHEMA", type: sql.VarChar, value: this.parameters[schemaKey]}]})
    return results.recordsets[0]
  
  }

  generateMetadata(tableInfo,server) {    
    const metadata = {}
    for (let table of tableInfo) {
      metadata[table.TABLE_NAME] = {
        owner                    : table.TABLE_SCHEMA
       ,tableName                : table.TABLE_NAME
       ,columns                  : table.COLUMN_LIST
       ,dataTypes                : JSON.parse('[' + table.DATA_TYPES + ']')
       ,sizeConstraints          : JSON.parse('[' + table.SIZE_CONSTRAINTS + ']')
	   ,collations               : JSON.parse('[' + table.COLLATION_NAMES + ']')
      }
    }
    return metadata;   
  }

  createParser(tableInfo,objectMode) {
    return new DBParser(tableInfo,objectMode,this.yadamuLogger);
  }  
  
  streamingError(err,stack,tableInfo) {
	 return new MsSQLError(err,stack,tableInfo.SQL_STATEMENT)
  }

  forceEndOnInputStreamError(error) {
	return true;
  }

  async getInputStream(tableInfo,parser) {

    let stack;
	const self = this
    let readFailed = false;
    try {
      // this.yadamuLogger.trace([`${this.constructor.name}.getInputStream()`,this.getSlaveNumber()],tableInfo.TABLE_NAME)
      const readStream = new Readable({objectMode: true });
      readStream._read = function() {};
      stack = new Error().stack;
      const request = this.getRequest();
      request.stream = true // You can set streaming differently for each request
      request.on('row', function(row) {readStream.push(row)})
	  request.on('error',(err, p) => {
        // Conversion to an MsSQLError will occur when the emitted error is processed
  	    // readStream.emit('error',new MsSQLError(err,stack,tableInfo.SQL_STATEMENT));
	    // self.yadamuLogger.trace([`${self.constructor.name}.getInputStream()`,`${request.constructor.name}.onError()`,`${tableInfo.TABLE_NAME}`,`${err.code}`],`Stream Failure: ${err.message}`); 
		// readStream.emit('error',err);
		if  (!readFailed) {
          readStream.destroy(err);
		}
		else {}
		readFailed = true;
      })
      request.on('done',function(result) {readStream.push(null)});
      request.query(tableInfo.SQL_STATEMENT);  
      return readStream;      
	} catch (e) {
	  throw new MsSQLError(e,stack,tableInfo.SQL_STATEMENT);
    }
  }      

  /*
  **
  ** The following methods are used by the YADAMU DBReader class
  **
  */
    
  async generateStatementCache(schema, executeDDL) {
    /* ### OVERRIDE ### Pass additional parameter Database Name */
    const statementGenerator = new StatementGenerator(this, schema, this.metadata, this.systemInformation.spatialFormat, this.batchSize, this.commitSize, this.status, this.yadamuLogger);
    this.statementCache = await statementGenerator.generateStatementCache(executeDDL, this.systemInformation.vendor, this.parameters.MSSQL_SCHEMA_DB ? this.parameters.MSSQL_SCHEMA_DB : this.connectionProperties.database)
  }

  getTableWriter(table) {
    // this.yadamuLogger.trace([`${this.constructor.name}.getTableWriter()`,this.getSlaveNumber()],'table')
    return super.getTableWriter(TableWriter,table)
  }

  async setSlaveConnection() {
    // Override the default implementation provided by YadamuDBI.

    // Use the connection provider (master) pool
    this.pool = this.connectionProvider.pool;
	this.requestProvider = this.pool
	this.transaction = this.getTransactionManager()	
  }
  
  async slaveDBI(slaveNumber) {
	const dbi = new MsSQLDBI(this.yadamu)
	await super.slaveDBI(slaveNumber,dbi)
	return dbi
  }


  tableWriterFactory(tableName) {
    return new TableWriter(this,tableName,this.statementCache[tableName],this.status,this.yadamuLogger)
  }
  
  async getConnectionID() {
	const results = await this.executeSQL(`select @@SPID "SPID"`)
	const pid = results.recordset[0].SPID
    return pid
}  
  
}

module.exports = MsSQLDBI

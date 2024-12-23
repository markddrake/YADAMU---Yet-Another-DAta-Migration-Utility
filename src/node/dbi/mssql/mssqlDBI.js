
import fs                             from 'fs';

import {
  setTimeout 
}                                     from "timers/promises"

import { 
  performance 
}                                     from 'perf_hooks';
							 
import {
  PassThrough
}                                     from 'stream';

import {
  pipeline
}                                     from 'stream/promises';
				 
/* Database Vendors API */                                    

import sql                            from 'mssql';

/*
**
** Unlike most Driver's which have a concept of a pool and connections MsSQL uses pool and request. 
** The pool which acts a requestProvider, providing request objects on demand.
** Each request is good for one operation.
**
** When working in parallel Manager and Worker instances share the same Pool.
**
** Transactions are managed via a Transaction object. The transaction object owns a connection.
** Each instance of the DBI owns it's own Transaction object. 
** When operations need to be transactional the Transaction object becomes the requestProvider for the duration of the transaction.
**
*/

/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

/* Yadamu DBI */                                    
							          							          
import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'
import ExportFileHeader               from '../file/exportFileHeader.js'

import {
  FileError, 
  FileNotFound, 
  DirectoryNotFound
}                                    from '../file/fileException.js'

/* Vendor Specific DBI Implimentation */                                   

import Comparitor                     from './mssqlCompare.js'
import DatabaseError                  from './mssqlException.js'
import DataTypes                      from './mssqlDataTypes.js'
import Parser                         from './mssqlParser.js'
import StatementGenerator             from './mssqlStatementGenerator.js'
import StatementLibrary               from './mssqlStatementLibrary.js'
import OutputManager                  from './mssqlOutputManager.js'
import Writer                         from './mssqlWriter.js'

import StatementGenerator2014         from './2014/mssqlStatementGenerator.js'
import StatementLibrary2014           from './2014/mssqlStatementLibrary.js'
					
import MsSQLConstants                 from './mssqlConstants.js'
import MsSQLFileLoader                from './mssqlFileLoader.js'
import MsSQLReader                    from './mssqlReader.js'

import {
  ConnectionError
}                                     from '../../core/yadamuException.js'


class MsSQLDBI extends YadamuDBI {

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze({
      ...DBIConstants.DBI_PARAMETERS
    , ...MsSQLConstants.DBI_PARAMETERS
    })
	return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return MsSQLDBI.DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
  
  get DB_COLLATION()                  { return this._DB_COLLATION }
    
  // Override YadamuDBI

  get DATABASE_KEY()                  { return MsSQLConstants.DATABASE_KEY};
  get DATABASE_VENDOR()               { return MsSQLConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()               { return MsSQLConstants.SOFTWARE_VENDOR};
  get SQL_COPY_OPERATIONS()           { return true }
  
  get STATEMENT_TERMINATOR()          { return MsSQLConstants.STATEMENT_TERMINATOR };
  get STATEMENT_SEPERATOR()           { return '\ngo\n--\n' }

  // Enable configuration via command line parameters

  get ROW_LIMIT()                     { return this.parameters.ROW_LIMIT             || MsSQLConstants.ROW_LIMIT }
  get SPATIAL_MAKE_VALID()            { return this.parameters.SPATIAL_MAKE_VALID    || MsSQLConstants.SPATIAL_MAKE_VALID }

  get DATABASE_NAME()                 { return this.parameters.DATABASE ? this.parameters.DATABASE : this.CONNECTION_PROPERTIES.database }
  get DEFAULT_COLATION()              { return this.DATABASE_VERSION < 15 ? 'Latin1_General_100_CS_AS_SC' : 'Latin1_General_100_CS_AS_SC_UTF8' }

  static get DEFAULT_STAGING_PLATFORM() { return DBIConstants.LOADER_STAGING[0]}
  get SUPPORTED_STAGING_PLATFORMS()     { return DBIConstants.LOADER_STAGING }
  get SUPPORTED_STAGING_FORMATS()       { return DBIConstants.CSV_FORMAT }
  
  // get TRANSACTION_IN_PROGRESS()       { return super.TRANSACTION_IN_PROGRESS || this.TEDIOUS_TRANSACTION_ISSUE  }
  // set TRANSACTION_IN_PROGRESS(v)      { super.TRANSACTION_IN_PROGRESS = v }

  get EXPECTED_ROLLBACK()             { return this._EXPECTED_ROLLBACK }
  set EXPECTED_ROLLBACK(v)            { this._EXPECTED_ROLLBACK = v }

  get TEDIOUS_TRANSACTION_ISSUE()     { return this._TEDIOUS_TRANSACTION_ISSUE }
  set TEDIOUS_TRANSACTION_ISSUE(v)    { this._TEDIOUS_TRANSACTION_ISSUE = v }

  get BEGIN_TRANSACTION_ISSUE()       { return this._BEGIN_TRANSACTION_ISSUE }
  set BEGIN_TRANSACTION_ISSUE(v)      { this._BEGIN_TRANSACTION_ISSUE = v }

  addVendorExtensions(connectionProperties)  {

    connectionProperties.requestTimeout  = 2 * 60 * 60 * 10000
    connectionProperties.options         = {
      encrypt                        : false // Use this if you're on Windows Azure
    , abortTransactionOnError        : false
    , enableArithAbort               : true
    }
	
	// MsSQL does not fallback to the default port if port is undefined
	
    if (connectionProperties.server === undefined) {
      connectionProperties.server = connectionProperties.host
	}

	if (connectionProperties.port === undefined) {
	  delete connectionProperties.port
	}

	if (connectionProperties.host     === undefined) {
	  delete connectionProperties.host
	}
	
	return connectionProperties

  }
  

  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)
	
	if (manager) {
      this.COMPARITOR_CLASS          = manager.COMPARITOR_CLASS         
      this.DATABASE_ERROR_CLASS      = manager.DATABASE_ERROR_CLASS     
      this.PARSER_CLASS              = manager.PARSER_CLASS             
      this.STATEMENT_GENERATOR_CLASS = manager.STATEMENT_GENERATOR_CLASS
      this.STATEMENT_LIBRARY_CLASS   = manager.STATEMENT_LIBRARY_CLASS  
	  this.OUTPUT_MANAGER_CLASS      = manager.OUTPUT_MANAGER_CLASS     
      this.WRITER_CLASS              = manager.WRITER_CLASS             
	} else {
      this.COMPARITOR_CLASS          = Comparitor
      this.DATABASE_ERROR_CLASS      = DatabaseError
      this.PARSER_CLASS              = Parser
      this.STATEMENT_GENERATOR_CLASS = StatementGenerator
      this.STATEMENT_LIBRARY_CLASS   = StatementLibrary	
	  this.OUTPUT_MANAGER_CLASS      = OutputManager
      this.WRITER_CLASS              = Writer	
	}
	
	this.DATA_TYPES = DataTypes

    this.yadamuRollack = false

	this.BEGIN_TRANSACTION_ISSUE = false;
	
    // Allow subclasses to access constants defined by the sql object. Redeclaring the SQL object in a subclass causes strange behavoir
    this.sql = sql
	
    sql.on('error',(err, p) => {
      this.LOGGER.handleException([this.DATABASE_VENDOR,`mssql.onError()`],err)
      throw err
    })
	
	this.EXPECTED_ROLLBACK         = false
	this.TEDIOUS_TRANSACTION_ISSUE = false
	this.BEGIN_TRANSACTION_ISSUE   = false
  }
  
  getDatabaseException(cause,stack,sql) {
    const err = this.createDatabaseError(cause,stack,sql)
	if (err.cancelledOperation() && this.CANCEL_REQUESTED) {
	  this.CANCEL_REQUESTED = false
	  return err
	}
    return this.trackExceptions(err)
  }
  
  initializeManager() {
	super.initializeManager()
  }	 

   setSchema(schema,key) {
     
	 switch (true) {
	   case (schema.hasOwnProperty('owner')):
	     this.parameters[key] = schema.owner
		 this.parameters.DATABASE = schema.database 
		 break
	   case (schema.hasOwnProperty('database')):
	     this.parameters[key] = schema.schema
		 this.parameters.DATABASE = schema.database 
		 break
	   default:
	     this.parameters[key] = 'dbo'
		 this.parameters.DATABASE = schema.schema
	 }

	this.DESCRIPTION = `:${this.parameters.DATABASE}"."${this.CURRENT_SCHEMA}"`
  }  
  
  getSchema(schema) {
	 switch (true) {
	   case (schema.hasOwnProperty('owner')):
	     return schema
	   case (schema.hasOwnProperty('database')):
	     return { database : schema.database, owner: schema.schema }
	   default:
	     return { database : schema.schema, owner: 'dbo' }
	 }
  }

  /*
  **
  ** Local methods 
  **
  */
1
  async testConnection() {   
    let stack
    try {
      this.setTargetDatabase()
      stack = new Error().stack
	  const connection = await sql.connect(this.CONNECTION_PROPERTIES)
      await sql.close()
    } catch (e) {
      await sql.close()
      throw this.createDatabaseError(e,stack,'testConnection.getConnection()')
    } 
  }

  getArgNameList(args) {

    if (args !== undefined) {
      if (args.inputs) {
        const argList = args.inputs.map((input) => {
          return `@${input.name}`
        }).join(',')
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
    this._DATABASE_VERSION =  parseInt(results.recordsets[0][0].DATABASE_VERSION)
    this._DB_COLLATION = results.recordsets[0][0].DB_COLLATION
    
  }
  
  setTargetDatabase() {  
    if ((this.parameters.DATABASE) && (this.parameters.DATABASE !== this.CONNECTION_PROPERTIES.database)) {
      this.CONNECTION_PROPERTIES.database = this.parameters.DATABASE
    }
  }
  
  reportTransactionState(operation) {
    const e = new Error(`Unexpected ${operation} operation`)
    this.LOGGER.handleWarning([this.DATABASE_VENDOR,this.ROLE,'TRANSACTION MANAGER',operation,this.getWorkerNumber()],this.createDatabaseError(e,e.stack,this.constructor.name))    
  }

  getTransactionManager() {

    // this.LOGGER.trace([`${this.constructor.name}.getTransactionManager()`,this.getWorkerNumber()],``)

    this.TRANSACTION_IN_PROGRESS = false;
    const transaction = new sql.Transaction(this.pool)
    transaction.on('rollback',() => { 
      if (!this.EXPECTED_ROLLBACK) {
        this.TEDIOUS_TRANSACTION_ISSUE = true;
        this.reportTransactionState('ROLLBACK')
      }
    })
    return transaction
  }
  
  async verifyTransactionState() {
    if (this.TEDIOUS_TRANSACTION_ISSUE) {
  	  await this.request.cancel()
	  this.transaction = this.getTransactionManager()
      await this.beginTransaction()
    }
  }
  
  getRequest() {
    let stack
    try {
      stack = new Error().stack;
	  // console.log(this.requestProvider.constructor.name)
      this.request = new sql.Request(this.requestProvider)
      this.CANCEL_REQUESTED = false
      this.request.on('info',(infoMsg) => { 
        this.LOGGER.info([this.DATABASE_VENDOR,`MESSAGE`],`${infoMsg.message}`)
      })
      return this.request;
    } catch (e) {
      throw this.getDatabaseException(e,stack,`sql.Request(${this.requestProvider.constuctor.name})`)
    }
  }
  
  getRequestWithArgs(args) {
     
    const request = this.getRequest()
    
    if (args !== undefined) {
      if (args.inputs) {
        args.inputs.forEach((input) => {
          request.input(input.name,input.type,input.value)
        })
      }
    }
    
    return request;
  }
  
  async getPreparedStatement(sqlStatement, dataTypes, rowSpatialFormat) {
      
    // this.LOGGER.trace([`${this.constructor.name}.getPreparedStatement()`,this.getWorkerNumber()],sqlStatement)

    const spatialFormat = rowSpatialFormat === undefined ? this.SPATIAL_FORMAT : rowSpatialFormat
    let stack
    let statement
	let precision
	try {
      stack = new Error().stack;
      statement = new sql.PreparedStatement(this.requestProvider)
      dataTypes.forEach((dataType,idx) => {
        const length = dataType.length > 0 && dataType.length < 65535 ? dataType.length : sql.MAX
        const column = 'C' + idx;
		switch (dataType.type.toLowerCase()) {
          case this.DATA_TYPES.BOOLEAN_TYPE:
            statement.input(column,sql.Bit)
            break;
          case this.DATA_TYPES.BIGINT_TYPE:
            // statement.input(column,sql.BigInt)
			statement.input(column,sql.VarChar(20))
            break;
          case this.DATA_TYPES.FLOAT_TYPE:
            statement.input(column,sql.Float)
            break;
          case this.DATA_TYPES.INTEGER_TYPE:
            statement.input(column,sql.Int)
            break;
          case this.DATA_TYPES.MSSQL_MONEY_TYPE:
            // statement.input(column,sql.Money)
            statement.input(column,sql.Decimal(19,4))
            break
          case this.DATA_TYPES.DECIMAL_TYPE:
            // sql.Decimal ([precision], [scale])
			precision = dataType.length || 18;
		    if ((precision) > 15) {
   			  statement.input(column,sql.VarChar(precision + 2))
			  break;
			}
            statement.input(column,sql.Decimal(dataType.length || 15,dataType.scale || 0))
            break;
          case this.DATA_TYPES.NUMERIC_TYPE:
            // sql.Numeric ([precision], [scale])
			precision = dataType.length || 18;
		    if ((precision) > 15) {
   			  statement.input(column,sql.VarChar(precision + 2))
			  break;
			}
            statement.input(column,sql.Numeric(dataType.length || 15,dataType.scale || 0))
            break;
          case this.DATA_TYPES.SMALLINT_TYPE:
            statement.input(column,sql.SmallInt)
            break;
          case this.DATA_TYPES.MSSQL_SMALLMONEY_TYPE:
            // statement.input(column,sql.SmallMoney)
            statement.input(column,sql.Decimal(10,4))
            break;
          case this.DATA_TYPES.FLOAT_TYPE:
            statement.input(column,sql.Real)
            break;
          case this.DATA_TYPES.DOUBLE_TYPE:
            statement.input(column,sql.Float)
            break;
          case this.DATA_TYPES.TINYINT_TYPE:
            statement.input(column,sql.TinyInt)
            break;
          case this.DATA_TYPES.CHAR_TYPE:
            statement.input(column,sql.Char(dataType.length))
            break;
          case this.DATA_TYPES.NCHAR_TYPE:
            statement.input(column,sql.NChar(dataType.length))
            break;
          case this.DATA_TYPES.TEXT_TYPE:
            statement.input(column,sql.Text)
            break;
          case this.DATA_TYPES.NEXT_TYPE:
            statement.input(column,sql.NText)
            break;
          case this.DATA_TYPES.VARCHAR_TYPE:
            statement.input(column,sql.VarChar(length))
            break;
          case this.DATA_TYPES.NVARCHAR_TYPE:
            statement.input(column,sql.NVarChar(length))
            break;
          case this.DATA_TYPES.JSON_TYPE:
            // statement.input(column,sql.Xml)
            statement.input(column,sql.NVarChar(sql.MAX))
            break;
          case this.DATA_TYPES.XML_TYPE:
            // statement.input(column,sql.Xml)
            statement.input(column,sql.NVarChar(sql.MAX))
            break;
          case this.DATA_TYPES.TIME_TYPE:
            // sql.Time ([scale])
            // statement.input(column,sql.Time(dataType.length))
            statement.input(column,sql.VarChar(32))
            break;
          case this.DATA_TYPES.DATE_TYPE:
            // statement.input(column,sql.Date)
            statement.input(column,sql.VarChar(32))
            break;
          case this.DATA_TYPES.DATETIME_TYPE:
            // sql.DateTime2 ([scale]
            // statement.input(column,sql.DateTime2())
            statement.input(column,sql.VarChar(32))
            break;
          case this.DATA_TYPES.MSSQL_DATETIME_TYPE:
            // statement.input(column,sql.DateTime)
            statement.input(column,sql.VarChar(32))
            break;
          case this.DATA_TYPES.TIMESTAMP_TYPE:
            // sql.DateTimeOffset ([scale])
            // statement.input(column,sql.DateTimeOffset(dataType.length))
            statement.input(column,sql.VarChar(32))
            break;
          case this.DATA_TYPES.SMALLDATETIME_TYPE:
            // statement.input(column,sql.SmallDateTime)
            statement.input(column,sql.VarChar(32))
            break;
          case this.DATA_TYPES.UUID_TYPE:
            // statement.input(column,sql.UniqueIdentifier)
            // TypeError: parameter.type.validate is not a function
            statement.input(column,sql.Char(36))
            break;
          case this.DATA_TYPES.VARIANT_TYPE:
            statement.input(column,sql.Variant)
            break;
          case this.DATA_TYPES.BINARY_TYPE:
            statement.input(column,sql.Binary(dataType.length))
            break;
          case this.DATA_TYPES.VARBINARY_TYPE:
            // Upload images as VarBinary(MAX). Convert data to Buffer. This enables bulk upload and avoids Collation issues...
            // sql.VarBinary ([length])
             statement.input(column,sql.VarBinary(length))
            break;
          case this.DATA_TYPES.IMAGE_TYPE:
            // statement.input(column,sql.Image)
            statement.input(column,sql.VarBinary(sql.MAX))
            break;
          case this.DATA_TYPES.USER_DEFINED_TYPE:
            statement.input(column,sql.UDT)
            break;
          case this.DATA_TYPES.GEOGRAPHY_TYPE:
            // statement.input(column,sql.Geography)
            // Upload Geography as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer.
            switch (spatialFormat) {
              case "WKB":
              case "EWKB":
                statement.input(column,sql.VarBinary(sql.MAX))
               break;
              default:
                statement.input(column,sql.VarChar(sql.MAX))
            }
            break;
          case this.DATA_TYPES.GEOMETRY_TYPE:
            // statement.input(column,sql.Geometry)
            // Upload Geometry as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer.
            switch (spatialFormat) {
              case "WKB":
              case "EWKB":
                statement.input(column,sql.VarBinary(sql.MAX))
                break;
              default:
                statement.input(column,sql.VarChar(sql.MAX))
            }
            break;
          case this.DATA_TYPES.MSSSQL_HEIRACHY_TYPE:
            statement.input(column,sql.VarChar(4000))
            break;
          default:
            this.LOGGER.warning([this.DATABASE_VENDOR,this.ROLE,`PREPARED STATEMENT`],`Unmapped data type [${dataType.type}].`)
        }
      })
      
      stack = new Error().stack;
      await statement.prepare(sqlStatement)
      return statement;
    } catch (e) {
      try {
        await statement.unprepare()
      } catch (e) {}
      throw this.getDatabaseException(e,stack,`sql.PreparedStatement(${sqlStatement}`)
    }
  }

  async createConnectionPool() {
      
    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE],`createConnectionPool()`)
  
    this.setTargetDatabase()
    this.logConnectionProperties()

    let stack
    let operation                                                                        
    try {
      const sqlStartTime = performance.now()
      stack = new Error().stack;
      operation = 'sql.connectionPool()'
      this.pool = new sql.ConnectionPool(this.CONNECTION_PROPERTIES)
      this.pool.on('error',(err, p) => {
        const cause = err instanceof DatabaseError ? err : this.getDatabaseException(err,stack,`${operation}.onError()`)
        if (!cause.suppressedError())  {
          this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,`sql.ConnectionPool.onError()`],cause)
          if (!this.RECONNECT_IN_PROGRESS) {
            throw cause
          }
        }
      })
      
      stack = new Error().stack;
      operation = 'sql.ConnectionPool.connect()'
      await this.pool.connect()
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      this.requestProvider = this.pool;
      this.transaction = this.getTransactionManager()
      
    } catch (e) {
      throw this.getDatabaseException(e,stack,operation)
    }       

    await this.configureConnection()
  }

  async _getDatabaseConnection() {
	  
    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`_getDatabaseConnection()`)
	
	let stack
    try {
	  stack = new Error().stack
      await this.createConnectionPool()
    } catch (e) {
      const connectionError = new ConnectionError(e,this.redactPasswords())
      throw connectionError 
    }
  } 
  
  async closeConnection(options) {

    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`closeConnection(${(this.preparedStatement !== undefined)},${this.TRANSACTION_IN_PROGRESS})`)
    
    await this.clearCachedStatement()
	
	if (this.ACTIVE_INPUT_STREAM === true) {
	  await this.cancelRequest(true)	
	}
	
    if (this.TRANSACTION_IN_PROGRESS) {
	  // If there is a Transaction Rollback, even if the connection has been lost, else the pool will hang on close.
      const latestError = this.ERROR_STATE.LATEST_ERROR
	  try {
	    // await this.rollbackTransaction(this.ERROR_STATE.LATEST_ERROR)	
	    await this.rollbackTransaction()	
	  } catch(e) {
	    this.ERROR_STATE.LATEST_ERROR = latestError || e
	  }
    }
	
  }
  
  unhandledRejectionHandler(err,p) {
    if (err.code  === 'ENOTOPEN') {
	  err.ignoreUnhandledRejection = true
	}
  }

  async closePool(options) {
    
    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,'CLOSE POOL'],`closePool(${(this.pool !== undefined)})`)
	
	/*
	**
	** ### It appears closePool() can hang if a BEGIN TRANSACTiON operation failed due to a lost connection error
	**
	*/
	
    if (this.pool !== undefined) {
     
	  /*
	  **
 	  ** Set a timer to deal with pool.close() hanging
	  **
	  */
		
	  const timerAbort = new AbortController()
      setTimeout(1000,null,{ref: false, signal: timerAbort.signal}).then(() => {
        this.LOGGER.warning([this.DATABASE_VENDOR,this.ROLE,'CLOSE POOL',this.BEGIN_TRANSACTION_ISSUE],`Close pool operation timed out`)
		// console.dir(this.pool.pool.used,{depth:null})
	  }).catch((e) => { 
	    /* Swallow the Abort */
	  })
	 	 		  
      let stack
      let psudeoSQL
      try {
		if (this.request) {
		  try {
    		await this.cancelRequest(true)
		  } catch(e) {
			this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,'CLOSE POOL'],e)
		  }
		}
			  
        stack = new Error().stack
        psudeoSQL = 'MsSQL.Pool.close()'
		
		/*
		**
        ** Sometimes pool.close() results in an unhandledRejection "ConnectionError: Connection not yet open." 
		** Add an unhandledRejection listener to catch this and prevent it from terminating the process.
		** Remove the listener if no exception is thrown when closing the pool.
		**
		*/
		   
		process.prependOnceListener('unhandledRejection',this.unhandledRejectionHandler)		
		// this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,'CLOSE POOL'],`Closing pool`)
		await this.pool.close()
		this.pool = undefined
		timerAbort.abort()
		// this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,'CLOSE POOL'],`Pool Closed`)
		process.removeListener('unhandledRejection',this.unhandledRejectionHandler)


		// this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,'CLOSE POOL'],`Closing SQL`)
        stack = new Error().stack
        psudeoSQL = 'MsSQL.close()'
        await sql.close()
        // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,'CLOSE POOL'],`SQL Closed`)
        
        /*
		**
		** Setting pool to undefined seems to cause Error: No connection is specified for that request if a new pool is created.. ### Makes no sense
        
		 
		**
		*/
		return
      } catch(e) {
		timerAbort.abort()
        // this.pool = undefined
        this.LOGGER.info([this.DATABASE_VENDOR,this.ROLE],`Error Closing Pool`)
        throw this.getDatabaseException(e,stack,psudeoSQL)
      }
	} 
  }
 
  async _reconnect() {
	await this.pool.connect() 
    this.requestProvider = this.pool
    await this.executeSQL('select 1')
    this.transaction = this.getTransactionManager()
  }
     
  async cancelRequest(expected) {
	
    // this.LOGGER.trace([this.DATABASE_VENDOR,this.ROLE,'CANCEL REQUEST'],`cancelRequest(${(expected)}),${this.request.canceled}`)
    
	if (this.request.canceled === false) {
	  let stack
      try {
		if (!expected) {
	      const e = new Error(`Unexpected Cancel Request`)
          this.LOGGER.handleWarning([this.DATABASE_VENDOR,this.ROLE,'REQUEST MANAGER'],this.createDatabaseError(e,e.stack,this.constructor.name))    
        }
        const sqlStartTime = performance.now()
        stack = new Error().stack;
		this.CANCEL_REQUESTED = true;
        await this.request.cancel()
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      } catch (e) {
        const cause = this.getDatabaseException(e,stack,'sql.request.cancel()')
        throw cause
      }
    }
  }  
   
  isExpectedCancellation (e) {
	// console.log(this.CANCEL_REQUESTED)
    if (this.CANCEL_REQUESTED && (e instanceof DatabaseError) && e.cancelledOperation()) {
	  this.CANCEL_REQUESTED = false
      return true
	}
	else {
	  return false
	}
  }
  
  trackExceptions(err) {
	if (!this.isExpectedCancellation(err)) {
      return super.trackExceptions(err)
	}
	else {
	  return err
	}
  }
  
  createBulkOperation(database, tableName, columnList, dataTypes) {
    
	const table = new sql.Table(database + '.' + this.CURRENT_SCHEMA + '.' + tableName)
    table.create = false
    let precision
    dataTypes.forEach((dataType,idx) => {
      const length = dataType.length > 0 && dataType.length < 65535 ? dataType.length : sql.MAX
      switch (dataType.type.toLowerCase()) {
        case this.DATA_TYPES.BOOLEAN_TYPE:
          table.columns.add(columnList[idx],sql.Bit);
          break;
        case this.DATA_TYPES.BIGINT_TYPE:
		  // Bind as VarChar to avoid rounding issues
          // table.columns.add(columnList[idx],sql.BigInt, {nullable: true});
		  table.columns.add(columnList[idx],sql.VarChar(21), {nullable: true});  
          break;
        case this.DATA_TYPES.FLOAT_TYPE:
          table.columns.add(columnList[idx],sql.Float, {nullable: true});
          break;
        case this.DATA_TYPES.INTEGER_TYPE:
          table.columns.add(columnList[idx],sql.Int, {nullable: true});
          break;
        case this.DATA_TYPES.MSSQL_MONEY_TYPE:
          // table.columns.add(columnList[idx],sql.Money, {nullable: true});
          table.columns.add(columnList[idx],sql.Decimal(19,4), {nullable: true});
          break
        case this.DATA_TYPES.DECIMAL_TYPE:
		  precision = dataType.length || 18
		  if (precision > 15) {
			// Bind as VarChar to avoid rounding issues
			table.columns.add(columnList[idx],sql.VarChar(precision+2), {nullable: true});  
		  }
		  else {
            // sql.Decimal ([precision], [scale])
            table.columns.add(columnList[idx],sql.Decimal(dataType.length || 18,dataType.scale || 0), {nullable: true});
		  }
          break;
        case this.DATA_TYPES.NUMERIC_TYPE:
		  precision = dataType.length || 18
		  if (precision > 15) {
			// Bind as VarChar to avoid rounding issues
			table.columns.add(columnList[idx],sql.VarChar(precision+2), {nullable: true});  
		  }
		  else {
            // sql.Numeric ([precision], [scale])
            table.columns.add(columnList[idx],sql.Numeric(dataType.length || 18,dataType.scale || 0), {nullable: true});
		  }
          break;
        case this.DATA_TYPES.SMALLINT_TYPE:
          table.columns.add(columnList[idx],sql.SmallInt, {nullable: true});
          break;
        case this.DATA_TYPES.MSSQL_SMALLMONEY_TYPE:
          // table.columns.add(columnList[idx],sql.SmallMoney, {nullable: true});
          table.columns.add(columnList[idx],sql.Decimal(10,4), {nullable: true});
          break;
        case this.DATA_TYPES.FLOAT_TYPE:
          table.columns.add(columnList[idx],sql.Real, {nullable: true}, {nullable: true});
          break;
        case this.DATA_TYPES.DOUBLE_TYPE:
          table.columns.add(columnList[idx],sql.Float, {nullable: true}, {nullable: true});
          break;
        case this.DATA_TYPES.TINYINT_TYPE:
          table.columns.add(columnList[idx],sql.TinyInt, {nullable: true});
          break;
        case this.DATA_TYPES.CHAR_TYPE:
          table.columns.add(columnList[idx],sql.Char(length), {nullable: true});
          break;
        case this.DATA_TYPES.NCHAR_TYPE:
          table.columns.add(columnList[idx],sql.NChar(length), {nullable: true});
          break;
        case this.DATA_TYPES.TEXT_TYPE:
          table.columns.add(columnList[idx],sql.Text, {nullable: true});
          break;
        case this.DATA_TYPES.NTEXT_TYPE:
          table.columns.add(columnList[idx],sql.NText, {nullable: true});
          break;
        case this.DATA_TYPES.VARCHAR_TYPE:
          table.columns.add(columnList[idx],sql.VarChar(length), {nullable: true});
          break;
        case this.DATA_TYPES.NVARCHAR_TYPE:
          table.columns.add(columnList[idx],sql.NVarChar(length), {nullable: true});
          break;
        case this.DATA_TYPES.JSON_TYPE:
          table.columns.add(columnList[idx],sql.NVarChar(sql.MAX), {nullable: true});
          break;
        case this.DATA_TYPES.XML_TYPE:
          // Added to Unsupported
          // Invalid column data type for BCP
          table.columns.add(columnList[idx],sql.Xml, {nullable: true});
          break;
        case this.DATA_TYPES.TIME_TYPE:
          // sql.Time ([scale])
          // Binding as sql.Time must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.Time(length), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case this.DATA_TYPES.DATE_TYPE:
          // Binding as sql.Date must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.Date, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case this.DATA_TYPES.DATETIME_TYPE:
          // Binding as sql.DateTime2 must supply values as type Date. 
          // sql.DateTime2 ([scale]
          // table.columns.add(columnList[idx],sql.DateTime2(7), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case this.DATA_TYPES.MSSQL_DATETIME_TYPE:
          // Binding as sql.DateTime must supply values as type Date. 
          // sql.DateTime ([scale]
          // table.columns.add(columnList[idx],sql.DateTime, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case this.DATA_TYPES.TIMESTAMP_TYPE:
          // sql.DateTimeOffset ([scale])
          // Binding as sql.DateTime2 must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.DateTimeOffset(length), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case this.DATA_TYPES.MSSQL_SMALLDATETIME_TYPE:
          // Binding as sql.SamllDateTime must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.SmallDateTime, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case this.DATA_TYPES.UUID_TYPE:
          // table.columns.add(columnList[idx],sql.UniqueIdentifier, {nullable: true});
          // TypeError: parameter.type.validate is not a function
          table.columns.add(columnList[idx],sql.Char(36), {nullable: true});
          break;
        case this.DATA_TYPES.MSSQL_VARIANT_TYPE:
          table.columns.add(columnList[idx],sql.Variant, {nullable: true});
          break;
        case this.DATA_TYPES.BINARY_TYPE:
          table.columns.add(columnList[idx],sql.Binary(length), {nullable: true});
          break;
        case this.DATA_TYPES.VARBINARY_TYPE:
          // sql.VarBinary ([length])
           table.columns.add(columnList[idx],sql.VarBinary(length), {nullable: true});
          break;
        case this.DATA_TYPES.IMAGE_TYPE:
  	      // Upload images as VarBinary(MAX). Convert data to Buffer. This enables BCP operationa and avoids Collation issues..
          // table.columns.add(columnList[idx],sql.Image, {nullable: true});
          table.columns.add(columnList[idx],sql.VarBinary(sql.MAX), {nullable: true});
          break;
        case this.DATA_TYPES.MSSQL_UDT_TYPE:
          table.columns.add(columnList[idx],sql.UDT, {nullable: true});
          break;
        case this.DATA_TYPES.GEOGRAPHY_TYPE:
          // Added to Unsupported
          // TypeError: parameter.type.validate is not a function
          // table.columns.add(columnList[idx],sql.Geography, {nullable: true});
  	      // Upload geography as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer. This enables BCP Operations.
		  switch (this.INBOUND_SPATIAL_FORMAT) {
			case "WKB":
            case "EWKB":
              table.columns.add(columnList[idx],sql.VarBinary(sql.MAX), {nullable: true});
			  break;
			default:
		      table.columns.add(columnList[idx],sql.VarChar(sql.MAX), {nullable: true});
		  }
          break;
        case this.DATA_TYPES.GEOMETRY_TYPE:
          // Added to Unsupported
          // TypeError: parameter.type.validate is not a function
          // table.columns.add(columnList[idx],sql.Geometry, {nullable: true});
  	      // Upload geometry as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer. This enables BCP Operations.
		  switch (this.INBOUND_SPATIAL_FORMAT) {
			case "WKB":
            case "EWKB":
              table.columns.add(columnList[idx],sql.VarBinary(sql.MAX), {nullable: true});
			  break;
			default:
		      table.columns.add(columnList[idx],sql.VarChar(sql.MAX), {nullable: true});
		  }
          break;
        case this.DATA_TYPES.MSSQL_HIERARCHY_ID_TYPE:
          table.columns.add(columnList[idx],sql.VarChar(4000),{nullable: true});
          break;
        default:
          this.LOGGER.warning([this.DATABASE_VENDOR,`BCP`,`"${tableName}"`],`Unmapped data type [${dataType.type}].`);
      }
    })
    return table
  }
 
  releaseBatch(batch) {
	if (Array.isArray(batch.rows)) {
	  batch.rows.length = 0;
	}
  }
  
  async executeBatch(sqlStatment) {

    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.SQL_TRACE.traceSQL(sqlStatment)

    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now()
        stack = new Error().stack
        const request = this.getRequest()
        const results = await request.batch(sqlStatment)  
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
        return results;
      } catch (e) {
        const cause = this.getDatabaseException(e,stack,sqlStatment)
        if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
          // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'EXECUTE BATCH')
          continue;
        }
        throw cause
      }      
    } 
  }     

  async execute(procedure,args,output) {
     
    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    const psuedoSQL = `SET @RESULTS = '{}';CALL ${procedure}(${this.getArgNameList(args)}) SELECT @RESULTS "${output}";`
    this.SQL_TRACE.traceSQL(psuedoSQL)

    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now()
        stack = new Error().stack
        const request = this.getRequestWithArgs(args)
        const results = await request.execute(procedure)
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
        return results;
      } catch (e) {
        const cause = this.getDatabaseException(e,stack,psuedoSQL)
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
    
    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.SQL_TRACE.traceSQL(this.preparedStatement.sqlStatement)

    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now()
        stack = new Error().stack
        const results = await this.preparedStatement.statement.execute(args)
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
        return results;
      } catch (e) {
        const cause = this.getDatabaseException(e,stack,this.preparedStatement.sqlStatement)
        if (attemptReconnect && cause.lostConnection()) {
          this.preparedStatement === undefined;
          attemptReconnect = false;
          // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'PREPARE STATEMENT')
          this.cachePreparedStatement(this.preparedStatement.sqlStatement,this.preparedStatement.dataTypes)
          continue;
        }
        throw cause
      }      
    } 
  }

  async clearCachedStatement() {

     // this.LOGGER.trace([`${this.constructor.name}.clearCachedStatement()`,this.getWorkerNumber()],`clearCachedStatement(${this.preparedStatement ? this.preparedStatement.sqlStatement : undefined})`)

     if (this.preparedStatement !== undefined) {
       await this.preparedStatement.statement.unprepare()
     }
     this.preparedStatement = undefined;
  }

  async executePreparedStatement(sqlStatement,dataTypes,args) {

    await this.cachePreparedStatement(sqlStatement,dataTypes)
    const results = await this.executeCachedStatement(args)
    await this.clearCachedStatement()
    return results;
    
  }
    
  async bulkInsert(bulkOperation) {

    // this.LOGGER.trace([`${this.constructor.name}.bulkInsert()`,this.getWorkerNumber()],``)
     
    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    let operation = `BCP Operation: ${bulkOperation.path}. [${bulkOperation.rows.length}] rows.`
    this.SQL_TRACE.comment(operation)
   
    
    while (true) {
      // Exit with result or exception.  
      try {		
        const sqlStartTime = performance.now()
        stack = new Error().stack
        const request = this.getRequest()
        const results = await request.bulk(bulkOperation)
        // this.LOGGER.trace([`${this.constructor.name}.bulkInsert()`,this.getWorkerNumber()],`done`)
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
        return results;
      } catch (e) {
		const cause = this.getDatabaseException(e,stack,operation)
        if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
          // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'BULK INSERT')
		  await this.verifyTransactionState()
          continue		  
        }
		throw cause
      }      
    } 
  }

  async executeSQL(sqlStatement,args,noReconnect) {

    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.SQL_TRACE.traceSQL(sqlStatement)
    
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now()
        stack = new Error().stack
        const request = this.getRequestWithArgs(args)
        this.currentRequest = request
        const results = await request.query(sqlStatement)  
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
        return results;
      } catch (e) {
		const cause = this.getDatabaseException(e,stack,sqlStatement)
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
 
  async _executeDDL(ddl) {
    
    await this.beginTransaction()     

    await this.createSchema(this.CURRENT_SCHEMA)
    // Cannot use Promise.all with mssql Transaction class
	let results = []
    for (let ddlStatement of ddl) {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.CURRENT_SCHEMA)
      try {
        // May need to use executeBatch if we support SQL Server 2000.
        results.push(await this.executeSQL(ddlStatement))
      } catch (e) {
		await this.rollbackTransaction()   
  	    this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,'DDL'],e)
	    return e
      } 
    }
    await this.commitTransaction()   
	return results;
  }
    
  async verifyDataLoad(request,tableSpec) {    
    const statement = `select ISJSON("${tableSpec.columnName}") "VALID_JSON" from "${tableSpec.tableName}"`;
    const results = await this.executeSQL(statement)  
    this.LOGGER.info([`${this.constructor.name}.verifyDataLoad()`],`: Upload succesful: ${results.recordsets[0][0].VALID_JSON === 1}. Elapsed time ${performance.now() - startTime}ms.`)
    return results;
  }
  
  async createSchema(schema) {
    
    if (schema !== 'dbo') {
      const createSchema = `if not exists (select 1 from sys.schemas where name = N'${schema}') exec('create schema "${schema}"')`;
      try {
        const results = await this.executeSQL(createSchema)
      } catch (e) {
        this.LOGGER.logException([`${this.constructor.name}.createSchema()`],e)
      }
    }     
  }
  
  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  setSpatialSerializer(spatialFormat) {      
    switch (spatialFormat) {
      case "WKB":
        this.SPATIAL_SERIALIZER = "STAsBinary()";
        break;
      case "EWKB":
        this.SPATIAL_SERIALIZER = "AsBinaryZM()";
        break;
      case "WKT":
        this.SPATIAL_SERIALIZER = "STAsText()";
        break;
      case "EWKT":
        this.SPATIAL_SERIALIZER = "AsTextZM()";
        break;
     default:
        this.SPATIAL_SERIALIZER = "AsBinaryZM()";
    }  
    
  }   

  setLibraries() {
	  
	switch (this.DATABASE_VERSION) {
	  case 11:
	  case 12:
	    this.STATEMENT_LIBRARY_CLASS = StatementLibrary2014
		this.STATEMENT_GENERATOR_CLASS = StatementGenerator2014
	    break;
      default:
	}
	this.setSpatialSerializer(this.SPATIAL_FORMAT)
	this.statementLibrary = new this.STATEMENT_LIBRARY_CLASS(this)
  }
  

  /*
  **
  ** Begin the current transaction
  **
  */
  
  async beginTransaction() {

    // this.LOGGER.trace([`${this.constructor.name}.beginTransaction()`,this.ROLE,this.TRANSACTION_IN_PROGRESS,this.getWorkerNumber()],``)

    let stack
    const psuedoSQL = 'begin transaction'
    this.SQL_TRACE.traceSQL(psuedoSQL)
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    
	let logSuccess = false;
	
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now()
        stack = new Error().stack
        await this.transaction.begin()
		this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
        this.TEDIOUS_TRANSACTION_ISSUE = false;
        this.requestProvider = this.transaction
        super.beginTransaction()
        if (logSuccess) {
		  this.LOGGER.info([this.DATABASE_VENDOR,this.ROLE,'TRANSACTION MANAGER','BEGIN','RECONNECTION'],'Success')
        }
        break;
      } catch (e) {
        const cause = this.getDatabaseException(e,stack,'sql.Transaction.begin()')
		
        if (attemptReconnect && cause.lostConnection()) {

  	      /*
	      **
	      ** ### A Lost connection error during a BEGIN TRANSACTION operation can cause the closePool() operation to hang.
	      */

	      try {
	        this.BEGIN_TRANSACTION_ISSUE = true
	        await this.transaction.rollback()
	      } catch (e) {
			if (e.code && (e.code !== 'EINVALIDSTATE')) {
              stack = new Error().stack
  		      this.LOGGER.handleException([this.DATABASE_VENDOR,this.ROLE,'TRANSACTION MANAGER','BEGIN','ERROR_CLEAN_UP]'],this.createDatabaseError(e,stack,'sql.Transaction.rollback()'))
			}
	      }

          attemptReconnect = false;
          // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'BEGIN TRANSACTION')
		  logSuccess = true;
          continue;
        }
        throw cause
      }      
    } 
  }

  /*
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
      
    // this.LOGGER.trace([`${this.constructor.name}.commitTransaction()`,this.TRANSACTION_IN_PROGRESS,this.getWorkerNumber()],``)

    if (this.TEDIOUS_TRANSACTION_ISSUE) {
	  this.LOGGER.warning([this.DATABASE_VENDOR,this.ROLE,'TRANSACTION MANAGER','COMMIT'],`Unable to COMMIT following TEDIOUS FORCED ROLLBACK operation.`)
	  return;
	}

    let stack
    const psuedoSQL = 'commit transaction'
    this.SQL_TRACE.traceSQL(psuedoSQL)
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
      
    try {
      super.commitTransaction()
      const sqlStartTime = performance.now()
      stack = new Error().stack;
      await this.transaction.commit()
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
      this.requestProvider = this.pool
    } catch (e) {
      const cause = this.getDatabaseException(e,stack,'sql.Transaction.commit()')
      if (attemptReconnect && cause.lostConnection()) {
        attemptReconnect = false;
        // reconnect() throws cause if it cannot reconnect...
        await this.reconnect(cause,'COMMIT TRANSACTION')
      }
      throw this.getDatabaseException(e,stack,'sql.Transaction.commit()')
    }
    
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction(cause) {

    // this.LOGGER.trace([`${this.constructor.name}.rollbackTransaction()`,this.TRANSACTION_IN_PROGRESS,this.TEDIOUS_TRANSACTION_ISSUE,this.getWorkerNumber(),(this.preparedStatement !== undefined)],`${this.cause ? this.cause.message : undefined}`)
	
	this.checkConnectionState(cause)
    
    if (this.TEDIOUS_TRANSACTION_ISSUE) {
      return
    }

    // Clear any Prepared Statements associated with the transaction otherwise rollback will result in "Can't rollback transaction. There is a request in progress."

    await this.clearCachedStatement()
    
    // If rollbackTransaction was invoked due to encounterng an error and the rollback operation results in a second exception being raised, log the exception raised by the rollback operation and throw the original error.
    // Note the underlying error is not thrown unless the rollback itself fails. This makes sure that the underlying error is not swallowed if the rollback operation fails.


    let stack
    const psuedoSQL = 'rollback transaction'
    this.SQL_TRACE.traceSQL(psuedoSQL)
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    let attemptCancellation = true
	
	while (true) {
	  try {
        super.rollbackTransaction()
        const sqlStartTime = performance.now()
        stack = new Error().stack;
        this.EXPECTED_ROLLBACK = true;
        await this.transaction.rollback()
        this.EXPECTED_ROLLBACK = false;
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
        this.requestProvider = this.pool
		return
      } catch (e) {
        this.EXPECTED_ROLLBACK = false;
        let newIssue = this.getDatabaseException(e,stack,'sql.Transaction.rollback()')
        if (attemptReconnect && newIssue.lostConnection()) {
          attemptReconnect = false;
          // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(newIssue,'ROLLBACK TRANSACTION')
		  break;
        }
	    if (attemptCancellation  && newIssue.requestInProgress()) {
		  attemptCancellation = false
	      await this.cancelRequest(false)
		  continue;
		}
        this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)
      }   
    }
  }
  
  async createSavePoint() {

    // this.LOGGER.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber(),this.PIPELINE_STATE.written,this.PIPELINE_STATE.cached],``)

    await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_CREATE_SAVE_POINT)
    super.createSavePoint()
  }
  
  async restoreSavePoint(cause) {

    // this.LOGGER.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)

    this.checkConnectionState(cause)
    
    if (this.TEDIOUS_TRANSACTION_ISSUE) {
      return
    }

    // If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
    // Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.
    
    try {
      await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_RESTORE_SAVE_POINT)
      super.restoreSavePoint()
    } catch (newIssue) {
	  if (this.handleCancelledRequest(newIssue)) {
		 throw cause
	  }
      this.checkCause('RESTORE SAVPOINT',cause,newIssue)
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

    let results
	let stack
	await this.beginTransaction();

    let statement = `drop table if exists "${MsSQLConstants.STAGING_TABLE.tableName}"`;
	results = await this.executeBatch(statement)

	statement = `create table "${MsSQLConstants.STAGING_TABLE.tableName}" ("${MsSQLConstants.STAGING_TABLE.columnName}" NVARCHAR(MAX) collate ${this.DEFAULT_COLATION})`;
    results = await this.executeBatch(statement)

    statement = `insert into "${MsSQLConstants.STAGING_TABLE.tableName}" values ('')`;
    results = await this.executeBatch(statement)
  
    statement = `update "${MsSQLConstants.STAGING_TABLE.tableName}" set "${MsSQLConstants.STAGING_TABLE.columnName}" .write(@C0,null,null)`;  
	await this.cachePreparedStatement(statement, [{type : "nvarchar"}]) 
	
    const is = await new Promise((resolve,reject) => {
      const stack = new Error().stack
	  const inputStream = fs.createReadStream(importFilePath);
      inputStream.on('open',() => {resolve(inputStream)}).on('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(this,err,stack,importFilePath) : new FileError(this,err,stack,importFilePath) )})
    })

    const loader = new MsSQLFileLoader(this,this.status);
    const multiplexor = new PassThrough()
    const exportFileHeader = new ExportFileHeader (multiplexor, importFilePath, this.LOGGER)

	stack = new Error().stack		
    const startTime = performance.now()
	await pipeline(is,multiplexor,loader)

    // results = await this.verifyDataLoad(this.generateRequest(),MsSQLConstants.STAGING_TABLE)

    this.setSystemInformation(exportFileHeader.SYSTEM_INFORMATION)
	this.setMetadata(exportFileHeader.METADATA)
	const ddl = exportFileHeader.DDL
      
    const elapsedTime = performance.now() - startTime
    is.close() 
    await this.clearCachedStatement(); 
    await this.commitTransaction();
	return elapsedTime;
  }
  
  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */


  async processFile(hndl) {
    
	const typeMappings = await this.getVendorDataTypeMappings()
    
    const args = { 
            inputs: [{
  			  name: 'TYPE_MAPPINGS',   type: sql.VarChar,  value: typeMappings
			},{
              name: 'TARGET_DATABASE', type: sql.VarChar,  value: this.CURRENT_SCHEMA
            },{
              name: 'DB_COLLATION',    type: sql.VarChar,  value: this.DB_COLLATION  
            }]
          }    

     let results = await this.execute('sp_YADAMU_IMPORT',args,'')                   
     results = results.recordset;
     const log = JSON.parse(results[0][Object.keys(results[0])[0]])
	 super.processLog(log,'OPENJSON',this.status, this.LOGGER)
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
  
    const results = await this.executeSQL(this.STATEMENT_LIBRARY_CLASS.SQL_SYSTEM_INFORMATION)
    const sysInfo =  results.recordsets[0][0];
    const serverProperties = JSON.parse(sysInfo.SERVER_PROPERTIES)  
    const dbProperties = JSON.parse(sysInfo.DATABASE_PROPERTIES)    
    return Object.assign(
	  super.getSystemInformation()
	, {
        sessionUser                 : sysInfo.SESSION_USER
      , currentUser                 : sysInfo.CURRENT_USER
      , dbName                      : sysInfo.DATABASE_NAME
      , databaseVersion             : serverProperties.ProductVersion
      , hostname                    : serverProperties.MachineName
      , serverProperties            : serverProperties
      , databaseProperties          : dbProperties
	  , yadamuInstanceID            : sysInfo.YADAMU_INSTANCE_ID
	  , yadamuInstallationTimestamp : sysInfo.YADAMU_INSTALLATION_TIMESTAMP
      }
	)
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */

  async getDDLOperations() {
    return undefined
  }
   
  generateMetadata(schemaInformation) {    
    const metadata = super.generateMetadata(schemaInformation) 
    schemaInformation.forEach((table,idx) => {
       metadata[table.TABLE_NAME].collationNames = JSON.parse(table.COLLATION_NAME_ARRAY)
    }) 
    return metadata

  }  
      
  async getSchemaMetadata() {

    this.SQL_TRACE.comment(`@SCHEMA="${this.CURRENT_SCHEMA}"`)
   
    const statement = this.statementLibrary.SQL_SCHEMA_INFORMATION
	
    const results = await this.executeSQL(statement, { inputs: [{name: "SCHEMA", type: sql.VarChar, value: this.CURRENT_SCHEMA}]})
    return results.recordsets[0]
  }
  
  async _getInputStream(queryInfo) {

    // if the previous pipleline operation failed, there will be a pending request that needs to cleared otherwise the close pool will hang
	
    if (this.ACTIVE_INPUT_STREAM === true) {
	  await this.cancelRequest(true)
	}
	
    // this.LOGGER.trace([`${this.constructor.name}.getInputStream()`,this.getWorkerNumber()],queryInfo.TABLE_NAME)
	let attemptReconnect = this.ATTEMPT_RECONNECTION;
    
    while (true) {
      // Exit with result or exception.  
	  let stack
      try {
        this.SQL_TRACE.traceSQL(queryInfo.SQL_STATEMENT)
		stack = new Error().stack
        const sqlStartTime = performance.now()
        const request = this.getRequest()
	    let is
		if (typeof request.toReadableStream === 'function') {
          request.query(queryInfo.SQL_STATEMENT)
  	      is = request.toReadableStream()
		  is.on('error',(err) => {
		  })
	    }
		else {
		  is = new MsSQLReader(request,queryInfo.SQL_STATEMENT,queryInfo.TABLE_NAME,this.LOGGER)
		}
	    this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
        return is;
      } catch (e) {
		const cause = this.getDatabaseException(e,stack,sqlStatement)
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
 
  /*
  **
  ** The following methods are used by the YADAMU DBReader class
  **
  */
  
  async setWorkerConnection() {
    // Override the default implementation provided by YadamuDBI
    // Use the master's connection provider / pool to generate request and transaction objects
    this.pool = this.manager.pool;
    this.requestProvider = this.pool
    this.transaction = this.getTransactionManager() 
  }

  classFactory(yadamu) {
    return new MsSQLDBI(yadamu,this,this.connectionParameters,this.parameters)
  }
  
  async getConnectionID() {
    const results = await this.executeSQL(`select @@SPID "SPID"`)
    const pid = results.recordset[0].SPID
    return pid
  }  

async copyOperation(tableName,copyOperation,copyState) {
    
    /*
    **
    ** Generic Basic Imementation - Override as required for error reporting etc
    **
    */
    
    try {
      copyState.startTime = performance.now()
	  
	  
	  
      let results = await this.beginTransaction()
      results = await this.executeSQL(copyOperation.dml)
      copyState.read = results.rowsAffected[0]
      copyState.written = results.rowsAffected[0]
      copyState.endTime = performance.now()
      results = await this.commitTransaction()
      copyState.committed = copyState.written 
      copyState.written = 0
    } catch(e) {
      this.LOGGER.handleException([this.DATABASE_VENDOR,'COPY',tableName, this.getWorkerNumber()],e)
	  // console.log(e)
      copyState.writerError = e
      try {
		await this.cancelRequest(true)
        let results = await this.rollbackTransaction()
		this.transaction = this.getTransactionManager() 
      } catch (e) {
        e.cause = copyState.writerError
        copyState.writerError = e
      }
    }
    return copyState
  }
  }

export { MsSQLDBI as default }


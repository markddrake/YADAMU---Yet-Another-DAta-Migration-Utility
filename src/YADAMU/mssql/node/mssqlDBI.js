"use strict" 
const fs = require('fs');
// const Readable = require('stream').Readable;
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
** When working in parallel Manager and Worker instances share the same Pool.
**
** Transactions are managed via a Transaction object. The transaction object owns a connection.
** Each instance of the DBI owns it's own Transaction object. 
** When operations need to be transactional the Transaction object becomes the requestProvider for the duration of the transaction.
**
*/

const YadamuDBI = require('../../common/yadamuDBI.js');
const DBIConstants = require('../../common/dbiConstants.js');
const YadamuConstants = require('../../common/yadamuConstants.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js')

const MsSQLConstants = require('./mssqlConstants.js')
const MsSQLError = require('./mssqlException.js')
const MsSQLParser = require('./mssqlParser.js');
const MsSQLWriter = require('./mssqlWriter.js');
const StatementGenerator = require('./statementGenerator.js');
const StagingTable = require('./stagingTable.js');
const MsSQLReader = require('./mssqlReader.js');
const StatementLibrary = require('./mssqlStatementLibrary.js')

const {ConnectionError} = require('../../common/yadamuException.js')


class MsSQLDBI extends YadamuDBI {

  static #_YADAMU_DBI_PARAMETERS

  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.YADAMU_DBI_PARAMETERS,MsSQLConstants.DBI_PARAMETERS))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
	return MsSQLDBI.YADAMU_DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
  
  get DB_COLLATION()           { return this._DB_COLLATION }
    
  // Override YadamuDBI

  get DATABASE_KEY()           { return MsSQLConstants.DATABASE_KEY};
  get DATABASE_VENDOR()        { return MsSQLConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()        { return MsSQLConstants.SOFTWARE_VENDOR};
  // get SQL_COPY_OPERATIONS()    { return true }
  get STATEMENT_TERMINATOR()   { return MsSQLConstants.STATEMENT_TERMINATOR };
  get STATEMENT_SEPERATOR()        { return '\ngo\n--\n' }

  // Enable configuration via command line parameters

  get SPATIAL_FORMAT()         { return this.parameters.SPATIAL_FORMAT        || MsSQLConstants.SPATIAL_FORMAT }
  get SPATIAL_MAKE_VALID()     { return this.parameters.SPATIAL_MAKE_VALID    || MsSQLConstants.SPATIAL_MAKE_VALID }

  get TRANSACTION_IN_PROGRESS()       { return super.TRANSACTION_IN_PROGRESS ||this.tediousTransactionError  }
  set TRANSACTION_IN_PROGRESS(v)      { super.TRANSACTION_IN_PROGRESS = v }

  get DATABASE_NAME()          { return this.parameters.YADAMU_DATABASE ? this.parameters.YADAMU_DATABASE : this.vendorProperties.database }

  constructor(yadamu,settings,parameters) {
    super(yadamu,settings,parameters);
    this.requestProvider = undefined;
    this.transaction = undefined;
    this.pool = undefined;
    this.yadamuRollack = false
    this.tediousTransactionError = false;
	this.beginTransactionError = false;
    // Allow subclasses to access constants defined by the sql object. Redeclaring the SQL object in a subclass causes strange behavoir
    this.sql = sql
	this.StatementGenerator = StatementGenerator
	this.StatementLibrary = StatementLibrary
    this.statementLibrary = undefined
    
    sql.on('error',(err, p) => {
      this.yadamuLogger.handleException([`${this.DATABASE_VENDOR}`,`mssql.onError()`],err);
      throw err
    })
	        
  }
  
  getSchemaIdentifer(key) {
	return `${this.parameters.YADAMU_DATABASE}"."${this.parameters[key]}`
  }  
  
  /*
  **
  ** Local methods 
  **
  */

  async testConnection(connectionProperties,parameters) {   
    try {
      this.setConnectionProperties(connectionProperties);
      this.setTargetDatabase();
      const connection = await sql.connect(this.vendorProperties);
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
    this._DB_VERSION =  parseInt(results.recordsets[0][0].DATABASE_VERSION)
    this._DB_COLLATION = results.recordsets[0][0].DB_COLLATION
    
    this.defaultCollation = this.DB_VERSION < 15 ? 'Latin1_General_100_CS_AS_SC' : 'Latin1_General_100_CS_AS_SC_UTF8';
  }
  
  setTargetDatabase() {  
    if ((this.parameters.YADAMU_DATABASE) && (this.parameters.YADAMU_DATABASE !== this.vendorProperties.database)) {
      this.vendorProperties.database = this.parameters.YADAMU_DATABASE
    }
  }
  
  reportTransactionState(operation) {
    const e = new Error(`Unexpected ${operation} operation`)
    this.yadamuLogger.handleWarning([this.DATABASE_VENDOR,'TRANSACTION MANAGER',operation],new MsSQLError(e,e.stack,this.constructor.name))
    
  }
  
  getTransactionManager() {

    // this.yadamuLogger.trace([`${this.constructor.name}.getTransactionManager()`,this.getWorkerNumber()],``)

    this.TRANSACTION_IN_PROGRESS = false;
    const transaction = new sql.Transaction(this.pool)
    transaction.on('rollback',() => { 
      if (!this.yadamuRollback) {
        this.tediousTransactionError = true;
        this.reportTransactionState('ROLLBACK')
		// await this.recoverTransactionState()
      }
    });
    return transaction
  }
  
  async recoverTransactionState(newTransaction) {
	await this.request.cancel();
	this.transaction = this.getTransactionManager()
    if (newTransaction) {
	  // Error Recovery if Rows are lost
	  //
      await this.beginTransaction()
	};

  }

  getRequest() {
    let stack
    try {
      stack = new Error().stack;
      this.request = new sql.Request(this.requestProvider)
      this.request.on('info',(infoMsg) => { 
        this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`MESSAGE`],`${infoMsg.message}`);
      })
      return this.request;
    } catch (e) {
      throw this.trackExceptions(new MsSQLError(e,stack,`sql.Request(${this.requestProvider.constuctor.name})`))
    }
  }
  
  getRequestWithArgs(args) {
     
    const request = this.getRequest();
    
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
      
    // this.yadamuLogger.trace([`${this.constructor.name}.getPreparedStatement()`,this.getWorkerNumber()],sqlStatement);

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
          case 'bit':
            statement.input(column,sql.Bit);
            break;
          case 'bigint':
            // statement.input(column,sql.BigInt);
			statement.input(column,sql.VarChar(20))
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
			precision = dataType.length || 18;
		    if ((precision) > 15) {
   			  statement.input(column,sql.VarChar(precision + 2))
			  break;
			}
            statement.input(column,sql.Decimal(dataType.length || 15,dataType.scale || 0));
            break;
          case 'numeric':
            // sql.Numeric ([precision], [scale])
			precision = dataType.length || 18;
		    if ((precision) > 15) {
   			  statement.input(column,sql.VarChar(precision + 2))
			  break;
			}
            statement.input(column,sql.Numeric(dataType.length || 15,dataType.scale || 0));
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
            statement.input(column,sql.VarChar(length));
            break;
          case 'nvarchar':
            statement.input(column,sql.NVarChar(length));
            break;
          case 'json':
            statement.input(column,sql.NVarChar(sql.MAX));
			break;
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
            statement.input(column,sql.Binary(dataType.length));
            break;
          case 'varbinary':
            // Upload images as VarBinary(MAX). Convert data to Buffer. This enables bulk upload and avoids Collation issues...
            // sql.VarBinary ([length])
             statement.input(column,sql.VarBinary(length));
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
            this.yadamuLogger.warning([this.DATABASE_VENDOR,`PREPARED STATEMENT`],`Unmapped data type [${dataType.type}].`);
        }
      })
      
      stack = new Error().stack;
      await statement.prepare(sqlStatement);
      return statement;
    } catch (e) {
      try {
        await statement.unprepare();
      } catch (e) {}
      throw this.trackExceptions(new MsSQLError(e,stack,`sql.PreparedStatement(${sqlStatement}`))
    }
  }

  async createConnectionPool() {
      
    // this.yadamuLogger.trace([this.DATABASE_VENDOR],`createConnectionPool()`)
  
    this.setTargetDatabase();
    this.logConnectionProperties();

    let stack
    let operation                                                                        
    try {
      const sqlStartTime = performance.now();
      stack = new Error().stack;
      operation = 'sql.connectionPool()'
      this.pool = new sql.ConnectionPool(this.vendorProperties)
      this.pool.on('error',(err, p) => {
        const cause = err instanceof MsSQLError ? err : this.trackExceptions(new MsSQLError(err,stack,`${operation}.onError()`))
        if (!cause.suppressedError())  {
          this.yadamuLogger.handleException([this.DATABASE_VENDOR,`sql.ConnectionPool.onError()`],cause);
          if (!this.RECONNECT_IN_PROGRESS) {
            throw cause
          }
        }
      })
      
      stack = new Error().stack;
      operation = 'sql.ConnectionPool.connect()'
      await this.pool.connect();
      this.traceTiming(sqlStartTime,performance.now())
      this.requestProvider = this.pool;
      this.transaction = this.getTransactionManager()
      
    } catch (e) {
      throw this.trackExceptions(new MsSQLError(e,stack,operation))
    }       

    await this.configureConnection();
  }

  async _getDatabaseConnection() {
    try {
      // this.yadamuLogger.trace(this.DATABASE_VENDOR,this.getWorkerNumber()],`_getDatabaseConnection()`)
      await this.createConnectionPool();
    } catch (e) {
      const err = new ConnectionError(e,this.vendorProperties);
      throw err
    }
  } 
  
  async closeConnection(options) {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`closeConnection(${(this.preparedStatement !== undefined)},${this.TRANSACTION_IN_PROGRESS})`)
    
    if (this.preparedStatement !== undefined ) {
      await this.clearCachedStatement()
    }   
    
    if (this.TRANSACTION_IN_PROGRESS) {
      await this.rollbackTransaction()
    }
  }
  
  unhandledRejectionHandler(err,p) {
    if (err.code  === 'ENOTOPEN') {
	  err.ignoreUnhandledRejection = true
	}
  }
  
  async closePool(options) {
    
    // this.yadamuLogger.trace([this.DATABASE_VENDOR,'CLOSE POOL'],`closePool(${(this.pool !== undefined)})`)
	
	/*
	**
	** ### It appears closePool() can hang if a BEGIN TRANSACTiON operation failed due to a lost connection error
	**
	*/
	
    if (this.pool !== undefined) {
      await new Promise(async(resolve,reject) => {
		  
		  /*
		  **
 		  ** Set a timer to deal with pool.close() hanging
		  **
		  */
		  
          const timer = setTimeout(
		    () => {
              this.yadamuLogger.warning([this.DATABASE_VENDOR,'CLOSE POOL',this.beginTransactionError],`Close pool operation timed out`)
		      resolve()
	        }
		   ,1000
		  )
		 		  
         let stack
         let psudeoSQL
         try {
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
           // this.yadamuLogger.trace([this.DATABASE_VENDOR,'CLOSE POOL'],`Closing pool`)
		   await this.pool.close();
		   clearTimeout(timer)
           // this.yadamuLogger.trace([this.DATABASE_VENDOR,'CLOSE POOL'],`Pool Closed`)
		   process.removeListener('unhandledRejection',this.unhandledRejectionHandler)

          stack = new Error().stack
          psudeoSQL = 'MsSQL.close()'
          await sql.close();
        
		  /*
		  **
		  ** Setting pool to undefined seems to cause Error: No connection is specified for that request if a new pool is created.. ### Makes no sense
          
		  this.pool = undefined;
		  
		  **
		  */
		  resolve()
        } catch(e) {
		  clearTimeout(timer)
          // this.pool = undefined
          this.yadamuLogger.info([this.DATABASE_VENDOR],`Error Closing Pool`)
          reject(this.trackExceptions(new MsSQLError(e,stack,psudeoSQL)))
        }
	  })
	} 
  }
 
  async _reconnect() {
    await this.pool.connect() 
    this.requestProvider = this.pool
    await this.executeSQL('select 1');
    this.transaction = this.getTransactionManager()
  }
  
  setVendorProperties(connectionSettings) {
    super.setVendorProperties(connectionSettings) 
    if (this.vendorProperties) {
	  if (!this.vendorProperties.hasOwnProperty("options")) {    
        this.vendorProperties.options = {}
      }
      this.vendorProperties.options.abortTransactionOnError = false
      this.vendorProperties.options.enableArithAbort = true;
    }
  }
   
  async executeBatch(sqlStatment) {

    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.status.sqlTrace.write(this.traceSQL(sqlStatment))

    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
        stack = new Error().stack
        const request = this.getRequest();
        const results = await request.batch(sqlStatment);  
        this.traceTiming(sqlStartTime,performance.now())
        return results;
      } catch (e) {
        const cause = this.trackExceptions(new MsSQLError(e,stack,sqlStatment))
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
    const psuedoSQL = `SET @RESULTS = '{}';CALL ${procedure}(${this.getArgNameList(args)}); SELECT @RESULTS "${output}";`
    this.status.sqlTrace.write(this.traceSQL(psuedoSQL))

    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
        stack = new Error().stack
        const request = this.getRequestWithArgs(args);
        const results = await request.execute(procedure);
        this.traceTiming(sqlStartTime,performance.now())
        return results;
      } catch (e) {
        const cause = this.trackExceptions(new MsSQLError(e,stack,psuedoSQL))
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
    this.status.sqlTrace.write(this.traceSQL(this.preparedStatement.sqlStatement))

    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
        stack = new Error().stack
        const results = await this.preparedStatement.statement.execute(args);
        this.traceTiming(sqlStartTime,performance.now())
        return results;
      } catch (e) {
        const cause = this.trackExceptions(new MsSQLError(e,stack,this.preparedStatement.sqlStatement))
        if (attemptReconnect && cause.lostConnection()) {
          this.preparedStatement === undefined;
          attemptReconnect = false;
          // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'PREPARE STATEMENT')
          this.cachePreparedStatement(this.preparedStatement.sqlStatement,this.preparedStatement.dataTypes);
          continue;
        }
        throw cause
      }      
    } 
  }

  async clearCachedStatement() {
     // this.yadamuLogger.trace([`${this.constructor.name}.clearCachedStatement()`,this.getWorkerNumber()],`clearCachedStatement(${this.preparedStatement ? this.preparedStatement.sqlStatement : undefined})`)
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
     
    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    let operation = `Bulk Operation: ${bulkOperation.path}. [${bulkOperation.rows.length}] rows.`
    this.status.sqlTrace.write(this.traceComment(operation))
   
    
    const previousRequest = this.request
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
        stack = new Error().stack
        const request = this.getRequest();
        const results = await request.bulk(bulkOperation);
        this.traceTiming(sqlStartTime,performance.now())
        return results;
      } catch (e) {
        const cause = this.trackExceptions(new MsSQLError(e,stack,operation))
		/*
        if (e.code === 'EREQINPROG') {
		  try {
			 await previousRequest.cancel();
			 continue
		  } catch (e) {
			cause.cancelFailed = e
			throw cause;
		  } 
		}
		*/
        if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
          // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'BULK INSERT')
          continue		  
        }
		throw this.trackExceptions(cause)
      }      
    } 
  }

  async executeSQL(sqlStatement,args,noReconnect) {

    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.status.sqlTrace.write(this.traceSQL(sqlStatement))
    
    const previousRequest = this.request
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
        stack = new Error().stack
        const request = this.getRequestWithArgs(args)
        this.currentRequest = request
        const results = await request.query(sqlStatement);  
        this.traceTiming(sqlStartTime,performance.now())
        return results;
      } catch (e) {
		const cause = this.trackExceptions(new MsSQLError(e,stack,sqlStatement));
		/*
        if (e.code === 'EREQINPROG') {
		  try {
			 await previousRequest.cancel();
			 continue
		  } catch (e) {
			cause.cancelFailed = e
			throw cause;
		  } 
		}
		*/
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

    await this.createSchema(this.parameters.TO_USER);
    // Cannot use Promise.all with mssql Transaction class
	let results = []
    for (let ddlStatement of ddl) {
      ddlStatement = ddlStatement.replace(/%%SCHEMA%%/g,this.parameters.TO_USER);
      try {
        // May need to use executeBatch if we support SQL Server 2000.
        results.push(await this.executeSQL(ddlStatement))
      } catch (e) {
  	    this.yadamuLogger.handleException([this.DATABASE_VENDOR,'DDL'],e)
	    return e
      } 
    }
    await this.commitTransaction()   
	return results;
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
  
  async initialize() {
    await super.initialize(true);   
	switch (this.DB_VERSION) {
	  case 12:
	    this.StatementLibrary = require('./2014/mssqlStatementLibrary.js')
		this.StatementGenerator = require('./2014/statementGenerator.js');
	    break;
      default:
	}
	this.setSpatialSerializer(this.SPATIAL_FORMAT);
	this.statementLibrary = new this.StatementLibrary(this)
  }    

  updateVendorProperties(vendorProperties) {

    vendorProperties.server          = this.parameters.HOSTNAME        || vendorProperties.server 
    vendorProperties.user            = this.parameters.USERNAME        || vendorProperties.user
    vendorProperties.database        = this.parameters.DATABASE        || vendorProperties.database    
    vendorProperties.password        = this.parameters.PASSWORD        || vendorProperties.password    
    vendorProperties.port            = parseInt(this.parameters.PORT)  || vendorProperties.port
    vendorProperties.requestTimeout  = 2 * 60 * 60 * 10000
    vendorProperties.options         = {
      encrypt                        : false // Use this if you're on Windows Azure
    , abortTransactionOnError        : false
    , enableArithAbort               : true
    }
	
	// MsSQL does not fallback to the default port if port is undefined
	
	if (vendorProperties.port === undefined) {
	  delete vendorProperties.port
	}
  }
      
  /*
  **
  **  Gracefully close down the database connection and pool
  **
  */

  async finalize(options) {
    await super.finalize(options)
  }

  /*
  **
  **  Abort the database connection and pool.
  **
  */

  async abort(e) {
    await super.abort(e);
  }

  /*
  **
  ** Begin the current transaction
  **
  */
  
  async beginTransaction() {

    // this.yadamuLogger.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)
          
    let stack
    const psuedoSQL = 'begin transaction'
    this.status.sqlTrace.write(this.traceSQL(psuedoSQL));
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    
	let logSuccess = false;
	
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
        stack = new Error().stack
        await this.transaction.begin();
		this.traceTiming(sqlStartTime,performance.now())
        this.tediousTransactionError = false;
        this.requestProvider = this.transaction
        super.beginTransaction()
        if (logSuccess) {
		  this.yadamuLogger.info([this.DATABASE_VENDOR,'TRANSACTION MANAGER','BEGIN','RECONNECTION'],'Success')
        }
        break;
      } catch (e) {
        const cause = this.trackExceptions(new MsSQLError(e,stack,'sql.Transaction.begin()'))
		
        if (attemptReconnect && cause.lostConnection()) {

  	      /*
	      **
	      ** ### A Lost connection error during a BEGIN TRANSACTION operation can cause the closePool() operation to hang.
	      */

	      try {
	        this.beginTransactionError = true
	        await this.transaction.rollback()
	      } catch (e) {
			if (e.code && (e.code !== 'EINVALIDSTATE')) {
              stack = new Error().stack
  		      this.yadamuLogger.handleException([this.DATABASE_VENDOR,'TRANSACTION MANAGER','BEGIN','ERROR_CLEAN_UP]'],new MsSQLError(e,stack,'sql.Transaction.rollback()'))
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
      
    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

    if (this.tediousTransactionError) {
	  this.yadamuLogger.warning([this.DATABASE_VENDOR,'TRANSACTION MANAGER','COMMIT'],`Unable to COMMIT following TEDIOUS FORCED ROLLBACK operation.`)
	  return;
	}


    let stack
    const psuedoSQL = 'commit transaction'
    this.status.sqlTrace.write(this.traceSQL(psuedoSQL));
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
      
    try {
      super.commitTransaction()
      const sqlStartTime = performance.now();
      stack = new Error().stack;
      await this.transaction.commit();
      this.traceTiming(sqlStartTime,performance.now())
      this.requestProvider = this.pool
    } catch (e) {
      const cause = this.trackExceptions(new MsSQLError(e,stack,'sql.Transaction.commit()'))
      if (attemptReconnect && cause.lostConnection()) {
        attemptReconnect = false;
        // reconnect() throws cause if it cannot reconnect...
        await this.reconnect(cause,'COMMIT TRANSACTION')
      }
      throw this.trackExceptions(new MsSQLError(e,stack,'sql.Transaction.commit()'))
    }
    
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.rollbackTransaction()`,this.getWorkerNumber(),(this.preparedStatement !== undefined)],`${this.cause ? this.cause.message : undefined}`)
    
	this.checkConnectionState(cause)
    
    if (this.tediousTransactionError) {
      return
    }

    // Clear any Prepared Statements associated with the transaction otherwise rollback will result in "Can't rollback transaction. There is a request in progress."

    await this.clearCachedStatement()
    
    // If rollbackTransaction was invoked due to encounterng an error and the rollback operation results in a second exception being raised, log the exception raised by the rollback operation and throw the original error.
    // Note the underlying error is not thrown unless the rollback itself fails. This makes sure that the underlying error is not swallowed if the rollback operation fails.


    let stack
    const psuedoSQL = 'rollback transaction'
    this.status.sqlTrace.write(this.traceSQL(psuedoSQL));
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
      
    try {
      super.rollbackTransaction()
      const sqlStartTime = performance.now();
      stack = new Error().stack;
      this.yadamuRollback = true;
      await this.transaction.rollback();
      this.yadamuRollback = false;
      this.traceTiming(sqlStartTime,performance.now())
      this.requestProvider = this.pool
    } catch (e) {
      this.yadamuRollback = false;
      let newIssue = this.trackExceptions(new MsSQLError(e,stack,'sql.Transaction.rollback()'))
      if (attemptReconnect && cause.lostConnection()) {
        attemptReconnect = false;
        // reconnect() throws cause if it cannot reconnect...
        await this.reconnect(cause,'ROLLBACK TRANSACTION')
      }
      this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)
    }   
    
  }
  
  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber(),this.metrics.written,this.metrics.cached],``)

    await this.executeSQL(this.StatementLibrary.SQL_CREATE_SAVE_POINT);
    super.createSavePoint()
  }
  
  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)

    this.checkConnectionState(cause)
    
    if (this.tediousTransactionError) {
      return
    }

    // If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
    // Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.
    
    try {
      await this.executeSQL(this.StatementLibrary.SQL_RESTORE_SAVE_POINT);
      super.restoreSavePoint()
    } catch (newIssue) {
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

	this.DESCRIPTION = this.getSchemaIdentifer('TO_USER')
         
    const stagingTable = new StagingTable(this,MsSQLConstants.STAGING_TABLE,importFilePath,this.status); 
    let results = await stagingTable.uploadFile()
    // results = await this.verifyDataLoad(this.generateRequest(),MsSQLConstants.STAGING_TABLE);
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
                name: 'DB_COLLATION',    type: sql.VarChar,  value: this.DB_COLLATION  
             }]
           }    

     let results = await this.execute('sp_YADAMU_IMPORT',args,'')                   
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
  
    const results = await this.executeSQL(this.StatementLibrary.SQL_SYSTEM_INFORMATION)
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
      
  async getSchemaInfo(keyName) {

    this.status.sqlTrace.write(this.traceComment(`@SCHEMA="${this.parameters[keyName]}"`))
      
    const statement = this.statementLibrary.SQL_SCHEMA_INFORMATION
    const results = await this.executeSQL(statement, { inputs: [{name: "SCHEMA", type: sql.VarChar, value: this.parameters[keyName]}]})
    return results.recordsets[0]
  
  }
  
  createParser(tableInfo) {
    return new MsSQLParser(tableInfo,this.yadamuLogger);
  }  
  
  inputStreamError(err,sqlStatement) {
     return this.trackExceptions(new MsSQLError(err,this.streamingStackTrace,sqlStatement))
  }

  async getInputStream(tableInfo) {
    // this.yadamuLogger.trace([`${this.constructor.name}.getInputStream()`,this.getWorkerNumber()],tableInfo.TABLE_NAME)
    this.streamingStackTrace = new Error().stack;
    const request = this.getRequest();
	this.status.sqlTrace.write(this.traceSQL(tableInfo.SQL_STATEMENT));
	if (typeof request.toReadableStream === 'function') {
  	  const stream = request.toReadableStream();
      request.query(tableInfo.SQL_STATEMENT);
      return stream
	}
    return new MsSQLReader(request,tableInfo.SQL_STATEMENT,tableInfo.TABLE_NAME,this.yadamuLogger);
  }      

  /*
  **
  ** The following methods are used by the YADAMU DBReader class
  **
  */
    
  async generateStatementCache(schema) {
    /* ### OVERRIDE ### Pass additional parameter Database Name */
    const statementGenerator = new this.StatementGenerator(this, schema, this.metadata ,this.yadamuLogger);
    this.statementCache = await statementGenerator.generateStatementCache(this.DATABASE_NAME)
	return this.statementCache
  }

  getOutputStream(tableName,ddlComplete) {
     return super.getOutputStream(MsSQLWriter,tableName,ddlComplete)
  }
 
 async setWorkerConnection() {
    // Override the default implementation provided by YadamuDBI.

    // Use the connection provider (master) pool
    this.pool = this.manager.pool;
    this.requestProvider = this.pool
    this.transaction = this.getTransactionManager() 
  }

  classFactory(yadamu) {
    return new MsSQLDBI(yadamu)
  }
  
  async getConnectionID() {
    const results = await this.executeSQL(`select @@SPID "SPID"`)
    const pid = results.recordset[0].SPID
    return pid
  }  
  
    validStagedDataSet(vendor,controlFilePath,controlFile) {

    /*
	**
	** Return true if, based on te contents of the control file, the data set can be consumed directly by the RDBMS using a COPY operation.
	** Return false if the data set cannot be consumed using a Copy operation
	** Do not throw errors if the data set cannot be used for a COPY operatio
	** Generate Info messages to explain why COPY cannot be used.
	**
	*/

    if (!MsSQLConstants.STAGED_DATA_SOURCES.includes(vendor)) {
       return false;
	}
	
	return this.reportCopyOperationMode(controlFile.settings.contentType === 'CSV',controlFilePath,controlFile.settings.contentType)
  }
   
  async copyOperation(tableName,copy) {
	
    /*
    **
    ** Generic Basic Imementation - Override as required for error reporting etc
    **
    */
	
	try {
	  const startTime = performance.now();
	  const stack = new Error().stack
	  let results = await this.beginTransaction();
	  results = await this.executeSQL(copy.dml);
	  const rowsRead = results.rowsAffected
	  const elapsedTime = performance.now() - startTime;
	  results = await this.commitTransaction()
  	  await this.reportCopyResults(tableName,rowsRead,0,elapsedTime,copy.dml,stack)
	} catch(e) {
	  this.yadamuLogger.handleException([this.DATABASE_VENDOR,'COPY',tableName],e)
	  let results = await this.rollbackTransaction()
	}
  }

}

module.exports = MsSQLDBI


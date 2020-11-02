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

const Yadamu = require('../../common/yadamu.js');
const YadamuConstants = require('../../common/yadamuConstants.js');
const YadamuDBI = require('../../common/yadamuDBI.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js')
const {ConnectionError} = require('../../common/yadamuError.js')
const MsSQLConstants = require('./mssqlConstants.js')
const MsSQLError = require('./mssqlError.js')
const MsSQLParser = require('./mssqlParser.js');
const MsSQLWriter = require('./mssqlWriter.js');
const StatementGenerator = require('./statementGenerator.js');
const StagingTable = require('./stagingTable.js');
const MsSQLReader = require('./mssqlReader.js');

class MsSQLDBI extends YadamuDBI {

  static get SQL_SET_CURRENT_SCHEMA()         { return _SQL_SET_CURRENT_SCHEMA }
  static get SQL_DISABLE_CONSTRAINTS()        { return _SQL_DISABLE_CONSTRAINTS }
  static get SQL_ENABLE_CONSTRAINTS()         { return _SQL_ENABLE_CONSTRAINTS }
  static get SQL_REFRESH_MATERIALIZED_VIEWS() { return _SQL_REFRESH_MATERIALIZED_VIEWS }
  static get SQL_CONFIGURE_CONNECTION()       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_GET_DLL_STATEMENTS_19C()     { return _SQL_GET_DLL_STATEMENTS_19C }
  static get SQL_GET_DLL_STATEMENTS_11G()     { return _SQL_GET_DLL_STATEMENTS_11G }
  static get SQL_DROP_WRAPPERS_11G()          { return _SQL_DROP_WRAPPERS_11G } 
  static get SQL_SCHEMA_INFORMATION()         { return _SQL_SCHEMA_INFORMATION } 
  static get SQL_CREATE_SAVE_POINT()          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return _SQL_RESTORE_SAVE_POINT }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
  
  get DB_COLLATION()           { return this._DB_COLLATION }
    
  // Override YadamuDBI

  get DATABASE_VENDOR()        { return MsSQLConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()        { return MsSQLConstants.SOFTWARE_VENDOR};
  get STATEMENT_TERMINATOR()   { return MsSQLConstants.STATEMENT_TERMINATOR };

  // Enable configuration via command line parameters

  get SPATIAL_FORMAT()         { return this.parameters.SPATIAL_FORMAT        || MsSQLConstants.SPATIAL_FORMAT }
  get SPATIAL_MAKE_VALID()     { return this.parameters.SPATIAL_MAKE_VALID    || MsSQLConstants.SPATIAL_MAKE_VALID }
  get OBJECTS_AS_JSON()        { return this.parameters.OBJECTS_AS_JSON       || MsSQLConstants.OBJECTS_AS_JSON }
  get TREAT_RAW1_AS_BOOLEAN()  { return this.parameters.TREAT_RAW1_AS_BOOLEAN || MsSQLConstants.TREAT_RAW1_AS_BOOLEAN }
  
  constructor(yadamu) {
    super(yadamu,MsSQLConstants.DEFAULT_PARAMETERS);
    this.requestProvider = undefined;
    this.transaction = undefined;
    this.pool = undefined;
    this.yadamuRollack = false
    this.tediousTransactionError = false;
    // Allow subclasses to access constants defined by the sql object. Redeclaring the SQL object in a subclass causes strange behavoir
    
    this.sql = sql
    
    sql.on('error',(err, p) => {
      this.yadamuLogger.logException([`${this.DATABASE_VENDOR}`,`mssql.onError()`],err);
      throw err
    })
        
  }

  /*
  **
  ** Local methods 
  **
  */
  
  SQL_SCHEMA_INFORMATION() {
     
    const spatialClause = this.SPATIAL_MAKE_VALID === true ? `concat('case when "',c."COLUMN_NAME",'".STIsValid() = 0 then "',c."COLUMN_NAME",'".MakeValid().${this.spatialSerializer} else "',c."COLUMN_NAME",'".${this.spatialSerializer} end "',c."COLUMN_NAME",'"')` : `concat('"',c."COLUMN_NAME",'".${this.spatialSerializer} "',c."COLUMN_NAME",'"')`
    
    return `select t."TABLE_SCHEMA" "TABLE_SCHEMA"
                  ,t."TABLE_NAME"   "TABLE_NAME"
                  ,concat('[',string_agg(concat('"',c."COLUMN_NAME",'"'),',') within group (order by "ORDINAL_POSITION"),']') "COLUMN_NAME_ARRAY"
                  ,concat('[',string_agg(case
                                           when cc."CONSTRAINT_NAME" is not NULL then 
                                             '"JSON"'
                                           else 
                                             concat('"',"DATA_TYPE",'"')
                                           end
                                         ,',') within group (order by "ORDINAL_POSITION"),']') "DATA_TYPE_ARRAY"
                  ,concat('[',string_agg(concat('"',"COLLATION_NAME",'"'),',') within group (order by "ORDINAL_POSITION"),']') "COLLATION_NAME_ARRAY"
                  ,concat('[',string_agg(case
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
                    within group (order by "ORDINAL_POSITION"),']') "SIZE_CONSTRAINT_ARRAY"
                   ,string_agg(case 
                                                  when "DATA_TYPE" = 'hierarchyid' then
                                                    concat('cast("',c."COLUMN_NAME",'" as NVARCHAR(4000)) "',c."COLUMN_NAME",'"') 
                                                  when "DATA_TYPE" in ('geometry','geography') then
                                                    ${spatialClause}
                                                  when "DATA_TYPE" = 'datetime2' then
                                                    concat('convert(VARCHAR(33),"',c."COLUMN_NAME",'",127) "',c."COLUMN_NAME",'"') 
                                                  when "DATA_TYPE" = 'datetimeoffset' then
                                                    concat('convert(VARCHAR(33),"',c."COLUMN_NAME",'",127) "',c."COLUMN_NAME",'"') 
                                                  when "DATA_TYPE" = 'xml' then
                                                    concat('replace(replace(convert(NVARCHAR(MAX),"',c."COLUMN_NAME",'"),''&#x0A;'',''\n''),''&#x20;'','' '') "',c."COLUMN_NAME",'"') 
                                                  else 
                                                    concat('"',c."COLUMN_NAME",'"') 
                                                end
                                               ,','
                                               ) 
                              within group (order by "ORDINAL_POSITION"
                             ) "CLIENT_SELECT_LIST"
              from "INFORMATION_SCHEMA"."COLUMNS" c
                   left join "INFORMATION_SCHEMA"."TABLES" t
                       on t."TABLE_CATALOG" = c."TABLE_CATALOG"
                      and t."TABLE_SCHEMA" = c."TABLE_SCHEMA"
                      and t."TABLE_NAME" = c."TABLE_NAME"
                    left outer join (
                       "INFORMATION_SCHEMA"."CONSTRAINT_COLUMN_USAGE" ccu
                       left join "INFORMATION_SCHEMA"."CHECK_CONSTRAINTS" cc
                         on cc."CONSTRAINT_CATALOG" = ccu."CONSTRAINT_CATALOG"
                        and cc."CONSTRAINT_SCHEMA" = ccu."CONSTRAINT_SCHEMA"
                        and cc."CONSTRAINT_NAME" = ccu."CONSTRAINT_NAME"
                       )
                       on ccu."TABLE_CATALOG" = c."TABLE_CATALOG"
                      and ccu."TABLE_SCHEMA" = c."TABLE_SCHEMA"
                      and ccu."TABLE_NAME" = c."TABLE_NAME"
                      and ccu."COLUMN_NAME" = c."COLUMN_NAME"
                      and UPPER("CHECK_CLAUSE") like '(ISJSON(%)>(0))'
             where t."TABLE_TYPE" = 'BASE TABLE'
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
    if ((this.parameters.YADAMU_DATABASE) && (this.parameters.YADAMU_DATABASE !== this.connectionProperties.database)) {
      this.connectionProperties.database = this.parameters.YADAMU_DATABASE
    }
  }
  
  reportTransactionState(operation) {
    const e = new Error(`Unexpected ${operation} operation`)
    this.yadamuLogger.handleException([this.DATABASE_VENDOR,'TRANSACTION MANAGER',operation],new MsSQLError(e,e.stack,this.constructor.name))
    
  }
  
  getTransactionManager() {

    // this.yadamuLogger.trace([`${this.constructor.name}.getTransactionManager()`,this.getWorkerNumber()],``)

    this.transactionInProgress = false;
    const transaction = new sql.Transaction(this.pool)
    transaction.on('rollback',async () => { 
      if (!this.yadamuRollback) {
        this.transactionInProgress = false;
        this.tediousTransactionError = true;
        this.reportTransactionState('ROLLBACK')
      }
    });
    return transaction
  }

  getRequest() {
    let stack
    try {
      stack = new Error().stack;
      const request = new sql.Request(this.requestProvider)
      request.on('info',(infoMsg) => { 
        this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`MESSAGE`],`${infoMsg.message}`);
      })
      return request;
    } catch (e) {
      throw this.captureException(new MsSQLError(e,stack,`sql.Request(${this.requestProvider.constuctor.name})`))
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
            statement.input(column,sql.VarChar(length));
            break;
          case 'nvarchar':
            statement.input(column,sql.NVarChar(length));
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
      throw this.captureException(new MsSQLError(e,stack,`sql.PreparedStatement(${sqlStatement}`))
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
      this.pool = new sql.ConnectionPool(this.connectionProperties)
      this.pool.on('error',(err, p) => {
        const cause = err instanceof MsSQLError ? err : this.captureException(new MsSQLError(err,stack,`${operation}.onError()`))
        if (!cause.suppressedError())  {
          this.yadamuLogger.handleException([this.DATABASE_VENDOR,`sql.ConnectionPool.onError()`],cause);
          if (!this.reconnectInProgress) {
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
      throw this.captureException(new MsSQLError(e,stack,operation))
    }       

    await this.configureConnection();
  }

  async _getDatabaseConnection() {
    try {
      // this.yadamuLogger.trace(this.DATABASE_VENDOR,this.getWorkerNumber()],`_getDatabaseConnection()`)
      await this.createConnectionPool();
    } catch (e) {
      const err = new ConnectionError(e,this.connectionProperties);
      throw err
    }
  } 
  
  async closeConnection() {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`closeConnection(${(this.preparedStatement !== undefined)},${this.transactionInProgress})`)
    
    if (this.preparedStatement !== undefined ) {
      await this.clearCachedStatement()
    }   
    
    if (this.transactionInProgress === true) {
      try {
        await this.rollbackTransaction()
      } catch (e) {
        throw e
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
        throw this.captureException(new MsSQLError(e,stack,psudeoSQL))
      }
    }
  }
 
  async _reconnect() {
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
        const cause = this.captureException(new MsSQLError(e,stack,sqlStatment))
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
        const cause = this.captureException(new MsSQLError(e,stack,psuedoSQL))
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
        const cause = this.captureException(new MsSQLError(e,stack,this.preparedStatement.sqlStatement))
        if (attemptReconnect && cause.lostConnection()) {
          this.preparedStatement === undefined;
          attemptReconnect = false;
          // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'PREPARED STATEMENT')
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
        const cause = this.captureException(new MsSQLError(e,stack,operation))
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

  async executeSQL(sqlStatement,args,noReconnect) {

    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.status.sqlTrace.write(this.traceSQL(sqlStatement))
    
    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
        stack = new Error().stack
        const request = this.getRequestWithArgs(args)
        const results = await request.query(sqlStatement);  
        this.traceTiming(sqlStartTime,performance.now())
        return results;
      } catch (e) {
        const cause = new  MsSQLError(e,stack,sqlStatement);
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
      , enableArithAbort : true
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

    // this.yadamuLogger.trace([`${this.constructor.name}.beginTransaction()`,this.getWorkerNumber()],``)
          
    let stack
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    const psuedoSQL = 'begin transaction'
    this.status.sqlTrace.write(this.traceSQL(psuedoSQL));
    
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
        break;
      } catch (e) {
        const cause = this.captureException(new MsSQLError(e,stack,'sql.Transaction.begin()'))
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
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
      
    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

    let attemptReconnect = this.ATTEMPT_RECONNECTION;

    let stack
    const psuedoSQL = 'commit transaction'
    this.status.sqlTrace.write(this.traceSQL(psuedoSQL));
      
    try {
      super.commitTransaction()
      const sqlStartTime = performance.now();
      stack = new Error().stack;
      await this.transaction.commit();
      this.traceTiming(sqlStartTime,performance.now())
      this.requestProvider = this.pool
    } catch (e) {
      const cause = this.captureException(new MsSQLError(e,stack,'sql.Transaction.commit()'))
      if (attemptReconnect && cause.lostConnection()) {
        attemptReconnect = false;
        // reconnect() throws cause if it cannot reconnect...
        await this.reconnect(cause,'Commit')
      }
      throw this.captureException(new MsSQLError(e,stack,'sql.Transaction.commit()'))
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
      let newIssue = this.captureException(new MsSQLError(e,stack,'sql.Transaction.rollback()'))
      this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)
    }   
    
  }
  
  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber(),this.metrics.written,this.metrics.cached],``)
    await this.executeSQL(MsSQLDBI.SQL_CREATE_SAVE_POINT);
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
      await this.executeSQL(MsSQLDBI.SQL_RESTORE_SAVE_POINT);
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
  
    const results = await this.executeSQL(MsSQLDBI.SQL_SYSTEM_INFORMATION)
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
     ,exportVersion      : Yadamu.YADAMU_VERSION
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
   
  generateMetadata(schemaInformation) {    
    const metadata = super.generateMetadata(schemaInformation) 
    schemaInformation.forEach((table,idx) => {
     if (this.applyTableFilter(table.TABLE_NAME)) {
        metadata[table.TABLE_NAME].collationNames = JSON.parse(table.COLLATION_NAME_ARRAY)
     }
    }) 
    return metadata
  }  
      
  async getSchemaInfo(keyName) {

    this.status.sqlTrace.write(this.traceComment(`@SCHEMA="${this.parameters[keyName]}"`))
      
    const statement = this.SQL_SCHEMA_INFORMATION()
    const results = await this.executeSQL(statement, { inputs: [{name: "SCHEMA", type: sql.VarChar, value: this.parameters[keyName]}]})
    
    return results.recordsets[0]
  
  }
  
  createParser(tableInfo) {
    return new MsSQLParser(tableInfo,this.yadamuLogger);
  }  
  
  streamingError(err,sqlStatement) {
     return this.captureException(new MsSQLError(err,this.streamingStackTrace,sqlStatement))
  }

  async getInputStream(tableInfo) {
    // this.yadamuLogger.trace([`${this.constructor.name}.getInputStream()`,this.getWorkerNumber()],tableInfo.TABLE_NAME)
    this.streamingStackTrace = new Error().stack;
    const request = this.getRequest();
    return new MsSQLReader(request,tableInfo.SQL_STATEMENT);
  }      

  /*
  **
  ** The following methods are used by the YADAMU DBReader class
  **
  */
    
  async generateStatementCache(schema, executeDDL) {
    /* ### OVERRIDE ### Pass additional parameter Database Name */
    const statementGenerator = new StatementGenerator(this, schema, this.metadata, this.systemInformation.spatialFormat ,this.yadamuLogger);
    this.statementCache = await statementGenerator.generateStatementCache(executeDDL, this.systemInformation.vendor, this.parameters.YADAMU_DATABASE ? this.parameters.YADAMU_DATABASE : this.connectionProperties.database)
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
  
}

module.exports = MsSQLDBI

const _SQL_SYSTEM_INFORMATION = 
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
       
const _SQL_CREATE_SAVE_POINT  = `SAVE TRANSACTION ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RESTORE_SAVE_POINT = `ROLLBACK TRANSACTION ${YadamuConstants.SAVE_POINT_NAME}`;

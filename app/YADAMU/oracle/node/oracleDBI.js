"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;
const Writable = require('stream').Writable;
const Transform = require('stream').Transform;
const PassThrough = require('stream').PassThrough
const { performance } = require('perf_hooks');

const util = require('util')
const stream = require('stream')
const pipeline = util.promisify(stream.pipeline);

/* 
**
** Require Database Vendors API 
**
*/

const oracledb = require('oracledb');
oracledb.fetchAsString = [ oracledb.DATE ]

const Yadamu = require('../../common/yadamu.js')
const YadamuConstants = require('../../common/yadamuConstants.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js')
const YadamuDBI = require('../../common/yadamuDBI.js');
const StringWriter = require('../../common/stringWriter.js');
const BufferWriter = require('../../common/bufferWriter.js');
const HexBinToBinary = require('../../common/hexBinToBinary.js');
const JSONParser = require('../../file/node/jsonParser.js');
const OracleConstants = require('./oracleConstants.js');
const OracleError = require('./oracleError.js')
const OracleParser = require('./oracleParser.js');
const OracleWriter = require('./oracleWriter.js');
const StatementGenerator = require('./statementGenerator.js');
const StatementGenerator11 = require('./statementGenerator11.js');

class OracleDBI extends YadamuDBI {

  static get SQL_CONFIGURE_CONNECTION()       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_SCHEMA_INFORMATION()         { return _SQL_SCHEMA_INFORMATION } 
  static get SQL_CREATE_SAVE_POINT()          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return _SQL_RESTORE_SAVE_POINT }

  static get SQL_SET_CURRENT_SCHEMA()         { return _SQL_SET_CURRENT_SCHEMA }
  static get SQL_DISABLE_CONSTRAINTS()        { return _SQL_DISABLE_CONSTRAINTS }
  static get SQL_ENABLE_CONSTRAINTS()         { return _SQL_ENABLE_CONSTRAINTS }
  static get SQL_REFRESH_MATERIALIZED_VIEWS() { return _SQL_REFRESH_MATERIALIZED_VIEWS }
  static get SQL_GET_DLL_STATEMENTS_19C()     { return _SQL_GET_DLL_STATEMENTS_19C }
  static get SQL_GET_DLL_STATEMENTS_11G()     { return _SQL_GET_DLL_STATEMENTS_11G }
  static get SQL_DROP_WRAPPERS_11G()          { return _SQL_DROP_WRAPPERS_11G } 

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
  
  get MAX_STRING_SIZE()        { return this._MAX_STRING_SIZE }
  get JSON_STORAGE_MODEL()     { return this._JSON_STORAGE_MODEL }
  get XML_STORAGE_MODEL()      { return this._XML_STORAGE_MODEL }
  get NATIVE_DATA_TYPE()       { return this._NATIVE_DATA_TYPE }
  get JSON_PARSING_SUPPORTED() { return this._JSON_PARSER }
    
  // Override YadamuDBI

  get DATABASE_VENDOR()        { return OracleConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()        { return OracleConstants.SOFTWARE_VENDOR};
  get STATEMENT_TERMINATOR()   { return OracleConstants.STATEMENT_TERMINATOR };
 
  // Enable configuration via command line parameters
 
  get SPATIAL_FORMAT()         { return this.parameters.SPATIAL_FORMAT         || OracleConstants.SPATIAL_FORMAT }
  get OBJECTS_AS_JSON()        { return this.parameters.OBJECTS_AS_JSON        || OracleConstants.OBJECTS_AS_JSON }
  get JSON_STORAGE_FORMAT()    { return this.parameters.JSON_STORAGE_FORMAT    || OracleConstants.JSON_STORAGE_FORMAT}
  get MIGRATE_JSON_STORAGE()   { return this.parameters.MIGRATE_JSON_STORAGE   || OracleConstants.MIGRATE_JSON_STORAGE}
  get XML_STORAGE_FORMAT()     { return this.parameters.XML_STORAGE_FORMAT     || OracleConstants.XML_STORAGE_FORMAT}
  get TREAT_RAW1_AS_BOOLEAN()  { return this.parameters.TREAT_RAW1_AS_BOOLEAN  || OracleConstants.TREAT_RAW1_AS_BOOLEAN }  
  get LOB_MAX_SIZE()           { return this.parameters.LOB_MAX_SIZE           || OracleConstants.LOB_MAX_SIZE}
  get LOB_MIN_SIZE() { 
    // Set with anonymous function to enforce 4K limit in Oracle11g
    this._LOB_MIN_SIZE = this._LOB_MIN_SIZE ||(() => { let lobMinSize = this.parameters.LOB_MIN_SIZE || OracleConstants.LOB_MIN_SIZE; lobMinSize = ((this.DB_VERSION < 12) && (lobMinSize > 4000)) ? 4000 : lobMinSize; return lobMinSize})()
    return this._LOB_MIN_SIZE 
  }
  
  /*
  **
  **  User can specify the data type to be used to store JSON Data using the parameter JSON_STORAGE_FORMAT.
  **
  **  If set to JSON the actual Data Type used is determined by the database version and is exposed as JSON_STORAGE_MODEL
  **
  **  20c : Native JSON data type
  **  19c : BLOB with IS JSON constraint
  **  18c : BLOB with IS JSON constraint
  **  12c : CLOB with IS JSON constraint
  **  11g : CLOB - No JSON support in 11g
  **
  **  JSON_STORAGE_FORMAT : Defined via Configuration Files and Command Line Parameters. The data type to be used to store JSON_DATA_TYPE
  **  JSON_STORAGE_MODEL  : The preferred data type for this database.
  **  JSON_DATA_TYPE      : The data type that will be by the current session.
  **
  **  ### What about the actual data type when dealing with an existing table ???
  **
  */
  
  get JSON_DATA_TYPE() {
    this._JSON_DATA_TYPE = this._JSON_DATA_TYPE || (() => {
      switch (true) {
        case this.NATIVE_DATA_TYPE :
          // What ever the user specified, the default is JSON, IS JSON will be specified for CLOB, BLOB or VARCHAR2
          return this.JSON_STORAGE_FORMAT;
        case this.JSON_PARSING_SUPPORTED:
          return this.JSON_STORAGE_FORMAT === 'JSON' ? this.JSON_STORAGE_MODEL : this.JSON_STORAGE_FORMAT
        default:
          return this.JSON_STORAGE_MODEL
      }
    })();
    return this._JSON_DATA_TYPE
  }
  
  get XML_STORAGE_CLAUSE() {
    this._XML_STORAGE_CLAUSE = this._XML_STORAGE_CLAUSE || (() => {
       return this.XML_STORAGE_FORMAT === 'XML' ? this.XML_STORAGE_MODEL : this.XML_STORAGE_FORMAT
    })()
    return this._XML_STORAGE_CLAUSE
  }
  
  constructor(yadamu) {
	  
    super(yadamu,OracleConstants.DEFAULT_PARAMETERS);

	// make oracledb constants available to decendants of OracleDBI
	
	this.oracledb = oracledb
	
    this.ddl = [];
	this.dropWrapperStatements = []
    this.systemInformation = undefined;
	
	// Oracle always has a transaction in progress, so beginTransaction is a no-op
	
	this.transactionInProgress = true;
	
	// this.yadamuLogger.trace([this.DATABASE_VENDOR],'Constructor Complete');
  }

  getConnectionProperties() {
    
    if (this.parameters.USERID) {
      return this.parseConnectionString(this.parameters.USERID)
    }
    else {
     return{
       user             : this.parameters.USER
     , password         : this.parameters.PASSWORD
     , connectionString : this.parameters.CONNECT_STRING
     }
    }
  }
  
  parseConnectionString(connectionString) {
    
    const user = YadamuLibrary.convertQuotedIdentifer(connectionString.substring(0,connectionString.indexOf('/')));
    let password = connectionString.substring(connectionString.indexOf('/')+1);
    let connectString = '';
    if (password.indexOf('@') > -1) {
	  connectString = password.substring(password.indexOf('@')+1);
	  password = password.substring(password,password.indexOf('@'));
      console.log(`${new Date().toISOString()}[WARNING][${this.constructor.name}]: Suppling a password on the command line interface can be insecure`);
    }
    return {
      user          : user,
      password      : password,
      connectString : connectString
    }
  }     

  async testConnection(connectionProperties,parameters) {   
    super.setConnectionProperties(connectionProperties);
	try {
      const conn = await oracledb.getConnection(connectionProperties)
      await conn.close();
	  super.setParameters(parameters)
	} catch (e) {
      throw e;
	}
	
  }
  
  async createConnectionPool() {
	let stack;
    this.logConnectionProperties();
	const sqlStartTime = performance.now();
	this.connectionProperties.poolMax = this.yadamu.PARALLEL ? parseInt(this.yadamu.PARALLEL) + 1 : 3
	try {
      stack = new Error().stack
      // this.yadamuLogger.trace([this.DATABASE_VENDOR],'Creating Pool');
	  this.pool = await oracledb.createPool(this.connectionProperties);
      // this.yadamuLogger.trace([this.DATABASE_VENDOR],'Pool Created');
      this.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
	  throw this.captureException(new OracleError(e,stack,'Oracledb.createPool()'))
	}
  }
  
  async getConnectionFromPool() {
	
	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`getConnectionFromPool()`)
	
	//  Do not Configure Connection here. 
	
	let stack;
    this.status.sqlTrace.write(this.traceComment(`Gettting Connection From Pool.`));
	try {
      stack = new Error().stack
      const sqlStartTime = performance.now();
      // this.yadamuLogger.trace([this.DATABASE_VENDOR],'Requestng Connection From Pool');
	  const connection = await this.pool.getConnection();
      // this.yadamuLogger.trace([this.DATABASE_VENDOR],'Connection Assigned');
      this.traceTiming(sqlStartTime,performance.now())
	  return connection
    } catch (e) {
	  throw this.captureException(new OracleError(e,stack,'Oracledb.Pool.getConnection()'))
	}
	
  }

  async getConnection() {
    this.logConnectionProperties();
	const sqlStartTime = performance.now();
	this.connection = await oracledb.getConnection(this.connectionProperties);
	this.traceTiming(sqlStartTime,performance.now())
  }
  
  async closeConnection() {
	  
	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`closeConnection(${(this.connection !== undefined && (typeof this.connection.close === 'function'))})`)
	
	// console.log(new Error().stack)
	
	if (this.connection !== undefined && (typeof this.connection.close === 'function')) {
      let stack;
      try {
        stack = new Error().stack
        await this.connection.close();
        this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
  	    throw this.captureException(new OracleError(e,stack,'Oracledb.Connection.close()'))
	  }
	}
  };

  async closePool(drainTime) {
	  
    // this.yadamuLogger.trace([this.DATABASE_VENDOR],`closePool(${(this.pool instanceof oracledb.Pool) && (this.pool.status === oracledb.POOL_STATUS_OPEN)},${drainTime})`)
	
    if ((this.pool instanceof oracledb.Pool) && (this.pool.status === oracledb.POOL_STATUS_OPEN)) {
      let stack;
      try {
        if (drainTime !== undefined) {
          stack = new Error().stack
		  await this.pool.close(drainTime);
		}
	    else {
          stack = new Error().stack
		  await this.pool.close();	
	    }
        this.pool = undefined
      } catch (e) {
        this.pool = undefined
	    throw this.captureException(new OracleError(e,stack,'Oracledb.Pool.close()'))
      }
    }
  }  

  async _reconnect() {
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.manager.getConnectionFromPool()
  }

  async createLob(lobType) {

    let stack
    try {
      const sqlStartTime = performance.now();
	  stack = new Error().stack
      const lob =  await this.connection.createLob(lobType);
      this.traceTiming(sqlStartTime,performance.now())
	  return lob;
   	} catch (e) {
	  throw this.captureException(new OracleError(e,stack,`Oracledb.Connection.createLob()`))
    }
  }

  async closeLob(lob) {
	let stack
	const operation = 'oracledb.LOB.close()'
    try {
      await lob.close()
	} catch (e) {
      this.yadamuLogger.handleException([this.constructor.name,'CLOSE_LOB'],new OracleError(e,stack,operation))
    }
  }	 

  async blobToBuffer(blob) {
	  
	let stack
	const operation = 'oracledb.BLOB.pipe(Buffer)'
    try {
      const bufferWriter = new BufferWriter();
	  stack = new Error().stack
  	  await pipeline(blob,bufferWriter)
  	  await this.closeLob(blob)
      return bufferWriter.toBuffer()
	} catch(e) {
	  await this.closeLob(blob)
	  throw new OracleError(e,stack,operation)
	}
  }	

  async clobToString(clob) {
     
    let stack
	const operation = 'oracledb.CLOB.pipe(String)'
	try {
      const stringWriter = new  StringWriter();
      clob.setEncoding('utf8');  // set the encoding so we get a 'string' not a 'buffer'
  	  await pipeline(clob,stringWriter)
	  await this.closeLob(clob)
	  return stringWriter.toString()
	} catch(e) {
	  await this.closeLob(clob)
	  throw new OracleError(e,stack,operation)
	}
  };

  async clientClobToString(clob) {
     // ### Ugly workaround due to the fact it does not appear possible to directly re-read a local CLOB 
     const sql = `select :tempClob "newClob" from dual`;
     const results = await this.executeSQL(sql,{tempClob:clob});
     return await this.clobToString(results.rows[0][0])
  }

  async clientBlobToBuffer(blob) {
     // ### Ugly workaround due to the fact it does not appear possible to directly re-read a local BLOB 
     const sql = `select :tempBlob "newBlob" from dual`;
     const results = await this.executeSQL(sql,{tempBlob:blob});
     return await this.blobToBuffer(results.rows[0][0])
  }
    
  async streamToBlob(readable) {
    
    let stack
	const operation = 'buffer.pipe(oracledb.BLOB)'
    try {
      const blob = await this.createLob(oracledb.BLOB);
      stack = new Error().stack
  	  await pipeline(readable,blob)
      // this.yadamuLogger.trace([this.DATABASE_VENDOR,'WRITE TO BLOB'],`Bytes Written: ${blob.offset-1}.`)
	  return blob
	} catch(e) {
	  throw e instanceof OracleError ? e : new OracleError(e,stack,operation)
	}
  };

  async fileToBlob(filename) {
     const stream = await new Promise((resolve,reject) => {
     const inputStream = fs.createReadStream(filename);
       inputStream.on('open',() => {resolve(inputStream)}).on('error',(err) => {reject(err)})
    })
    return this.streamToBlob(stream);
  };
  
  stringToBlob(string) {
    const stream = new Readable();
    stream.push(string);
    stream.push(null);
    return this.streamToBlob(stream);
  };

  blobFromBuffer(buffer) {
     let stream = new Readable ();
     stream.push(buffer);
     stream.push(null);
     return this.streamToBlob(stream);
  }

  async jsonToBlob(json) { 
    return this.stringToBlob(JSON.stringify(json))
  };
    
  async streamToClob(readable) {
    
    let stack
	const operation = 'readable.pipe(oracledb.CLOB)'
    try {
      const clob = await this.createLob(oracledb.CLOB);
      stack = new Error().stack
  	  await pipeline(readable,clob)
      // this.yadamuLogger.trace([this.DATABASE_VENDOR,'WRITE TO CLOB'],`Characters Written: ${clob.offset-1}.`)
	  return clob
	} catch(e) {
	  throw e instanceof OracleError ? e : new OracleError(e,stack,operation)
	}
  };

  stringToClob(str) {  
    const s = new Readable();
    s.push(str);
    s.push(null);
    return this.streamToClob(s)
    
  }

  jsonToClob(json) {  
    const s = new Readable();
    s.push(JSON.stringify(json));
    s.push(null);
    return this.streamToClob(s);
    
  }
  
  getDateFormatMask(vendor) {
    
    return OracleConstants.DATE_FORMAT_MASKS[vendor] ? OracleConstants.DATE_FORMAT_MASKS[vendor] : OracleConstants.DATE_FORMAT_MASKS.Oracle
 
  }
  
  getTimeStampFormatMask(vendor) {
    
    return OracleConstants.TIMESTAMP_FORMAT_MASKS[vendor] ? OracleConstants.TIMESTAMP_FORMAT_MASKS[vendor] : OracleConstants.TIMESTAMP_FORMAT_MASKS.Oracle
 
  }
  
  statementTooLarge(sql) {

    return sql.some((sqlStatement) => {
      return sqlStatement.length > this.MAX_STRING_SIZE
    })      
  }
  
  async setDateFormatMask(conn,status,vendor) {

    const SQL_SET_DATE_FORMAT = `ALTER SESSION SET NLS_DATE_FORMAT = '${this.getDateFormatMask(vendor)}' NLS_TIMESTAMP_FORMAT = '${this.getTimeStampFormatMask(vendor)}' NLS_TIMESTAMP_TZ_FORMAT = '${this.getTimeStampFormatMask(vendor)}'`

    status.sqlTrace.write(this.traceSQL(SQL_SET_DATE_FORMAT));
    let result = await conn.execute(SQL_SET_DATE_FORMAT);
  }
   
  async configureConnection() {
	  
    const SQL_SET_TIMESTAMP_FORMAT = `ALTER SESSION SET TIME_ZONE = '+00:00' NLS_DATE_FORMAT = '${this.getDateFormatMask('Oracle')}' NLS_TIMESTAMP_FORMAT = '${this.getTimeStampFormatMask('Oracle')}' NLS_TIMESTAMP_TZ_FORMAT = '${this.getTimeStampFormatMask('Oracle')}' NLS_LENGTH_SEMANTICS = 'CHAR'`

    let result = await this.executeSQL(SQL_SET_TIMESTAMP_FORMAT,{});

    let args = {
		DB_VERSION:         {dir: oracledb.BIND_OUT, type: oracledb.STRING}, 
		MAX_STRING_SIZE:    {dir: oracledb.BIND_OUT, type: oracledb.NUMBER}, 
		JSON_STORAGE_MODEL: {dir: oracledb.BIND_OUT, type: oracledb.STRING}, 
    	XML_STORAGE_MODEL:  {dir: oracledb.BIND_OUT, type: oracledb.STRING}, 
		JSON_PARSING:       {dir: oracledb.BIND_OUT, type: oracledb.STRING}, 
		NATIVE_JSON_TYPE:   {dir: oracledb.BIND_OUT, type: oracledb.STRING}
	}
	
    result = await this.executeSQL(OracleDBI.SQL_CONFIGURE_CONNECTION,args);
    
    this._DB_VERSION = parseFloat(result.outBinds.DB_VERSION);
    this._MAX_STRING_SIZE = result.outBinds.MAX_STRING_SIZE;
    this._XML_STORAGE_MODEL = result.outBinds.XML_STORAGE_MODEL;
	this._JSON_STORAGE_MODEL = result.outBinds.JSON_STORAGE_MODEL;
    this._NATIVE_DATA_TYPE = result.outBinds.NATIVE_JSON_TYPE === 'TRUE';
	this._JSON_PARSER = result.outBinds.JSON_PARSING === 'TRUE';
	
    if ((this.isManager()) && (this.MAX_STRING_SIZE < 32768)) {
      const lobMinSize = this.LOB_MIN_SIZE
      this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`${this.DB_VERSION}`,`Configuration`],`Maximum VARCHAR2 size for JSON operations is ${this.MAX_STRING_SIZE}.`)
    }    	
    
    if ((this.isManager()) && (this.DB_VERSION < 12)) {
       this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`${this.DB_VERSION}`,`Configuration`],`LOB_MIN_SIZE set to ${this.LOB_MIN_SIZE}.`)
    }	

    if ((this.isManager()) && (this.XML_STORAGE_CLAUSE !== this.XML_STORAGE_MODEL )) {
       this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`${this.DB_VERSION}`,`Configuration`],`XMLType storage model is ${this.XML_STORAGE_CLAUSE}.`)
    }	

	if (this.isManager()) {
      this.yadamuLogger.info([this.DATABASE_VENDOR,this.DB_VERSION,`Configuration`],`JSON storage model is ${this.JSON_DATA_TYPE}.`)
	}

  }    
  
  processLog(results,operation) {
    if (results.outBinds.log !== null) {
      const log = JSON.parse(results.outBinds.log.replace(/\\r/g,'\\n'));
      this.logSummary = super.processLog(log, operation, this.status, this.yadamuLogger)
	  return log
    }
    else {
      return null
    }
  }

  async setCurrentSchema(schema) {

    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 1024} , schema:schema}
    const results = await this.executeSQL(OracleDBI.SQL_SET_CURRENT_SCHEMA,args)
    this.processLog(results,'Set Current Schema')
    this.currentSchema = schema;
  }
  
  /*
  ** 
  ** The following methods are used by the YADAMU DBwriter class
  **
  */

  async disableConstraints() {
  
    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH} , schema:this.parameters.TO_USER}
    const results = await this.executeSQL(OracleDBI.SQL_DISABLE_CONSTRAINTS,args)
    this.processLog(results,'Disable Constraints')

  }
    
  async enableConstraints() {
	  
    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH} , schema:this.parameters.TO_USER} 
    const results = await this.executeSQL(OracleDBI.SQL_ENABLE_CONSTRAINTS,args)
    this.processLog(results,'Enable Constraints')
    
  }
  
  async refreshMaterializedViews() {
      
    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH} , schema:this.parameters.TO_USER}     
    const results = await this.executeSQL(OracleDBI.SQL_REFRESH_MATERIALIZED_VIEWS,args)
    this.processLog(results,'Materialized View Refresh')

  }
    
  async executeMany(sqlStatement,rows,binds) {
 
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

    if (rows.length > 0) {
      this.status.sqlTrace.write(this.traceComment(`Bulk Operation: ${rows.length} records.`))
      this.status.sqlTrace.write(this.traceSQL(sqlStatement));
      
  	  let stack
	  let results;
      while (true) {
        // Exit with result or exception.  
        try {
          const sqlStartTime = performance.now();
          stack = new Error().stack
          results = await this.connection.executeMany(sqlStatement,rows,binds);
	      this.traceTiming(sqlStartTime,performance.now())
		  return results;
        } catch (e) {
		  const cause = new OracleError(e,stack,sqlStatement,binds,{rows : rows.length})
		  if (attemptReconnect && cause.lostConnection()) {
            attemptReconnect = false;
		    // reconnect() throws cause if it cannot reconnect...
            await this.reconnect(cause,'BATCH')
            await this.setCurrentSchema(this.parameters.TO_USER)
		    await this.setDateFormatMask(this.connection,this.status,this.systemInformation.vendor);
		    continue;
          }		  	  
		  throw this.captureException(cause)
        }      
      } 
	}
  }

  async executeSQL(sqlStatement,args,outputFormat) {
     
    let attemptReconnect = this.ATTEMPT_RECONNECTION;

	args = args === undefined ? {} : args
	outputFormat = outputFormat === undefined ? {} : outputFormat
		
    let stack
	let results
    this.status.sqlTrace.write(this.traceSQL(sqlStatement));

    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
        stack = new Error().stack
        results = await this.connection.execute(sqlStatement,args,outputFormat);
        this.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = new OracleError(e,stack,sqlStatement,args,outputFormat)
        if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'SQL')
          await this.setCurrentSchema(this.parameters.TO_USER)
		  await this.setDateFormatMask(this.connection,this.status,this.systemInformation ? this.systemInformation.vendor : "oracle");
		  continue;
        }
        throw this.captureException(cause)
      }      
    } 
  }  

  async applyDDL(ddl,sourceSchema,targetSchema) {
	  
    let sqlStatement = `declare V_ABORT BOOLEAN;begin V_ABORT := YADAMU_EXPORT_DDL.APPLY_DDL_STATEMENT(:statement,:sourceSchema,:targetSchema); :abort := case when V_ABORT then 1 else 0 end; end;`; 
    let args = {abort:{dir: oracledb.BIND_OUT, type: oracledb.NUMBER} , statement:{type: oracledb.CLOB, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH, val:null}, sourceSchema:sourceSchema, targetSchema:this.parameters.TO_USER};
	
	if ((this.DB_VERSION < 12) && (this.XML_STORAGE_CLAUSE === 'CLOB')) {
       // Force XMLType Store as CLOB ???
	   args.statement.value = `ALTER SESSION SET EVENTS = ''1050 trace name context forever,level 0x2000'`;
       const results = await this.executeSQL(sqlStatement,args);
    }
    
    for (const ddlStatement of ddl) {
      args.statement.val = ddlStatement
      const results = await this.executeSQL(sqlStatement,args);
      if (results.outBinds.abort === 1) {
        break;
      }
    }
    
    sqlStatement = `begin :log := YADAMU_EXPORT_DDL.GENERATE_LOG(); end;`; 
    args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH}};
    const results = await this.executeSQL(sqlStatement,args);   
    return this.processLog(results,'DDL Operation');
  }

  async convertDDL2XML(ddlStatements) {
    const ddl = ddlStatements.map((ddlStatement) => { return `<ddl>${ddlStatement.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}</ddl>`}).join('\n')
    return this.stringToBlob(`<ddlStatements>\n${ddl}\n</ddlStatements>`);
  }
  
  remapJSONColumns(jsonColumns,ddl) {
	
	 // Migrate JSON columns to Native JSON Datatype
	 
     ddl.forEach((ddlStatement,idx) => {
       jsonColumns.forEach((json) => {
		  if (ddlStatement.indexOf(`ALTER TABLE "${json.owner}"."${json.tableName}" ADD CHECK (`) === 0) {		
		    const constraintTokens = ddlStatement.substring(ddlStatement.indexOf('(')+1,ddlStatement.lastIndexOf(')')).split(' ');
			if ((constraintTokens.length > 2) && (constraintTokens[1].toUpperCase() === 'IS') &&  (constraintTokens[2].toUpperCase() === 'JSON')) {
			  ddl[idx] = null;
			}
		  }
		  if (ddlStatement.indexOf(`CREATE TABLE "${json.owner}"."${json.tableName}"`) === 0) {
		    const lines = ddlStatement.split('\n');
			lines.forEach((line,idx) => {
			  // Look for the line that defines the target column.
  		      const columnOffset = line.indexOf(`"${json.columnName}" ${json.dataType}`)
			  if (columnOffset > -1) {
				// Generate a new line.
				lines[idx] = `${line.trim().startsWith('(') ? '  (' : ''}\t"${json.columnName}" ${this.JSON_DATA_TYPE}${line.indexOf('NOT NULL ENABLE') > -1 ? ' NOT NULL ENABLE' : ''}${line.trim().endsWith(',') ? ',' : ''}`
			  }
		    })
            ddl[idx] = lines.join('\n');
		  }
		})
     });
	 
	 // Strip NULL entries
     return ddl.filter((n) => {return n !== null})
	 
  }
  
  remapObjectColumns(ddl) {
	  
	 // Migrate Object Columns to JSON.
	  
     ddl.forEach((ddlStatement,idx) => {
       if (ddlStatement.indexOf(`CREATE TABLE`) === 0) {
         const lines = ddlStatement.split('\n');
         const createTableTokens = lines[0].split(' ')
	     lines.forEach((line,idx) => {
		   const qualifiedTableName = createTableTokens[2]
 		   if (line.indexOf('\t') > -1) {
  	         const tokens = line.substr(line.indexOf('\t')).split(' ')
			 // Do nor remap Oracle Spatial Objects
		     if ((tokens[1].indexOf('"."') > -1) && (tokens[1].indexOf("MDSYS") !== 0)) {
		       tokens[1] = this.JSON_DATA_TYPE;
               lines[idx] = `${line.substr(0,line.indexOf('\t'))}${tokens.join(' ')}`;
			   if (this.JSON_PARSING_SUPPORTED && !this.JSON_DATA_TYPE_SUPPORTED) {
				 ddl.push(`ALTER TABLE ${qualifiedTableName} ADD CHECK (${tokens[0]} IS JSON)`)
			   }
			 }
		   }
		 })
         ddl[idx] = lines.join('\n');
	   }
	 })
	 
	 return ddl;
  }		   
	    
  async _executeDDL(ddl) {
	const jsonColumns = JSON.parse(ddl.shift())
	
	// Replace \r with \n.. Earlier database versions generate ddl statements with \r characters.
	
	ddl = ddl.map((ddlStatement) => {
      return ddlStatement.replace(/\r/g,'\n')
	});	
	
    if (jsonColumns.jsonColumns !== null) {
	  if ((this.MIGRATE_JSON_STORAGE === true))   {
		 //### Do not remap JSON columns during export. Leave it until import to leave open the possiblility roundtripping objects via JSON.
	    ddl = this.remapJSONColumns(jsonColumns.jsonColumns,ddl)
	  } 
	}
	
	if (this.systemInformation.objectFormat === 'JSON') {
	  ddl = this.remapObjectColumns(ddl)
    }
	
    if ((this.MAX_STRING_SIZE < 32768) && (this.statementTooLarge(ddl))) {
      // DDL statements are too large send for server based execution (JSON Extraction will fail)
      await this.applyDDL(ddl,this.systemInformation.schema,this.parameters.TO_USER);
    }
    else {
      // ### OVERRIDE ### - Send Set of DDL operations to the server for execution   
      const sqlStatement = `begin :log := YADAMU_EXPORT_DDL.APPLY_DDL_STATEMENTS(:ddl,:sourceSchema,:targetSchema); end;`;
      const ddlLob = await (this.DB_VERSION < 12 ? this.convertDDL2XML(ddl) : this.jsonToBlob({ddl : ddl}))
     
      const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH} , ddl:ddlLob, sourceSchema:this.systemInformation.schema, targetSchema:this.parameters.TO_USER};
      const results = await this.executeSQL(sqlStatement,args);
      await ddlLob.close();
      const log = this.processLog(results,'DDL Execution')
    }
	
    this.yadamuLogger.ddl([`${this.DATABASE_VENDOR}`],`Errors: ${this.logSummary.errors}, Warnings: ${this.logSummary.warnings}, Ingnoreable ${this.logSummary.ignoreable}, Duplicates: ${this.logSummary.duplicates}, Unresolved: ${this.logSummary.reference}, Compilation: ${this.logSummary.recompilation}, Miscellaneous ${this.logSummary.aq}.`)

  }
  

  
  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
    
  async initialize() {
    await super.initialize(true);
  }
    
  /*
  **
  **  Gracefully close down the database connection.
  **
  */

  async initializeExport() {
    await this.setCurrentSchema(this.parameters.FROM_USER)
  }

  async finalizeExport() {
	this.checkConnectionState(this.fatalError) 
    await this.setCurrentSchema(this.connectionProperties.user);
  }

  async initializeImport() {
    await this.setCurrentSchema(this.parameters.TO_USER)
  }

  async initializeData() {
    await this.disableConstraints();
    await this.setDateFormatMask(this.connection,this.status,this.systemInformation.vendor);
  }
  
  async finalizeData() {
	// this.yadamuLogger.trace([this.DATABASE_VENDOR],`finalizeData()`);
    this.checkConnectionState(this.fatalError) 
	await this.refreshMaterializedViews();
    await this.enableConstraints();
  }  

  async finalizeImport() {
    this.checkConnectionState(this.fatalError) 
	await this.setCurrentSchema(this.connectionProperties.user);
  }

  /*
  **
  **  Gracefully close down the database connection and pool.
  **
  */
  
  async finalize() {
	// Oracle11g: Drop any wrappers that were created
	await Promise.all(this.dropWrapperStatements.map((sqlStatement) => {
	  return this.executeSQL(sqlStatement,{})
	}))
    await super.finalize();
  }

  /*
  **
  **  Abort the database connection and pool.
  **
  */

  async abort() {
    await super.abort(0); 
  }
  
  async beginTransaction() {
	super.beginTransaction()
  }
  
  /*
  **
  ** Commit the current transaction
  **
  */
  async commitTransaction() {

    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

    this.status.sqlTrace.write(this.traceSQL(`commit transaction`));

	let stack
    const sqlStartTime = performance.now();
	try {
	  super.commitTransaction()
      stack = new Error().stack
      await this.connection.commit();
  	  this.traceTiming(sqlStartTime,performance.now())
	} catch (e) {
	  throw this.captureException(new OracleError(e,stack,`Oracledb.Transaction.commit()`))
	}
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction(cause) {
	  
    // this.yadamuLogger.trace([`${this.constructor.name}.rollbackTransaction()`,this.getWorkerNumber()],``)

	this.checkConnectionState(cause)
	// If rollbackTransaction was invoked due to encounterng an error and the rollback operation results in a second exception being raised, log the exception raised by the rollback operation and throw the original error.
	// Note the underlying error is not thrown unless the rollback itself fails. This makes sure that the underlying error is not swallowed if the rollback operation fails.

    this.status.sqlTrace.write(this.traceSQL(`rollback transaction`));

	let stack
    const sqlStartTime = performance.now();
	try {
	  super.rollbackTransaction()
      stack = new Error().stack
      await this.connection.rollback();
  	  this.traceTiming(sqlStartTime,performance.now())
	} catch (e) {
	  const newIssue = this.captureException(new OracleError(e,stack,`Oracledb.Transaction.rollback()`))
	  this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)
	}	
  }
  
  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)

    await this.executeSQL(OracleDBI.SQL_CREATE_SAVE_POINT,[]);
	super.createSavePoint()
  }

  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)

    this.checkConnectionState(cause)
	
	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.

	try {
	  await this.executeSQL(OracleDBI.SQL_RESTORE_SAVE_POINT,[]);
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
      
     if (this.MAX_STRING_SIZE > 32767) {
       const json = await this.fileToBlob(importFilePath);
       return json;
     }
     else {
		 
       // Need to capture the SystemInformation and DDL objects of the export file to make sure the DDL can be processed on the RDBMS.
       // If any DDL statement exceeds MAX_STRING_SIZE then DDL will have to executed statement by statement from the client
       // 'Tee' the input stream used to create the temporary lob that contains the export file and pass it through the JSON Parser.
       // If any of the DDL operations exceed the maximum string size supported by server side JSON operations cache the ddl statements on the client
       
	   
       const inputStream = await new Promise((resolve,reject) => {
         const inputStream = fs.createReadStream(importFilePath);
         inputStream.on('open',() => {resolve(inputStream)}).on('error',(err) => {reject(err)})
       })
      
	   const multiplexor = new PassThrough();
       const jsonParser = new JSONParser(this.yadamuLogger,'DDL_ONLY',importFilePath);
	   const ddlCache = new DDLCache(this.yadamuLogger,multiplexor,jsonParser);
	   multiplexor.pipe(jsonParser).pipe(ddlCache)
	   
       const blob = await this.createLob(oracledb.BLOB);
       await pipeline(inputStream,multiplexor,blob)

       const ddl = ddlCache.getDDL();
       if ((ddl.length > 0) && this.statementTooLarge(ddl)) {
         this.ddl = ddl
         this.systemInformation = ddlCache.getSystemInformation();
       }
       return blob
     }
  }

  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */
  
  async processFile(hndl) {

    /*
    **
    ** If the ddl array is populdated DDL operations have to be executed from the client.
    **
    */

    let settings = '';
    switch (this.MODE) {
	   case 'DDL_AND_DATA':
         if (this.ddl.length > 0) {
           // Execute the DDL statement by statement.
           await this.applyDDL(this.ddl);
           settings = `YADAMU_IMPORT.DATA_ONLY_MODE(TRUE);\n  YADAMU_IMPORT.DDL_ONLY_MODE(FALSE);`;
         }
         else {
           settings = `YADAMU_IMPORT.DATA_ONLY_MODE(FALSE);\n  YADAMU_IMPORT.DDL_ONLY_MODE(FALSE);`;
         }
	     break;
	   case 'DATA_ONLY':
         settings = `YADAMU_IMPORT.DATA_ONLY_MODE(TRUE);\n  YADAMU_IMPORT.DDL_ONLY_MODE(FALSE);`;
         break;
	   case 'DDL_ONLY':
         if (this.ddl.length > 0) {
           // Execute the DDL statement by statement
          await his.applyDDL(this.ddl);
           settings = `YADAMU_IMPORT.DDL_ONLY_MODE(TRUE);\n  YADAMU_IMPORT.DATA_ONLY_MODE(TRUE);`;
         }
         else {
           settings = `YADAMU_IMPORT.DDL_ONLY_MODE(TRUE);\n  YADAMU_IMPORT.DATA_ONLY_MODE(FALSE);`;
         }
	     break;
    }	 
	 
	const sqlStatement = `begin\n  ${settings}\n  :log := YADAMU_IMPORT.IMPORT_JSON(:json, :schema, :JSON_STORAGE_MODEL, :XML_STORAGE_MODEL);\nend;`;
	const results = await this.executeSQL(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH}, json:hndl, schema:this.parameters.TO_USER, JSON_STORAGE_MODEL: this.JSON_STORAGE_MODEL, XML_STORAGE_MODEL: this.XML_STORAGE_CLAUSE})
    return this.processLog(results,'JSON_TABLE');  
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

	const results = await this.executeSQL(OracleDBI.SQL_SYSTEM_INFORMATION,{sysInfo:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH}})

    return Object.assign({
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT 
	 ,objectFormat       : this.OBJECTS_AS_JSON === true ? 'JSON' : 'NATIVE'
     ,schema             : this.parameters.FROM_USER ? this.parameters.FROM_USER : this.parameters.TO_USER
     ,softwareVendor     : this.SOFTWARE_VENDOR
     ,exportVersion      : Yadamu.EXPORT_VERSION
     ,nodeClient         : {
        version          : process.version
       ,architecture     : process.arch
       ,platform         : process.platform
      }
     ,oracleDriver       : {
        oracledbVersion  : oracledb.versionString
       ,clientVersion    : oracledb.oracleClientVersionString
       ,serverVersion    : this.connection.oracleServerVersionString
      }
    },JSON.parse(results.outBinds.sysInfo));
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */
  
  async getDDLOperations() {

    let ddl;
    let results;
    let bindVars
    	
    switch (true) {
      case this.DB_VERSION < 12.2:
        /*
        **
        ** The pipelined table approach used by YADAMU_EXPORT_DDL appears to fail starting with release 19c. 
        ** Using Dynamic SQL appears to work correctly.
        ** 
        */     
        bindVars = {v1 : this.parameters.FROM_USER, v2 : {dir : oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH}};
        results = await this.executeSQL(OracleDBI.SQL_GET_DLL_STATEMENTS_11G,bindVars)
        ddl = JSON.parse(results.outBinds.v2);
        break;
      case this.DB_VERSION < 19:
        results = await this.executeSQL(OracleDBI.SQL_GET_DLL_STATEMENTS,{schema: this.parameters.FROM_USER},{outFormat: oracledb.OBJECT,fetchInfo:{JSON:{type: oracledb.STRING}}})
        ddl = results.rows.map((row) => {
          return row.JSON;
        });
        break;
      default:
        /*
        **
        ** The pipelined table approach used by YADAMU_EXPORT_DDL appears to fail starting with release 19c. 
        ** Using Dynamic SQL appears to work correctly.
        **  
        */
     
        bindVars = {v1 : this.parameters.FROM_USER, v2 : {dir : oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH}};
        results = await this.executeSQL(OracleDBI.SQL_GET_DLL_STATEMENTS_19C,bindVars)
        ddl = JSON.parse(results.outBinds.v2);
    }
    return ddl;    

  }

  async getSchemaInfo(keyName) {

    const results = await this.executeSQL(OracleDBI.SQL_SCHEMA_INFORMATION
	                                     ,{schema: this.parameters[keyName], spatialFormat: this.SPATIAL_FORMAT, objectsAsJSON : new Boolean(this.OBJECTS_AS_JSON).toString().toUpperCase(), raw1AsBoolean: new Boolean(this.TREAT_RAW1_AS_BOOLEAN).toString().toUpperCase()}
										 ,{outFormat: 
										    oracledb.OBJECT
										   ,fetchInfo: {
                                              COLUMN_NAME_ARRAY:     {type: oracledb.STRING}
                                             ,DATA_TYPE_ARRAY:       {type: oracledb.STRING}
                                             ,SIZE_CONSTRAINT_ARRAY: {type: oracledb.STRING}
                                             ,CLIENT_SELECT_LIST:    {type: oracledb.STRING}
                                             ,EXPORT_SELECT_LIST:    {type: oracledb.STRING}
                                             ,WITH_CLAUSE:           {type: oracledb.STRING}
                                             ,SQL_STATEMENT:         {type: oracledb.STRING}
											 ,PARTITION_LIST:        {type: oracledb.STRING}
	                                        }
                                          }
    )
	
    const schemaInformation = results.rows
    return schemaInformation;
  }

  mapLongIdentifers(metadata) {
        
    // ### Todo Add better algorthim than simple tuncation. Check for Duplicates and use counter when duplicates are detected.

    const tableMappings = {}
    let mappingRequired = false;
    const tables = Object.keys(metadata)    
    tables.forEach((table,idx) => {
      const tableName = metadata[table].tableName
      if (tableName.length > 30) {
        mappingRequired = true;
        const newTableName = tableName.substring(0,29);
        tableMappings[table] = {tableName : newTableName}
	    this.yadamuLogger.warning([this.DATABASE_VENDOR,tableName],`Mapped to "${newTableName}".`)
        metadata[table].tableName = newTableName;
      }
      const columnNames = metadata[table].columnNames
      let mapColumns = false;
      let columnMappings = {}
      columnNames.forEach((columnName,idx) => {
        if (columnName.length > 30) {
          mappingRequired = true;
          mapColumns = true;
          const newColumnName = columnName.substring(0,29);
          columnMappings[columnName] = newColumnName
          this.yadamuLogger.warning([this.DATABASE_VENDOR,metadata[table].tableName,columnName],`Mapped to "${newColumnName}".`)
          columnNames[idx] = newColumnName
        }
      });
      if (mapColumns) {
        metadata[table].sqlColumnList = '"' + columnNames.join('","')  + '"'
		metadata[table].columnNames = columnNames
        if (tableMappings[table]) {
          tableMappings[table].columnNames = columnMappings;
        }
        else {
          tableMappings[table] = {tableName : tableName, columnNames : columnMappings}
        }
      }
    })        
	
	return mappingRequired ? tableMappings : undefined

  }    

  validateIdentifiers(metadata) {     
     return this.DB_VERSION < 12 ? this.mapLongIdentifers(metadata) : undefined
  }
  
  generateQueryInformation(tableMetadata) {
    
    // Generate a conventional relational select statement for this table
    const tableInfo = super.generateQueryInformation(tableMetadata)    

    tableInfo.jsonColumns = [];           
    tableInfo.DATA_TYPE_ARRAY.forEach((dataType,idx) => {
      switch (dataType) {
        case 'JSON':
          tableInfo.jsonColumns.push(idx);
          break
        case "GEOMETRY":
        case "\"MDSYS\".\"SDO_GEOMETRY\"":
        case "XMLTYPE":
        case "ANYDATA":
		  break;
        case "BFILE":
		  if (this.OBJECTS_AS_JSON === true) { 
            tableInfo.jsonColumns.push(idx);
	      }
		  break;
        default:
		  if ((this.OBJECTS_AS_JSON === true) && (dataType.indexOf('.') > -1)){ 
            tableInfo.jsonColumns.push(idx);
	      }
      }
    })
    
    return tableInfo
  }
  
  createParser(tableInfo) {
	const parser = new OracleParser(tableInfo,this.yadamuLogger); 
    this.inputStream.on('metadata',(metadata) => {parser.setColumnMetadata(metadata)})
	return parser;
  }  
  
  streamingError(cause,sqlStatement) {
	return this.captureException(new OracleError(cause,this.streamingStackTrace,sqlStatement))
  }
  
  async getInputStream(tableInfo) {

    if (tableInfo.WITH_CLAUSE !== null) {
      if (this.DB_VERSION < 12) {
		// The "WITH_CLAUSE" is a create procedure statement that creates a stored procedure that wraps the required conversions
        await this.executeSQL(tableInfo.WITH_CLAUSE,{})
		// The procedure needs to be dropped once the operation is complete.
		const wrapperName = tableInfo.WITH_CLAUSE.substring(tableInfo.WITH_CLAUSE.indexOf('"."')+3,tableInfo.WITH_CLAUSE.indexOf('"('))
		const sqlStatement = OracleDBI.SQL_DROP_WRAPPERS_11G.replace(':1:',this.parameters.FROM_USER).replace(':2:',wrapperName);
		this.dropWrapperStatements.push(sqlStatement);
      }
      else {
	   tableInfo.SQL_STATEMENT = `with\n${tableInfo.WITH_CLAUSE}\n${tableInfo.SQL_STATEMENT}`;
      }
	}

    let attemptReconnect = this.ATTEMPT_RECONNECTION;

    this.status.sqlTrace.write(this.traceSQL(tableInfo.SQL_STATEMENT))

    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
		this.streamingStackTrace = new Error().stack
        this.inputStream = await this.connection.queryStream(tableInfo.SQL_STATEMENT,[],{extendedMetaData: true}) 
	    this.traceTiming(sqlStartTime,performance.now())
	    return this.inputStream
	  } catch (e) {
		const cause = new OracleError(e,this.streamingStackTrace ,tableInfo.SQL_STATEMENT,{},{})
        if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'SQL')
          await this.setCurrentSchema(this.parameters.TO_USER)
		  await this.setDateFormatMask(this.connection,this.status,this.systemInformation.vendor);
		  continue;
        }
        throw cause		  
      }      
    } 
  }  
    
  /*
  **
  ** The following methods are used by the YADAMU DBWriter class
  **
  */
  
  async generateStatementCache(schema,executeDDL) {

    let statementGenerator 
    if (this.DB_VERSION < 12) {
      statementGenerator = new StatementGenerator11(this,schema,this.metadata,this.systemInformation.spatialFormat)
    }
    else {
      statementGenerator = new StatementGenerator(this,schema,this.metadata,this.systemInformation.spatialFormat)
    }
    this.statementCache = await statementGenerator.generateStatementCache(executeDDL,this.systemInformation.vendor)
  }

  getOutputStream(tableName,ddlComplete) {
	 return super.getOutputStream(OracleWriter,tableName,ddlComplete)
  }
 
  classFactory(yadamu) {
	return new OracleDBI(yadamu)
  }
  		  
  async workerDBI(workerNumber) {
    const dbi = await super.workerDBI(workerNumber)
    await dbi.setCurrentSchema(this.currentSchema);
    await dbi.setDateFormatMask(dbi.connection,this.status,this.systemInformation.vendor);
    return dbi;
  }

  async getConnectionID() {
	const results = await this.executeSQL(`SELECT SID, SERIAL# FROM V$SESSION WHERE AUDSID = Sys_Context('USERENV', 'SESSIONID')`)
	return {sid : results.rows[0][0], serial: results.rows[0][1]}
  }
}

class DDLCache extends Transform {
  
  constructor(yadamuLogger, passThrough, jsonParser) {
    super({objectMode: true });
	this.yadamuLogger = yadamuLogger
    this.systemInformation = undefined;
	this.passThrough = passThrough;
	this.jsonParser = jsonParser;
    this.ddl = [] 
   
  }

  async _transform(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
		case 'systemInformation':
          this.systemInformation = obj.systemInformation
          callback();
          break;
        case 'ddl':
          this.ddl = obj.ddl;
		  this.passThrough.unpipe(this.jsonParser);
		  this.jsonParser.destroy()
		  callback();
        case 'metadata':
		case 'table':
		default:
		  this.passThrough.unpipe(this.jsonParser);
		  this.jsonParser.destroy()
		  callback();
          break;
      }
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._transform()`],e);
      callback(e);
    }
  }
  
  getDDL() {
    return this.ddl;
  }
  
  getSystemInformation() {
    return this.systemInformation
  }
}
 
module.exports = OracleDBI

// SQL Statements

const _SQL_SET_CURRENT_SCHEMA         = `begin :log := YADAMU_IMPORT.SET_CURRENT_SCHEMA(:schema); end;`;

const _SQL_DISABLE_CONSTRAINTS        = `begin :log := YADAMU_IMPORT.DISABLE_CONSTRAINTS(:schema); end;`;

const _SQL_ENABLE_CONSTRAINTS         = `begin :log := YADAMU_IMPORT.ENABLE_CONSTRAINTS(:schema); end;`;

const _SQL_REFRESH_MATERIALIZED_VIEWS = `begin :log := YADAMU_IMPORT.REFRESH_MATERIALIZED_VIEWS(:schema); end;`;

const _SQL_CONFIGURE_CONNECTION = 
`begin 
   :DB_VERSION := YADAMU_EXPORT.DATABASE_RELEASE(); 
   :MAX_STRING_SIZE := YADAMU_FEATURE_DETECTION.C_MAX_STRING_SIZE; 
   :JSON_STORAGE_MODEL := YADAMU_IMPORT.C_JSON_STORAGE_MODEL; 
   :XML_STORAGE_MODEL := YADAMU_IMPORT.C_XML_STORAGE_MODEL; 
   if YADAMU_FEATURE_DETECTION.JSON_PARSING_SUPPORTED then :JSON_PARSING := 'TRUE'; else :JSON_PARSING := 'FALSE'; end if;
   if YADAMU_FEATURE_DETECTION.JSON_DATA_TYPE_SUPPORTED then :NATIVE_JSON_TYPE := 'TRUE'; else :NATIVE_JSON_TYPE := 'FALSE'; end if;
 end;`;

const _SQL_SYSTEM_INFORMATION   = `begin :sysInfo := YADAMU_EXPORT.GET_SYSTEM_INFORMATION(); end;`;

const _SQL_GET_DLL_STATEMENTS   = `select COLUMN_VALUE JSON from TABLE(YADAMU_EXPORT_DDL.FETCH_DDL_STATEMENTS(:schema))`;

const _SQL_GET_DLL_STATEMENTS_19C = `declare
  JOB_NOT_ATTACHED EXCEPTION;
  PRAGMA EXCEPTION_INIT( JOB_NOT_ATTACHED , -31623 );
  
  V_SCHEMA           VARCHAR2(128) := :V1;

  V_HDL_OPEN         NUMBER;
  V_HDL_TRANSFORM    NUMBER;

  V_DDL_STATEMENTS SYS.KU$_DDLS;
  V_DDL_STATEMENT  CLOB;
  
  C_NEWLINE          CONSTANT CHAR(1) := CHR(10);
  C_CARRIAGE_RETURN  CONSTANT CHAR(1) := CHR(13);
  C_SINGLE_QUOTE     CONSTANT CHAR(1) := CHR(39);
  
  V_RESULT JSON_ARRAY_T := new JSON_ARRAY_T();
  
  cursor indexedColumnList(C_SCHEMA VARCHAR2)
  is
   select aic.TABLE_NAME, aic.INDEX_NAME, LISTAGG(COLUMN_NAME,',') WITHIN GROUP (ORDER BY COLUMN_POSITION) INDEXED_EXPORT_SELECT_LIST
     from ALL_IND_COLUMNS aic
     join ALL_ALL_TABLES aat
       on aic.TABLE_NAME = aat.TABLE_NAME and aic.TABLE_OWNER = aat.OWNER
    where aic.TABLE_OWNER = C_SCHEMA
    group by aic.TABLE_NAME, aic.INDEX_NAME;

  CURSOR heirachicalTableList(C_SCHEMA VARCHAR2)
  is
  select distinct TABLE_NAME
    from ALL_XML_TABLES axt
   where exists(
           select 1
             from ALL_TAB_COLS atc
            where axt.TABLE_NAME = atc.TABLE_NAME and axt.OWNER = atc.OWNER and atc.COLUMN_NAME = 'ACLOID' and atc.HIDDEN_COLUMN = 'YES'
         )
     and exists(
           select 1
             from ALL_TAB_COLS atc
            where axt.TABLE_NAME = atc.TABLE_NAME and axt.OWNER = atc.OWNER and atc.COLUMN_NAME = 'OWNERID' and atc.HIDDEN_COLUMN = 'YES'
        )
    and OWNER = C_SCHEMA;


  V_JSON_COLUMNS   CLOB;
begin
--
  select JSON_OBJECT('jsonColumns' value 
           JSON_ARRAYAGG(
		     JSON_OBJECT(
			  'owner' value OWNER, 'tableName' value TABLE_NAME, 'columnName' value COLUMN_NAME, 'jsonFormat' value FORMAT, 'dataType' value  DATA_TYPE
		     )
         $IF YADAMU_FEATURE_DETECTION.CLOB_SUPPORTED $THEN
             returning CLOB
		   )
           returning CLOB
         $ELSIF YADAMU_FEATURE_DETECTION.EXTENDED_STRING_SUPPORTED $THEN
             returning VARCHAR2(32767)
		   )
           returning VARCHAR2(32767)
         )
         $ELSE   
             returning VARCHAR2(4000)
		   ) 
		   returning VARCHAR2(4000)
		 $END
       )
	into V_JSON_COLUMNS
    from ALL_JSON_COLUMNS
   where OBJECT_TYPE = 'TABLE'
     and OWNER = V_SCHEMA;
		 
  V_RESULT.APPEND(V_JSON_COLUMNS);

  -- Use DBMS_METADATA package to access the XMLSchemas registered in the target database schema

  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',false);

  begin
    V_HDL_OPEN := DBMS_METADATA.OPEN('XMLSCHEMA');
    DBMS_METADATA.SET_FILTER(V_HDL_OPEN,'SCHEMA',V_SCHEMA);
    loop
      -- TO DO Switch to FETCH_DDL and process table of statements..
      V_DDL_STATEMENT := DBMS_METADATA.FETCH_CLOB(V_HDL_OPEN);
      EXIT WHEN V_DDL_STATEMENT IS NULL;
      -- Strip leading and trailing white space from DDL statement
      V_DDL_STATEMENT := TRIM(BOTH C_NEWLINE FROM V_DDL_STATEMENT);
      V_DDL_STATEMENT := TRIM(BOTH C_CARRIAGE_RETURN FROM V_DDL_STATEMENT);
      V_DDL_STATEMENT := TRIM(V_DDL_STATEMENT);
      if (TRIM(V_DDL_STATEMENT) <> '10 10') then
        V_RESULT.APPEND(V_DDL_STATEMENT);
      end if;
    end loop;

    DBMS_METADATA.CLOSE(V_HDL_OPEN);
  exception
    when JOB_NOT_ATTACHED then
      DBMS_METADATA.CLOSE(V_HDL_OPEN);
    when others then
      RAISE;
  end;

  -- Use DBMS_METADATA package to access the DDL statements used to create the database schema

  begin
    V_HDL_OPEN := DBMS_METADATA.OPEN('SCHEMA_EXPORT');
    DBMS_METADATA.SET_FILTER(V_HDL_OPEN,'SCHEMA',V_SCHEMA);

    V_HDL_TRANSFORM := DBMS_METADATA.ADD_TRANSFORM(V_HDL_OPEN,'DDL');

    -- Suppress Segement information for TABLES, INDEXES and CONSTRAINTS

    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'SEGMENT_ATTRIBUTES',false,'TABLE');
    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'SEGMENT_ATTRIBUTES',false,'INDEX');
    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'SEGMENT_ATTRIBUTES',false,'CONSTRAINT');

    -- Return constraints as 'ALTER TABLE' operations

    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'CONSTRAINTS_AS_ALTER',true,'TABLE');
    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'REF_CONSTRAINTS',false,'TABLE');

    -- Exclude XML Schema Info. XML Schemas need to come first and are handled in the previous section

    DBMS_METADATA.SET_FILTER(V_HDL_OPEN,'EXCLUDE_PATH_EXPR','=''XMLSCHEMA''');

    loop
      -- Get the next batch of DDL_STATEMENTS. Each batch may contain zero or more spaces.
      V_DDL_STATEMENTS := DBMS_METADATA.FETCH_DDL(V_HDL_OPEN);
      EXIT WHEN V_DDL_STATEMENTS IS NULL;
      for i in 1 .. V_DDL_STATEMENTS.count loop

        V_DDL_STATEMENT := V_DDL_STATEMENTS(i).DDLTEXT;

        -- Strip leading and trailing white space from DDL statement
        V_DDL_STATEMENT := TRIM(BOTH C_NEWLINE FROM V_DDL_STATEMENT);
        V_DDL_STATEMENT := TRIM(BOTH C_CARRIAGE_RETURN FROM V_DDL_STATEMENT);
        V_DDL_STATEMENT := TRIM(V_DDL_STATEMENT);
        if (DBMS_LOB.getLength(V_DDL_STATEMENT) > 0) then
          V_RESULT.APPEND(V_DDL_STATEMENT);
        end if;
      end loop;
    end loop;

    DBMS_METADATA.CLOSE(V_HDL_OPEN);

  exception
    when JOB_NOT_ATTACHED then
      DBMS_METADATA.CLOSE(V_HDL_OPEN);
    when others then
      RAISE;
  end;

  -- Renable the heirarchy for any heirachically enabled tables in the export file

  for t in heirachicalTableList(V_SCHEMA) loop
    V_RESULT.APPEND('begin DBMS_XDBZ.ENABLE_HIERARCHY(SYS_CONTEXT(''USERENV'',''CURRENT_SCHEMA''),''' || t.TABLE_NAME  || '''); end;');
  end loop;

  for i in indexedColumnList(V_SCHEMA) loop
    V_RESULT.APPEND('begin YADAMU_EXPORT_DDL.RENAME_INDEX(''' || i.TABLE_NAME  || ''',''' || i.INDEXED_EXPORT_SELECT_LIST || ''',''' || i.INDEX_NAME || '''); end;');
  end loop;

  :V2 :=  V_RESULT.to_CLOB();
  
end;`;

const _SQL_GET_DLL_STATEMENTS_11G = 
`declare
  JOB_NOT_ATTACHED EXCEPTION;
  PRAGMA EXCEPTION_INIT( JOB_NOT_ATTACHED , -31623 );
  
  V_RESULT YADAMU_UTILITIES.KVP_TABLE := YADAMU_UTILITIES.KVP_TABLE();
  
  V_SCHEMA           VARCHAR2(128) := :V1;

  V_HDL_OPEN         NUMBER;
  V_HDL_TRANSFORM    NUMBER;

  V_DDL_STATEMENTS SYS.KU$_DDLS;
  V_DDL_STATEMENT  CLOB;
  
  C_NEWLINE          CONSTANT CHAR(1) := CHR(10);
  C_CARRIAGE_RETURN  CONSTANT CHAR(1) := CHR(13);
  C_SINGLE_QUOTE     CONSTANT CHAR(1) := CHR(39);
   
  cursor indexedColumnList(C_SCHEMA VARCHAR2)
  is
   select aic.TABLE_NAME, aic.INDEX_NAME, LISTAGG(COLUMN_NAME,',') WITHIN GROUP (ORDER BY COLUMN_POSITION) INDEXED_EXPORT_SELECT_LIST
     from ALL_IND_COLUMNS aic
     join ALL_ALL_TABLES aat
       on aic.TABLE_NAME = aat.TABLE_NAME and aic.TABLE_OWNER = aat.OWNER
    where aic.TABLE_OWNER = C_SCHEMA
    group by aic.TABLE_NAME, aic.INDEX_NAME;

  CURSOR heirachicalTableList(C_SCHEMA VARCHAR2)
  is
  select distinct TABLE_NAME
    from ALL_XML_TABLES axt
   where exists(
           select 1
             from ALL_TAB_COLS atc
            where axt.TABLE_NAME = atc.TABLE_NAME and axt.OWNER = atc.OWNER and atc.COLUMN_NAME = 'ACLOID' and atc.HIDDEN_COLUMN = 'YES'
         )
     and exists(
           select 1
             from ALL_TAB_COLS atc
            where axt.TABLE_NAME = atc.TABLE_NAME and axt.OWNER = atc.OWNER and atc.COLUMN_NAME = 'OWNERID' and atc.HIDDEN_COLUMN = 'YES'
        )
    and OWNER = C_SCHEMA;

begin

  V_RESULT.extend(1);
  V_RESULT(V_RESULT.COUNT) := YADAMU_UTILITIES.KVC(NULL,'{"jsonColumns":null}');

  -- Use DBMS_METADATA package to access the XMLSchemas registered in the target database schema

  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',false);

  begin
    V_HDL_OPEN := DBMS_METADATA.OPEN('XMLSCHEMA');
    DBMS_METADATA.SET_FILTER(V_HDL_OPEN,'SCHEMA',V_SCHEMA);
    loop
      -- TO DO Switch to FETCH_DDL and process table of statements..
      V_DDL_STATEMENT := DBMS_METADATA.FETCH_CLOB(V_HDL_OPEN);
      EXIT WHEN V_DDL_STATEMENT IS NULL;
      -- Strip leading and trailing white space from DDL statement
      V_DDL_STATEMENT := TRIM(BOTH C_NEWLINE FROM V_DDL_STATEMENT);
      V_DDL_STATEMENT := TRIM(BOTH C_CARRIAGE_RETURN FROM V_DDL_STATEMENT);
      V_DDL_STATEMENT := TRIM(V_DDL_STATEMENT);
      if (TRIM(V_DDL_STATEMENT) <> '10 10') then
        V_RESULT.extend(1);
        V_RESULT(V_RESULT.COUNT) := YADAMU_UTILITIES.KVC(NULL,V_DDL_STATEMENT);
      end if;
    end loop;

    DBMS_METADATA.CLOSE(V_HDL_OPEN);
  exception
    when JOB_NOT_ATTACHED then
      DBMS_METADATA.CLOSE(V_HDL_OPEN);
    when others then
      RAISE;
  end;

  -- Use DBMS_METADATA package to access the DDL statements used to create the database schema

  begin
    V_HDL_OPEN := DBMS_METADATA.OPEN('SCHEMA_EXPORT');
    DBMS_METADATA.SET_FILTER(V_HDL_OPEN,'SCHEMA',V_SCHEMA);

    V_HDL_TRANSFORM := DBMS_METADATA.ADD_TRANSFORM(V_HDL_OPEN,'DDL');

    -- Suppress Segement information for TABLES, INDEXES and CONSTRAINTS

    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'SEGMENT_ATTRIBUTES',false,'TABLE');
    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'SEGMENT_ATTRIBUTES',false,'INDEX');
    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'SEGMENT_ATTRIBUTES',false,'CONSTRAINT');

    -- Return constraints as 'ALTER TABLE' operations

    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'CONSTRAINTS_AS_ALTER',true,'TABLE');
    DBMS_METADATA.SET_TRANSFORM_PARAM(V_HDL_TRANSFORM,'REF_CONSTRAINTS',false,'TABLE');

    -- Exclude XML Schema Info. XML Schemas need to come first and are handled in the previous section
    DBMS_METADATA.SET_FILTER(V_HDL_OPEN,'EXCLUDE_PATH_EXPR','=''XMLSCHEMA''');

    -- Exclude Statisticstype
    DBMS_METADATA.SET_FILTER(V_HDL_OPEN,'EXCLUDE_PATH_EXPR','=''STATISTICS''');

    loop
      -- Get the next batch of DDL_STATEMENTS. Each batch may contain zero or more spaces.
      V_DDL_STATEMENTS := DBMS_METADATA.FETCH_DDL(V_HDL_OPEN);
      EXIT WHEN V_DDL_STATEMENTS IS NULL;
      for i in 1 .. V_DDL_STATEMENTS.count loop

        V_DDL_STATEMENT := V_DDL_STATEMENTS(i).DDLTEXT;

        -- Strip leading and trailing white space from DDL statement
        V_DDL_STATEMENT := TRIM(BOTH C_NEWLINE FROM V_DDL_STATEMENT);
        V_DDL_STATEMENT := TRIM(BOTH C_CARRIAGE_RETURN FROM V_DDL_STATEMENT);
        V_DDL_STATEMENT := TRIM(V_DDL_STATEMENT);
        if (DBMS_LOB.getLength(V_DDL_STATEMENT) > 0) then
          V_RESULT.extend(1);
          V_RESULT(V_RESULT.COUNT) := YADAMU_UTILITIES.KVC(NULL,V_DDL_STATEMENT);
        end if;
      end loop;
    end loop;

    DBMS_METADATA.CLOSE(V_HDL_OPEN);

/*
  exception
    when JOB_NOT_ATTACHED then
      DBMS_METADATA.CLOSE(V_HDL_OPEN);
    when others then
      RAISE;
*/
  end;

  -- Renable the heirarchy for any heirachically enabled tables in the export file

  for t in heirachicalTableList(V_SCHEMA) loop
    V_RESULT.extend(1);
    V_RESULT(V_RESULT.COUNT) := YADAMU_UTILITIES.KVC(NULL,'begin DBMS_XDBZ.ENABLE_HIERARCHY(SYS_CONTEXT(''USERENV'',''CURRENT_SCHEMA''),''' || t.TABLE_NAME  || '''); end;');
  end loop;

  for i in indexedColumnList(V_SCHEMA) loop
    V_RESULT.extend(1);
    V_RESULT(V_RESULT.COUNT) := YADAMU_UTILITIES.KVC(NULL,'begin YADAMU_EXPORT_DDL.RENAME_INDEX(''' || i.TABLE_NAME  || ''',''' || i.INDEXED_EXPORT_SELECT_LIST || ''',''' || i.INDEX_NAME || '''); end;');
  end loop;

  :V2 := YADAMU_UTILITIES.JSON_ARRAY_CLOB(V_RESULT);
  
end;`;

const _SQL_DROP_WRAPPERS_11G = `declare
  OBJECT_NOT_FOUND EXCEPTION;
  PRAGMA EXCEPTION_INIT( OBJECT_NOT_FOUND , -4043 );
begin
  execute immediate 'DROP FUNCTION ":1:".":2:"';
exception
  when OBJECT_NOT_FOUND then
    NULL;
  when others then
    RAISE;
end;`

const _SQL_SCHEMA_INFORMATION = `select * from table(YADAMU_EXPORT.GET_DML_STATEMENTS(:schema,:spatialFormat,:objectsAsJSON,:raw1AsBoolean))`;

const _SQL_CREATE_SAVE_POINT  = `SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RESTORE_SAVE_POINT = `ROLLBACK TO ${YadamuConstants.SAVE_POINT_NAME}`;

  

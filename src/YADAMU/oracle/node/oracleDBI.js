"use strict" 
const fs = require('fs');
const path = require('path');
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
oracledb.fetchAsString = [ oracledb.DATE, oracledb.NUMBER ]

const YadamuDBI = require('../../common/yadamuDBI.js');
const DBIConstants = require('../../common/dbiConstants.js');
const YadamuConstants = require('../../common/yadamuConstants.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js')

const OracleConstants = require('./oracleConstants.js');
const OracleError = require('./oracleException.js')
const OracleParser = require('./oracleParser.js');
const OracleWriter = require('./oracleWriter.js');
const OracleStatementLibrary = require('./oracleStatementLibrary.js');
const StatementGenerator = require('./statementGenerator.js');

const StringWriter = require('../../common/stringWriter.js');
const BufferWriter = require('../../common/bufferWriter.js');
const HexBinToBinary = require('../../common/hexBinToBinary.js');
const JSONParser = require('../../file/node/jsonParser.js');
const {FileError, FileNotFound, DirectoryNotFound} = require('../../file/node/fileException.js');

class OracleDBI extends YadamuDBI {

  static #_YADAMU_DBI_PARAMETERS

  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.YADAMU_DBI_PARAMETERS,OracleConstants.DBI_PARAMETERS))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
	return OracleDBI.YADAMU_DBI_PARAMETERS
  }	 

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called 
  
  get MAX_STRING_SIZE()            { return this._MAX_STRING_SIZE }
  get JSON_STORAGE_MODEL()         { return this._JSON_STORAGE_MODEL }
  get XML_STORAGE_MODEL()          { return this._XML_STORAGE_MODEL }
  get NATIVE_DATA_TYPE()           { return this._NATIVE_DATA_TYPE }
  get JSON_PARSING_SUPPORTED()     { return this._JSON_PARSER }
							      
  // Override YadamuDBI           
							      
  get DATABASE_KEY()               { return OracleConstants.DATABASE_KEY};
  get DATABASE_VENDOR()            { return OracleConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()            { return OracleConstants.SOFTWARE_VENDOR};
  get SQL_COPY_OPERATIONS()        { return true }
  get PARTITION_LEVEL_OPERATIONS() { return true }
  get STATEMENT_TERMINATOR()       { return OracleConstants.STATEMENT_TERMINATOR };
  get STATEMENT_SEPERATOR()        { return OracleConstants.STATEMENT_SEPERATOR };
  get DBI_PARAMETERS()             { return OracleConstants.DBI_PARAMETERS }

 
  // Enable configuration via command line parameters
 
  get SPATIAL_FORMAT()             { return this.parameters.SPATIAL_FORMAT              || OracleConstants.SPATIAL_FORMAT }
  get OBJECT_FORMAT()              { return this.parameters.OBJECT_FORMAT               || OracleConstants.OBJECT_FORMAT }
  get ORACLE_XML_TYPE()            { return this.parameters.ORACLE_XML_TYPE             || OracleConstants.ORACLE_XML_TYPE}
  get ORACLE_JSON_TYPE()           { return this.parameters.ORACLE_JSON_TYPE            || OracleConstants.ORACLE_JSON_TYPE}
  get MIGRATE_JSON_STORAGE()       { return this.parameters.MIGRATE_JSON_STORAGE        || OracleConstants.MIGRATE_JSON_STORAGE}
  get TREAT_RAW1_AS_BOOLEAN()      { return this.parameters.TREAT_RAW1_AS_BOOLEAN       || OracleConstants.TREAT_RAW1_AS_BOOLEAN }  
  get BYTE_TO_CHAR_RATIO()         { return this.parameters.BYTE_TO_CHAR_RATIO          || OracleConstants.BYTE_TO_CHAR_RATIO };
  get COPY_LOGFILE_DIRNAME()       { return this.parameters.COPY_LOGFILE_DIRNAME        || OracleConstants.COPY_LOGFILE_DIRNAME };
  get COPY_BADFILE_DIRNAME()       { return this.parameters.COPY_BADFILE_DIRNAME        || OracleConstants.COPY_BADFILE_DIRNAME };
  get BATCH_TEMPLOB_LIMIT()        { return this.parameters.BATCH_TEMPLOB_LIMIT         || OracleConstants.BATCH_TEMPLOB_LIMIT}
  get BATCH_CACHELOB_LIMIT()       { return this.parameters.BATCH_CACHELOB_LIMIT        || OracleConstants.BATCH_CACHELOB_LIMIT}
  get LOB_MAX_SIZE()               { return this.parameters.LOB_MAX_SIZE                || OracleConstants.LOB_MAX_SIZE}
  get LOB_MIN_SIZE() { 
    // Set with anonymous function to enforce 4K limit in Oracle11g
    this._LOB_MIN_SIZE = this._LOB_MIN_SIZE ||(() => { let lobMinSize = this.parameters.LOB_MIN_SIZE || OracleConstants.LOB_MIN_SIZE; lobMinSize = ((this.DB_VERSION < 12) && (lobMinSize > 4000)) ? 4000 : lobMinSize; return lobMinSize})()
    return this._LOB_MIN_SIZE 
  }

  get COMMIT_TEMPLOB_LIMIT() {
    this._COMMIT_TEMPLOB_LIMIT = this._COMMIT_TEMPLOB_LIMIT || (() => { return this.BATCH_TEMPLOB_LIMIT * (this.COMMIT_RATIO || 1)})()
    return this._COMMIT_TEMPLOB_LIMIT 
  }

  get COMMIT_CACHELOB_LIMIT() { 
    this._COMMIT_CACHELOB_LIMIT = this._COMMIT_CACHELOB_LIMIT || (() => { return this.BATCH_CACHELOB_LIMIT * (this.COMMIT_RATIO || 1)})()
    return this._COMMIT_CACHELOB_LIMIT 
  }

   get OBJECTS_AS_JSON()        { return this.OBJECT_FORMAT === 'JSON' }
  
  /*
  **
  ** Use parameter ORACLE_JSON_TYPE to determine how JSON content is stored in the database.
  **
  ** Set to BLOB or CLOB to force BLOB or CLOB storage regardless of Database Version. 
  **
  ** Set to JSON to allow driver to pick storage model based on Database Version. Choices are shown below:
  **
  **  20c : Native JSON data type
  **  19c : BLOB with IS JSON constraint
  **  18c : BLOB with IS JSON constraint
  **  12c : CLOB with IS JSON constraint
  **  11g : CLOB - No JSON support in 11g
  **
  **  ORACLE_JSON_TYPE    : Value is derived from Constants, Configuration Files and Command Line Parameters. Default is JSON
  **  JSON_STORAGE_MODEL  : The recommended Storage Model for this version of the database.
  **  JSON_DATA_TYPE      : The data type that will be used by the driver. 
  **
  **  ### What about the actual data type when dealing with an existing table ???
  **
  */
  
  get JSON_DATA_TYPE() {
    this._JSON_DATA_TYPE = this._JSON_DATA_TYPE || (() => {
      switch (true) {
        case this.NATIVE_DATA_TYPE :
          // What ever the user specified, the default is JSON, IS JSON will be specified for CLOB, BLOB or VARCHAR2
          return this.ORACLE_JSON_TYPE;
        case this.JSON_PARSING_SUPPORTED:
          return this.ORACLE_JSON_TYPE === 'JSON' ? this.JSON_STORAGE_MODEL : this.ORACLE_JSON_TYPE
        default:
          return this.JSON_STORAGE_MODEL
      }
    })();
    return this._JSON_DATA_TYPE
  }
  
  /*
  **
  ** Use parameter ORACLE_XML_TYPE to determine how XML content is stored in the database.
  **
  ** Set to CLOB to force XMLTYPE STORE AS CLOB, which provides best chance of preserving XML Fidelity.
  ** Set to BINARY to force XMLTYPE STORE AS BINARY XML, which provides best performance but not guarantee XML Fidelity in all use cases.
  ** Set to XML to allow the driver to pck the storage model based on the Database Version.  
  **
  **
  ** OBJECT RELATAIONAL XML is only supported when migrating between Oracle Databases in DDL_AND_DATA mode
  **
  **  ORACLE_XML_TYPE    : Value is derived from Constants, Configuration Files and Command Line Parameters. Default is JSON
  **  XML_STORAGE_MODEL  : The recommended Storage Model for this version of the database.
  **  JSON_DATA_TYPE     : The data type that will be used by the driver. 
  **
  **  ### What about the actual data type when dealing with an existing table ???
  **
  */
  
  get XML_STORAGE_CLAUSE() {
    this._XML_STORAGE_CLAUSE = this._XML_STORAGE_CLAUSE || (() => {
	   switch (this.ORACLE_XML_TYPE) {
		 case 'XML' : 
		   switch (this.XML_STORAGE_MODEL) {
			  case 'CLOB':
			    return 'CLOB';
		      case 'BINARY':
			  default:
			    return 'BINARY XML';
           }			  
		 case 'CLOB':
		   return 'CLOB'
		 case 'BINARY':
		 default:
		   return 'BINARY XML';
       }
    })()
    return this._XML_STORAGE_CLAUSE
  }
  
  constructor(yadamu,settings,parameters) {
    super(yadamu,settings,parameters);
	
	// make oracledb constants available to decendants of OracleDBI	
	this.oracledb = oracledb
	
    this.ddl = [];
	this.dropWrapperStatements = []
    this.systemInformation = undefined;
	
	this.StatementGenerator = StatementGenerator
	this.StatementLibrary = OracleStatementLibrary
	this.statementLibrary = undefined
	
	// Oracle always has a transaction in progress, so beginTransaction is a no-op
	
	this.TRANSACTION_IN_PROGRESS = true;
	// this.yadamuLogger.trace([this.DATABASE_VENDOR],'Constructor Complete');

  }
  
  parseConnectionString(vendorProperties, connectionString) {
    
    const user = YadamuLibrary.convertQuotedIdentifer(connectionString.substring(0,connectionString.indexOf('/')));
    
	let password = connectionString.substring(connectionString.indexOf('/')+1)
	
    let connectString = '';
    if (password.indexOf('@') > -1) {
	  connectString = password.substring(password.indexOf('@')+1);
	  password = password.substring(password,password.indexOf('@'));
      console.log(`${new Date().toISOString()}[WARNING][${this.constructor.name}]: Suppling a password on the command line interface can be insecure`);
    }
	
    vendorProperties.user             = user          || vendorProperties.user
    vendorProperties.password         = password      || vendorProperties.password
    vendorProperties.connectString    = connectString || vendorProperties.connectString 
  }     

  updateVendorProperties(vendorProperties) {

    if (this.parameters.USERID) {
      this.parseConnectionString(vendorProperties,this.parameters.USERID)
    }
    else {
     vendorProperties.user             = this.parameters.USER            || vendorProperties.user 
     vendorProperties.password         = this.parameters.PASSWORD        || vendorProperties.password  
     vendorProperties.connectString    = this.parameters.CONNECT_STRING  || vendorProperties.connectString 
    }
  }
  
  async testConnection(connectionProperties,parameters) {  
    super.setConnectionProperties(connectionProperties);
	try {
      const conn = await oracledb.getConnection(this.vendorProperties)
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
	this.vendorProperties.poolMax = this.yadamu.PARALLEL ? parseInt(this.yadamu.PARALLEL) + 1 : 3
	try {
      stack = new Error().stack
      // this.yadamuLogger.trace([this.DATABASE_VENDOR],'Creating Pool');
	  this.pool = await oracledb.createPool(this.vendorProperties);
      // this.yadamuLogger.trace([this.DATABASE_VENDOR],'Pool Created');
      this.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
	  throw this.trackExceptions(new OracleError(e,stack,'Oracledb.createPool()'))
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
	  throw this.trackExceptions(new OracleError(e,stack,'Oracledb.Pool.getConnection()'))
	}
	
  }

  async getConnection() {
    this.logConnectionProperties();
	const sqlStartTime = performance.now();
	const connection = await oracledb.getConnection(this.vendorProperties);
	this.traceTiming(sqlStartTime,performance.now())
    return connection
  }
  
  async closeConnection(options) {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`closeConnection(${(this.connection !== undefined && (typeof this.connection.close === 'function'))})`)
	
	if (this.connection !== undefined && (typeof this.connection.close === 'function')) {
      let stack;
      try {
        stack = new Error().stack
        await this.connection.close();
        this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
  	    throw this.trackExceptions(new OracleError(e,stack,'Oracledb.Connection.close()'))
	  }
	}
  };

  async closePool(options) {
	  
    // this.yadamuLogger.trace([this.DATABASE_VENDOR],`closePool(${(this.pool instanceof oracledb.Pool) && (this.pool.status === oracledb.POOL_STATUS_OPEN)},${options.drainTime})`)
	
    if ((this.pool instanceof oracledb.Pool) && (this.pool.status === oracledb.POOL_STATUS_OPEN)) {
      let stack;
      try {
        if (options.drainTime !== undefined) {
          stack = new Error().stack
		  await this.pool.close(options.drainTime);
		}
	    else {
          stack = new Error().stack
		  await this.pool.close();	
	    }
        this.pool = undefined
      } catch (e) {
        this.pool = undefined
	    throw this.trackExceptions(new OracleError(e,stack,'Oracledb.Pool.close()'))
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
      // this.traceTiming(sqlStartTime,performance.now())
	  return lob;
   	} catch (e) {
	  throw this.trackExceptions(new OracleError(e,stack,`Oracledb.Connection.createLob()`))
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
	let operation = 'oracledb.BLOB.pipe(Buffer)'
    try {
      const bufferWriter = new BufferWriter();
	  stack = new Error().stack
  	  await pipeline(blob,bufferWriter)
	  operation = 'oracledb.LOB.close(BLOB)'
  	  await this.closeLob(blob)
      return bufferWriter.toBuffer()
	} catch(e) {
	  await this.closeLob(blob)
	  throw new OracleError(e,stack,operation)
	}
  }	

  async clobToString(clob) {
     
    let stack
	let operation = 'oracledb.CLOB.pipe(String)'
	try {
      const stringWriter = new  StringWriter();
      clob.setEncoding('utf8');  // set the encoding so we get a 'string' not a 'buffer'
	  stack = new Error().stack
  	  await pipeline(clob,stringWriter)
	  operation = 'oracledb.LOB.close(CLOB)'
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
  
  async setDateFormatMask(vendor) {

    const SQL_SET_DATE_FORMAT = `ALTER SESSION SET NLS_DATE_FORMAT = '${this.getDateFormatMask(vendor)}' NLS_TIMESTAMP_FORMAT = '${this.getTimeStampFormatMask(vendor)}' NLS_TIMESTAMP_TZ_FORMAT = '${this.getTimeStampFormatMask(vendor)}'`
	let result = await this.executeSQL(SQL_SET_DATE_FORMAT);
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
	
    result = await this.executeSQL(this.StatementLibrary.SQL_CONFIGURE_CONNECTION,args);
    
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
      return []
    }
  }

  async setCurrentSchema(schema) {

    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 1024} , schema:schema}
    const results = await this.executeSQL(this.StatementLibrary.SQL_SET_CURRENT_SCHEMA,args)
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
    const results = await this.executeSQL(this.StatementLibrary.SQL_DISABLE_CONSTRAINTS,args)
    this.processLog(results,'Disable Constraints')

  }
    
  async enableConstraints() {
	  
	try  {
      // this.checkConnectionState(this.latestError) 
      const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH} , schema:this.parameters.TO_USER} 
      const results = await this.executeSQL(this.StatementLibrary.SQL_ENABLE_CONSTRAINTS,args)
      this.processLog(results,'Enable Constraints')
	} catch (e) {
      this.yadamuLogger.error(['DBA',this.DATABASE_VENDOR,'CONSTRAINTS'],`Unable to re-enable constraints.`);          
      this.yadamuLogger.handleException(['MATERIALIZED VIEWS',this.DATABASE_VENDOR,],e);          
    } 
    
  }
  
  async refreshMaterializedViews() {

    try  {
      // this.checkConnectionState(this.latestError) 
	  const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH} , schema:this.parameters.TO_USER}     
      const results = await this.executeSQL(this.StatementLibrary.SQL_REFRESH_MATERIALIZED_VIEWS,args)
      this.processLog(results,'Materialized View Refresh')
    } catch (e) {
      this.yadamuLogger.error(['DBA',this.DATABASE_VENDOR,'MATERIALIZED VIEWS'],`Unable to refresh materialzied views.`);          
      this.yadamuLogger.handleException(['MATERIALIZED VIEWS',this.DATABASE_VENDOR,],e);          
    } 
	
  } 
  
  async executeMany(sqlStatement,rows,binds,lobCount) {
	   
    let attemptReconnect = (this.ATTEMPT_RECONNECTION)
	
	/*
	**
	** Test for LOB argumnets. Reconnection is not useful with LOB binds since
	** LOBs are invalid after the reconnection 
	**
    ** PLS-00306: wrong number or types of arguments in call to ...
    ** ORA-06550: line 1, column 7:
	**
	*/

    if (rows.length > 0) {
      // this.status.sqlTrace.write())
      this.status.sqlTrace.write(this.traceSQL(sqlStatement,rows.length,lobCount))
	  
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
		    // reconnect() throws cause if it cannot reconnect or if rows are lost (ABORT, SKIP)
            await this.reconnect(cause,'EXECUTE MANY')
            await this.setCurrentSchema(this.parameters.TO_USER)
		    await this.setDateFormatMask(this.systemInformation.vendor);
			continue;
          }		  	  
		  throw this.trackExceptions(cause)
        }      
      } 
	}
  }

  async executeSQL(sqlStatement,args,outputFormat) {
     
    let attemptReconnect = this.ATTEMPT_RECONNECTION;
	
	/*
	**
	** Test for LOB argumnets. Reconnection is not useful with LOB arguments since
	** LOBs are invalid after the reconnection 
	**
    ** PLS-00306: wrong number or types of arguments in call to ...
    ** ORA-06550: line 1, column 7:
	**
	*/

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
		  await this.setDateFormatMask(this.systemInformation ? this.systemInformation.vendor : "oracle");
		  continue;
        }
        throw this.trackExceptions(cause)
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
	    
  prepareDDLStatements(ddlStatements) {
	ddlStatements.unshift(JSON.stringify({jsonColumns:null})); 
	return ddlStatements
  }
		
		
  async _executeDDL(ddl) {

	let results = []
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
      results = await this.applyDDL(ddl,this.systemInformation.schema,this.parameters.TO_USER);
    }
    else {
      // ### OVERRIDE ### - Send Set of DDL operations to the server for execution   
      const sqlStatement = `begin :log := YADAMU_EXPORT_DDL.APPLY_DDL_STATEMENTS(:ddl,:sourceSchema,:targetSchema); end;`;
      const ddlLob = await (this.DB_VERSION < 12 ? this.convertDDL2XML(ddl) : this.jsonToBlob({ddl : ddl}))
     
      const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH} , ddl:ddlLob, sourceSchema:this.systemInformation.schema, targetSchema:this.parameters.TO_USER};
      results = await this.executeSQL(sqlStatement,args);
	  await ddlLob.close();
      results = this.processLog(results,'DDL Execution')
    }
	
    this.yadamuLogger.ddl([`${this.DATABASE_VENDOR}`],`Errors: ${this.logSummary.errors}, Warnings: ${this.logSummary.warnings}, Ingnoreable ${this.logSummary.ignoreable}, Duplicates: ${this.logSummary.duplicates}, Unresolved: ${this.logSummary.reference}, Compilation: ${this.logSummary.recompilation}, Miscellaneous ${this.logSummary.aq}.`)
	return results

  }
  
  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
    
  async initialize() {
    await super.initialize(true);
    switch (true) {
      case this.DB_VERSION < 12:
	    this.StatementLibrary = require('./112/oracleStatementLibrary.js')
		this.StatementGenerator = require('./112/statementGenerator.js')
        break;
      case this.DB_VERSION < 19:
	    this.StatementLibrary = require('./18/oracleStatementLibrary.js')
        break;
      default:
	}
  }
    
  /*
  **
  **  Gracefully close down the database connection.
  **
  */

  async initializeExport() {
    super.initializeExport()
    await this.setCurrentSchema(this.parameters.FROM_USER)
  }

  async finalizeExport() {
	this.checkConnectionState(this.latestError) 
    await this.setCurrentSchema(this.vendorProperties.user);
  }

  async initializeImport() {
	super.initializeImport()
    await this.setCurrentSchema(this.parameters.TO_USER)
  }

  async initializeData() {
    await this.disableConstraints();
    await this.setDateFormatMask(this.systemInformation.vendor);
  }
  
  async finalizeData() {
	// this.yadamuLogger.trace([this.DATABASE_VENDOR],`finalizeData()`);
    await this.refreshMaterializedViews();
    await this.enableConstraints();
  }  

  async finalizeImport() {
    this.checkConnectionState(this.latestError) 
	await this.setCurrentSchema(this.vendorProperties.user);
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

  async abort(e) {
    await super.abort(e,{drainTime:0}); 
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
	  throw this.trackExceptions(new OracleError(e,stack,`Oracledb.Transaction.commit()`))
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
	  const newIssue = this.trackExceptions(new OracleError(e,stack,`Oracledb.Transaction.rollback()`))
	  this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)
	}	
  }
  
  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)

    await this.executeSQL(this.StatementLibrary.SQL_CREATE_SAVE_POINT,[]);
	super.createSavePoint()
  }

  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)

    this.checkConnectionState(cause)
	
	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.

	try {
	  await this.executeSQL(this.StatementLibrary.SQL_RESTORE_SAVE_POINT,[]);
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
         inputStream.on('open',() => {resolve(inputStream)}).on('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(err,stack,importFilePath) : new FileError(err,stack,importFilePath) )})
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
	
	
	const typeMappings = {
	  raw1AsBoolean    : new Boolean(this.TREAT_RAW1_AS_BOOLEAN).toString().toLowerCase()
	, jsonDataType     : this.JSON_DATA_TYPE
	, xmlStorageModel  : this.XML_STORAGE_CLAUSE
	}
	
	const sqlStatement = `begin\n  ${settings}\n  :log := YADAMU_IMPORT.IMPORT_JSON(:json, :schema, :typeMappings);\nend;`;
	const results = await this.executeSQL(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH}, json:hndl, schema:this.parameters.TO_USER, typeMappings: JSON.stringify(typeMappings)})
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
  
  getTypeMappings() {
   
    const typeMappings = super.getTypeMappings();
	typeMappings.objectFormat = this.OBJECT_FORMAT 
    return typeMappings; 
  }

  async getSystemInformation() {     

	const results = await this.executeSQL(this.StatementLibrary.SQL_SYSTEM_INFORMATION,{sysInfo:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH}})

    return Object.assign(
	  super.getSystemInformation()
	, JSON.parse(results.outBinds.sysInfo)
    , {
		oracleDriver       : {
          oracledbVersion  : oracledb.versionString
        , clientVersion    : oracledb.oracleClientVersionString
        , serverVersion    : this.connection.oracleServerVersionString
        }
      }
	);
	  
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
        results = await this.executeSQL(this.StatementLibrary.SQL_GET_DLL_STATEMENTS,bindVars)
        ddl = JSON.parse(results.outBinds.v2);
        break;
      case this.DB_VERSION < 19:
        results = await this.executeSQL(this.StatementLibrary.SQL_GET_DLL_STATEMENTS,{schema: this.parameters.FROM_USER},{outFormat: oracledb.OBJECT,fetchInfo:{JSON:{type: oracledb.STRING}}})
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
        results = await this.executeSQL(this.StatementLibrary.SQL_GET_DLL_STATEMENTS,bindVars)
        ddl = JSON.parse(results.outBinds.v2);
    }
	return ddl;    

  }

  async getSchemaInfo(keyName) {

    const results = await this.executeSQL(this.StatementLibrary.SQL_SCHEMA_INFORMATION
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
	
	this.partitionLists = {}
	
    const schemaInformation = results.rows.flatMap((tableInfo) => {
	  const partitionList = JSON.parse(tableInfo.PARTITION_LIST)
	  delete tableInfo.PARTITION_LIST
  	  if (this.yadamu.PARALLEL_ENABLED && this.PARTITION_LEVEL_OPERATIONS && (partitionList.length > 0)) {
		// Clone the table Info for each Partition and Add Parition Info
		this.partitionLists[tableInfo.TABLE_NAME] = partitionList
	    const partitionInfo = partitionList.map((partitionName,idx) => { return Object.assign({}, tableInfo, { PARTITION_COUNT: partitionList.length, partitionInfo : {PARTITION_NAME : partitionName, PARTITION_NUMBER: idx+1 }})})
		return partitionInfo;
	  }
      return tableInfo;
	})
	return schemaInformation;
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
  
  inputStreamError(cause,sqlStatement) {
	return this.trackExceptions(cause instanceof OracleError ? cause : new OracleError(cause,this.streamingStackTrace,sqlStatement))
  }
  
  async disableTriggers(schema,tableName) {
    const sqlStatement = `ALTER TABLE "${schema}"."${tableName}" DISABLE ALL TRIGGERS`;
    return await this.executeSQL(sqlStatement,[]);    
  }
  
  async enableTriggers(schema,tableName) {
   
	try {
  	  this.checkConnectionState(this.latestError) 
      const sqlStatement = `ALTER TABLE "${schema}"."${tableName}" ENABLE ALL TRIGGERS`;
      return await this.executeSQL(sqlStatement,[]);
	} catch (e) {
	  this.yadamuLogger.error(['DBA',this.DATABASE_VENDOR,'TRIGGERS',tableName],`Unable to re-enable triggers.`);          
      this.yadamuLogger.handleException(['TRIGGERS',this.DATABASE_VENDOR,],e);          
    } 
  }
  
  async getInputStream(tableInfo) {

    if (tableInfo.partitionInfo?.PARTITION_NAME) {
	  tableInfo.SQL_STATEMENT = `${tableInfo.SQL_STATEMENT.slice(0,-1)} PARTITION("${tableInfo.partitionInfo.PARTITION_NAME}") t`
	}
	

    if (tableInfo.WITH_CLAUSE !== null) {
      if (this.DB_VERSION < 12) {
		// The "WITH_CLAUSE" is a create procedure statement that creates a stored procedure that wraps the required conversions
		await this.executeSQL(tableInfo.WITH_CLAUSE,{})
		// The procedure needs to be dropped once the operation is complete.
		const wrapperName = tableInfo.WITH_CLAUSE.substring(tableInfo.WITH_CLAUSE.indexOf('"."')+3,tableInfo.WITH_CLAUSE.indexOf('"('))
		const sqlStatement = this.StatementLibrary.SQL_DROP_WRAPPERS.replace(':1:',this.parameters.FROM_USER).replace(':2:',wrapperName);
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
          await this.reconnect(cause,'INPUT STREAM')
          await this.setCurrentSchema(this.parameters.TO_USER)
		  await this.setDateFormatMask(this.systemInformation.vendor);
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
  
  async generateStatementCache(schema) {
    const statementGenerator = new this.StatementGenerator(this,schema,this.metadata,this.yadamuLogger)
    this.statementCache = await statementGenerator.generateStatementCache(this.systemInformation.vendor)
	return this.statementCache

  }

  getOutputStream(tableName,ddlComplete) {
	 return super.getOutputStream(OracleWriter,tableName,ddlComplete)
  }
 
  classFactory(yadamu) {
	return new OracleDBI(yadamu)
  }
 
  async cloneCurrentSettings(manager) {
    super.cloneCurrentSettings(manager)
	await this.setCurrentSchema(manager.currentSchema);
    await this.setDateFormatMask(this.systemInformation.vendor);
	this.SQL_DIRECTORY_PATH = this.manager.SQL_DIRECTORY_PATH
	this.partitionLists = this.manager.partitionLists
  }
  
  async getConnectionID() {
	const results = await this.executeSQL(`SELECT SID, SERIAL# FROM V$SESSION WHERE AUDSID = Sys_Context('USERENV', 'SESSIONID')`)
	return {sid : results.rows[0][0], serial: results.rows[0][1]}
  }
  
  generateDatabaseMappings(metadata) {
    
    const dbMappings = {}

    if (this.DB_VERSION < 12) { 
	
      // ### TODO: Impliment better algorithm that truncation. Check for clashes. Add Unique ID
	  
      Object.keys(metadata).forEach((table) => {
        const mappedTableName = metadata[table].tableName.length > 30 ? metadata[table].tableName.substring(0,30) : undefined
        if (mappedTableName) {
		  this.yadamuLogger.warning([this.DATABASE_VENDOR,this.DB_VERSION,'IDENTIFIER LENGTH',metadata[table].tableName],`Identifier Too Long (${metadata[table].tableName.length}). Identifier re-mapped as "${mappedTableName}".`)
          dbMappings[table] = {
			tableName : mappedTableName
		  }
        }
        const columnMappings = {}
        metadata[table].columnNames.forEach((columnName) => { 
		  if (columnName.length > 30) {
			const mappedColumnName =  columnName.substring(0,30)
  		    this.yadamuLogger.warning([this.DATABASE_VENDOR,this.DB_VERSION,'IDENTIFIER LENGTH',metadata[table].tableName,columnName],`Identifier Too Long (${columnName.length}). Identifier re-mapped as "${mappedColumnName}".`)
   		    columnMappings[columnName] = {name: mappedColumnName}
		  }
		})
        if (!YadamuLibrary.isEmpty(columnMappings)) {
          dbMappings[table] = dbMappings[table] || {}
          dbMappings[table].columnMappings = columnMappings
        }
      })
    }
    return dbMappings;    
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

    if (!OracleConstants.STAGED_DATA_SOURCES.includes(vendor)) {
       return false;
	}
    	
	return this.reportCopyOperationMode(controlFile.settings.contentType === 'CSV',controlFilePath,controlFile.settings.contentType)
  }
  
  async initializeCopy() {
	 await this.executeSQL(`create or replace directory ${this.SQL_DIRECTORY_NAME} as '${this.SQL_DIRECTORY_PATH}/'`);
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
	  results = await this.executeSQL(copy.ddl);
	  results = await this.executeSQL(copy.dml);
	  const rowsRead = results.rowsAffected
	  results = await this.executeSQL(copy.drop);
	  const endTime = performance.now();
	  results = await this.commitTransaction()
  	  await this.reportCopyResults(tableName,rowsRead,0,startTime,endTime,copy.dml,stack)
	} catch(e) {
      console.log(e)
	  if (e.copyFileNotFoundError()) {
		e.directoryPath = this.SQL_DIRECTORY_PATH
	  }
	  this.yadamuLogger.handleException([this.DATABASE_VENDOR,'COPY',tableName],e)
	  let results = await this.rollbackTransaction()
	}
  }

  async finalizeCopy() {
	 await this.executeSQL(`drop directory ${this.SQL_DIRECTORY_NAME}`);
  }
  
  async copyStagedData(vendor,controlFile,metadata,credentials) {
	this.SQL_DIRECTORY_PATH = path.join(this.REMOTE_STAGING_AREA,path.basename(controlFile.settings.baseFolder),'data').split(path.sep).join(path.posix.sep)
	return super.copyStagedData(vendor,controlFile,metadata,credentials) 
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
          break;
        case 'ddl':
          this.ddl = obj.ddl;
        case 'metadata':
		case 'table':
		default:
		  this.passThrough.unpipe(this.jsonParser);
		  this.jsonParser.destroy()
          break;
      }
      callback();
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



import fs                             from 'fs';
import path                           from 'path';

import { 
  performance 
}                                     from 'perf_hooks';

import {
  Readable, 
  Writable, 
  Transform, 
  PassThrough
}                                     from 'stream'
import { 
  pipeline 
}                                     from 'stream/promises';

/* Database Vendors API */                                    

import oracledb from 'oracledb';
oracledb.fetchAsString = [ oracledb.DATE, oracledb.NUMBER ]

/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'
import StringWriter                   from '../../util/stringWriter.js'
import BufferWriter                   from '../../util/bufferWriter.js'
import HexBinToBinary                 from '../../util/hexBinToBinary.js'


import {
  CopyOperationAborted
}                                     from '../../core/yadamuException.js'

/* Yadamu DBI */                                    
							          							          
import YadamuDBI                      from '../base/yadamuDBI.js'
import DBIConstants                   from '../base/dbiConstants.js'
import JSONParser                     from '../file/jsonParser.js'

import {
  FileError, 
  FileNotFound, 
  DirectoryNotFound
 }                                    from '../file/fileException.js'
 
/* Vendor Specific DBI Implimentation */                                   
					
import OracleConstants                from './oracleConstants.js'
import OracleDataTypes                from './oracleDataTypes.js'
import OracleParser                   from './oracleParser.js'
import OracleOutputManager            from './oracleOutputManager.js'
import OracleWriter                   from './oracleWriter.js'
import OracleStatementLibrary         from './oracleStatementLibrary.js'
import OracleStatementGenerator       from './oracleStatementGenerator.js'

import {
  OracleError, 
  StagingFileError
}                                     from './oracleException.js'

class OracleDBI extends YadamuDBI {

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS()  {
	this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.DBI_PARAMETERS,OracleConstants.DBI_PARAMETERS))
	return this.#_DBI_PARAMETERS
  }

  get DBI_PARAMETERS() {
	return OracleDBI.DBI_PARAMETERS
  }

  // Instance level getters.. invoke as this.METHOD

  // Not available until configureConnection() has been called

  get MAX_STRING_SIZE()            { return this._MAX_STRING_SIZE }
  // Is VARCHAR2(32000) enabled in the target database
  get EXTENDED_STRING()            { return this._EXTENDED_STRING }
  // Optimal JSON Storage model for current database
  get JSON_DB_STORAGE_MODEL()      { return this._JSON_DB_STORAGE_MODEL }
  // Optimal XML Storage model for current database
  get XMLTYPE_DB_STORAGE_CLAUSE()  { return _this.XMLTYPE_DB_STORAGE_CLAUSE }
  // Does the database support a Native JSON data type.
  get NATIVE_JSON_TYPE()           { return this._NATIVE_JSON_TYPE }
  // Does the database support JSON operations at some level 
  get JSON_PARSING_SUPPORTED()     { return this._JSON_PARSING_SUPPORTED }

  // Override YadamuDBI

  get DATABASE_KEY()               { return OracleConstants.DATABASE_KEY};
  get DATABASE_VENDOR()            { return OracleConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()            { return OracleConstants.SOFTWARE_VENDOR};
  get SQL_COPY_OPERATIONS()        { return true }
  get PARTITION_LEVEL_OPERATIONS() { return true }
  get STATEMENT_TERMINATOR()       { return OracleConstants.STATEMENT_TERMINATOR };
  get STATEMENT_SEPERATOR()        { return OracleConstants.STATEMENT_SEPERATOR };

  // Enable configuration via command line parameters

  get MIGRATE_JSON_STORAGE()       { return this.parameters.MIGRATE_JSON_STORAGE        || OracleConstants.MIGRATE_JSON_STORAGE}
  get BYTE_TO_CHAR_RATIO()         { return this.parameters.BYTE_TO_CHAR_RATIO          || OracleConstants.BYTE_TO_CHAR_RATIO };
  get COPY_LOGFILE_DIRNAME()       { return this.parameters.COPY_LOGFILE_DIRNAME        || OracleConstants.COPY_LOGFILE_DIRNAME };
  get COPY_BADFILE_DIRNAME()       { return this.parameters.COPY_BADFILE_DIRNAME        || OracleConstants.COPY_BADFILE_DIRNAME };
  get TEMPLOB_BATCH_LIMIT()        { return this.parameters.TEMPLOB_BATCH_LIMIT         || OracleConstants.TEMPLOB_BATCH_LIMIT}
  get CACHELOB_BATCH_LIMIT()       { return this.parameters.CACHELOB_BATCH_LIMIT        || OracleConstants.CACHELOB_BATCH_LIMIT}
  get LOB_MAX_SIZE()               { return this.parameters.LOB_MAX_SIZE                || OracleConstants.LOB_MAX_SIZE}


  /*
  **
  ** Use this.DATA_TYPES.storageOptions.XML_TYPE to determine how XML content is stored in the database.
  **
  ** Set to CLOB to force XMLTYPE STORE AS CLOB, which provides best chance of preserving XML Fidelity.
  ** Set to BINARY to force XMLTYPE STORE AS BINARY XML, which provides best performance but not guarantee XML Fidelity in all use cases.
  ** Set to XML to allow the driver to pck the storage model based on the Database Version.
  **
  **
  ** OBJECT RELATAIONAL XML is only supported when migrating between Oracle Databases in DDL_AND_DATA mode
  **
  **  XML_STORAGE_OPTION        : Value is derived from Constants, Configuration Files and Command Line Parameters. Default is XML - meaning choose he recommended storage model for this version of the database
  **  XMLTYPE_BB_STORAGE_CLAUSE : The recommended XML Storage Clause for this version of the database.
  **
  **  ### What about the actual data type when dealing with an existing table ???
  **
  */

  get XMLTYPE_STORAGE_CLAUSE() {
    this._XMLTYPE_STORAGE_CLAUSE = this._XMLTYPE_STORAGE_CLAUSE || (() => {
	   switch (this.DATA_TYPES.storageOptions.XML_TYPE) {
		 case 'XML' :
		   switch (this.XMLTYPE_DB_STORAGE_MODEL) {
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
    return this._XMLTYPE_STORAGE_CLAUSE
  }

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
  **  ORACLE_JSON_TYPE       : Value is derived from Constants, Configuration Files and Command Line Parameters. Default is JSON
  **  JSON_DB_STORAGE_MODEL  : The recommended Storage Model for this version of the database.
  **  JSON_DATA_TYPE         : The data type that will be used by the driver.
  **
  **  ### What about the actual data type when dealing with an existing table ???
  **
  */

  get JSON_DATA_TYPE() {
    this._JSON_DATA_TYPE = this._JSON_DATA_TYPE || (() => {
	  switch (true) {
        case this.NATIVE_JSON_TYPE:
          // What ever the user specified, the default is JSON, IS JSON will be specified for CLOB, BLOB or VARCHAR2
          return this.DATA_TYPES.storageOptions.JSON_TYPE;
        case this.JSON_PARSING_SUPPORTED:
		  return this.DATA_TYPES.storageOptions.JSON_TYPE === 'JSON' ? this.JSON_DB_STORAGE_MODEL : this.DATA_TYPES.storageOptions.JSON_TYPE 
        default:
          return this.JSON_DB_STORAGE_MODEL
      }
    })()
    return this._JSON_DATA_TYPE
  }

  get CACHELOB_MAX_SIZE ()         { return this.EXTENDED_STRING ? OracleConstants.VARCHAR_MAX_SIZE_EXTENDED : OracleConstants.VARCHAR_MAX_SIZE_STANDARD}

  get VARCHAR_MAX_SIZE() {
    // Set with anonymous function to enforce 4K limit in Oracle11g
    this._VARCHAR_MAX_SIZE = this._VARCHAR_MAX_SIZE ||(() => { 
	  let varcharMaxSize = this.parameters.VARCHAR_MAX_SIZE || OracleConstants.VARCHAR_MAX_SIZE_EXTENDED; 
	  // ### TODO : CHECK init.ora setting of MAX_STRING_SIZE
	  varcharMaxSize = ((!this.EXTENDED_STRING) && (varcharMaxSize > OracleConstants.VARCHAR_MAX_SIZE_STANDARD)) ? OracleConstants.VARCHAR_MAX_SIZE_STANDARD : varcharMaxSize; 
	  return varcharMaxSize
	})()
    return this._VARCHAR_MAX_SIZE
  }

  get COMMIT_TEMPLOB_LIMIT() {
    this._COMMIT_TEMPLOB_LIMIT = this._COMMIT_TEMPLOB_LIMIT || (() => { return this.TEMPLOB_BATCH_LIMIT * (this.COMMIT_RATIO || 1)})()
    return this._COMMIT_TEMPLOB_LIMIT
  }

  get COMMIT_CACHELOB_LIMIT() {
    this._COMMIT_CACHELOB_LIMIT = this._COMMIT_CACHELOB_LIMIT || (() => { return this.CACHELOB_BATCH_LIMIT * (this.COMMIT_RATIO || 1)})()
    return this._COMMIT_CACHELOB_LIMIT
  }

   get SCHEMA_METADATA_OPTIONS() {
	 return {
		spatialFormat : this.SPATIAL_FORMAT
   	  , booleanType   : this.DATA_TYPES.storageOptions.BOOLEAN_TYPE
	  , objectType    : this.DATA_TYPES.storageOptions.OBJECT_TYPE
	}
  }
  
  get SCHEMA_METADATA_OPTIONS_XML() {
	  
	const options = 
	   `<options>
		 <spatialFormat>${this.SPATIAL_FORMAT}</spatialFormat>
		 <booleanStorageOption>${this.DATA_TYPES.storageOptions.BOOLEAN_TYPE}</booleanStorageOption>
		 <objectStorageOption>${this.DATA_TYPES.storageOptions.OBJECT_TYPE}</objectStorageOption>
	   </options>`
	
	return options;
  }
  
  get SUPPORTED_STAGING_PLATFORMS()   { return DBIConstants.LOADER_STAGING }
  
  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)
	this.DATA_TYPES = OracleDataTypes

    this.DATA_TYPES.storageOptions.XML_TYPE     = this.parameters.ORACLE_XML_STORAGE_OPTION     || this.DBI_PARAMETERS.XML_STORAGE_OPTION     || this.DATA_TYPES.storageOptions.XML_TYPE
	this.DATA_TYPES.storageOptions.JSON_TYPE    = this.parameters.ORACLE_JSON_STORAGE_OPTION    || this.DBI_PARAMETERS.JSON_STORAGE_OPTION    || this.DATA_TYPES.storageOptions.JSON_TYPE
	this.DATA_TYPES.storageOptions.BOOLEAN_TYPE = this.parameters.ORACLE_BOOLEAN_STORAGE_OPTION || this.DBI_PARAMETERS.BOOLEAN_STORAGE_OPTION || this.DATA_TYPES.storageOptions.BOOLEAN_TYPE
	this.DATA_TYPES.storageOptions.OBJECT_TYPE  = this.parameters.ORACLE_OBJECT_STORAGE_OPTION  || this.DBI_PARAMETERS.OBJECT_STORAGE_OPTION  || this.DATA_TYPES.storageOptions.OBJECT_TYPE

	// make oracledb constants available to decendants of OracleDBI
	this.oracledb = oracledb

    this.ddl = [];
	this.dropWrapperStatements = []

    // Oracle always has a transaction in progress, so beginTransaction is a no-op

	this.TRANSACTION_IN_PROGRESS = true;
	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE],'Constructor Complete')
	
  }
  
  initializeManager() {
	super.initializeManager()
	this.StatementGenerator = OracleStatementGenerator
	this.StatementLibrary   = OracleStatementLibrary
	this.statementLibrary   = undefined
  }	 

  parseConnectionString(vendorProperties, connectionString) {

    const user = YadamuLibrary.convertQuotedIdentifer(connectionString.substring(0,connectionString.indexOf('/')))

	let password = connectionString.substring(connectionString.indexOf('/')+1)

    let connectString = '';
    if (password.indexOf('@') > -1) {
	  connectString = password.substring(password.indexOf('@')+1)
	  password = password.substring(password,password.indexOf('@'))
      console.log(`${new Date().toISOString()}[WARNING][${this.constructor.name}]: Suppling a password on the command line interface can be insecure`)
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
    super.setConnectionProperties(connectionProperties)
	try {
      const conn = await oracledb.getConnection(this.vendorProperties)
      await conn.close()
	  super.setParameters(parameters)
	} catch (e) {
      throw e;
	}

  }

  async createConnectionPool() {
	let stack;
    this.logConnectionProperties()
	const sqlStartTime = performance.now()
	this.vendorProperties.poolMax = this.yadamu.PARALLEL ? parseInt(this.yadamu.PARALLEL) + 1 : 3
	try {
      stack = new Error().stack
      // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE],'Creating Pool')
	  this.pool = await oracledb.createPool(this.vendorProperties)
      // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE],'Pool Created')
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
	  throw this.trackExceptions(new OracleError(this.DRIVER_ID,e,stack,'Oracledb.createPool()'))
	}
  }

  async getConnectionFromPool() {

	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`getConnectionFromPool()`)

	//  Do not Configure Connection here.
	
	let stack;
    this.SQL_TRACE.comment(`Gettting Connection From Pool.`)
	try {
      stack = new Error().stack
      const sqlStartTime = performance.now()
      // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE],'Requestng Connection From Pool')
	  const connection = await this.pool.getConnection()
      // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE],'Connection Assigned')
      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
	  return connection
    } catch (e) {
	  throw this.trackExceptions(new OracleError(this.DRIVER_ID,e,stack,'Oracledb.Pool.getConnection()'))
	}

  }

  async getConnection() {
    this.logConnectionProperties()
	const sqlStartTime = performance.now()
	const connection = await oracledb.getConnection(this.vendorProperties)
	this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
    return connection
  }

  async closeConnection(options) {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE,this.getWorkerNumber()],`closeConnection(${(this.connection !== undefined && (typeof this.connection.close === 'function'))})`)

	if (this.connection !== undefined && (typeof this.connection.close === 'function')) {
      let stack;
      try {
        stack = new Error().stack
        await this.connection.close()
        this.connection = undefined;
      } catch (e) {
        this.connection = undefined;
  	    throw this.trackExceptions(new OracleError(this.DRIVER_ID,e,stack,'Oracledb.Connection.close()'))
	  }
	}
  };

  async closePool(options) {

    // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE],`closePool(${(this.pool instanceof oracledb.Pool) },${(this.pool.status === oracledb.POOL_STATUS_OPEN)},${options.drainTime})`)

    if ((this.pool instanceof oracledb.Pool) && (this.pool.status === oracledb.POOL_STATUS_OPEN)) {
      let stack;
      try {
        if (options.drainTime !== undefined) {
          stack = new Error().stack
		  await this.pool.close(options.drainTime)
		}
	    else {
          stack = new Error().stack
		  await this.pool.close()
	    }
        this.pool = undefined
      } catch (e) {
        this.pool = undefined
	    throw this.trackExceptions(new OracleError(this.DRIVER_ID,e,stack,'Oracledb.Pool.close()'))
      }
    }
  }
  
  async createLob(lobType) {

    let stack
    try {
      const sqlStartTime = performance.now()
	  stack = new Error().stack
      const lob =  await this.connection.createLob(lobType)
      // this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
	  return lob;
   	} catch (e) {
	  throw this.trackExceptions(new OracleError(this.DRIVER_ID,e,stack,`Oracledb.Connection.createLob()`))
    }
  }

  async clientClobToString(clob) {
     // ### Ugly workaround due to the fact it does not appear possible to directly re-read a local CLOB
     const sql = `select :tempClob "newClob" from dual`;
     const results = await this.executeSQL(sql,{tempClob:clob})
     return await results.rows[0][0].getData()
  }

  async clientBlobToBuffer(blob) {
     // ### Ugly workaround due to the fact it does not appear possible to directly re-read a local BLOB
     const sql = `select :tempBlob "newBlob" from dual`;
     const results = await this.executeSQL(sql,{tempBlob:blob})
     return await results.rows[0][0].getData()
  }

  async streamToBlob(readable) {

    let stack
	const operation = 'buffer.pipe(oracledb.BLOB)'
    try {
      const blob = await this.createLob(oracledb.BLOB)
      stack = new Error().stack
  	  await pipeline(readable,blob)
      // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE,'WRITE TO BLOB'],`Bytes Written: ${blob.offset-1}.`)
	  return blob
	} catch(e) {
	  throw e instanceof OracleError ? e : new OracleError(this.DRIVER_ID,e,stack,operation)
	}
  };

  async fileToBlob(filename) {
     const stream = await new Promise((resolve,reject) => {
     const inputStream = fs.createReadStream(filename)
       inputStream.on('open',() => {resolve(inputStream)}).on('error',(err) => {reject(err)})
    })
    return this.streamToBlob(stream)
  };

  stringToBlob(string) {
    const stream = new Readable()
    stream.push(string)
    stream.push(null)
    return this.streamToBlob(stream)
  };

  blobFromBuffer(buffer) {
     let stream = new Readable ()
     stream.push(buffer)
     stream.push(null)
     return this.streamToBlob(stream)
  }

  async jsonToBlob(json) {
    return this.stringToBlob(JSON.stringify(json))
  };

  async streamToClob(readable) {

    let stack
	const operation = 'readable.pipe(oracledb.CLOB)'
    try {
      const clob = await this.createLob(oracledb.CLOB)
      stack = new Error().stack
  	  await pipeline(readable,clob)
      // this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE,'WRITE TO CLOB'],`Characters Written: ${clob.offset-1}.`)
	  return clob
	} catch(e) {
	  throw e instanceof OracleError ? e : new OracleError(this.DRIVER_ID,e,stack,operation)
	}
  };

  stringToClob(str) {
    const s = new Readable()
    s.push(str)
    s.push(null)
    return this.streamToClob(s)

  }

  jsonToClob(json) {
    const s = new Readable()
    s.push(JSON.stringify(json))
    s.push(null)
    return this.streamToClob(s)

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
	let result = await this.executeSQL(SQL_SET_DATE_FORMAT)
  }

  async configureConnection() {

    const SQL_SET_TIMESTAMP_FORMAT = `ALTER SESSION SET TIME_ZONE = '+00:00' NLS_DATE_FORMAT = '${this.getDateFormatMask('Oracle')}' NLS_TIMESTAMP_FORMAT = '${this.getTimeStampFormatMask('Oracle')}' NLS_TIMESTAMP_TZ_FORMAT = '${this.getTimeStampFormatMask('Oracle')}' NLS_LENGTH_SEMANTICS = 'CHAR'`

    let result = await this.executeSQL(SQL_SET_TIMESTAMP_FORMAT,{})

    let args = {
		DATABASE_VERSION:                {dir: oracledb.BIND_OUT, type: oracledb.STRING},
		MAX_STRING_SIZE:           {dir: oracledb.BIND_OUT, type: oracledb.NUMBER},
		JSON_DB_STORAGE_MODEL:     {dir: oracledb.BIND_OUT, type: oracledb.STRING},
    	XMLTYPE_DB_STORAGE_CLAUSE: {dir: oracledb.BIND_OUT, type: oracledb.STRING},
		EXTENDED_STRING_SUPPORTED: {dir: oracledb.BIND_OUT, type: oracledb.DB_TYPE_RAW},
		JSON_PARSING_SUPPORTED:    {dir: oracledb.BIND_OUT, type: oracledb.DB_TYPE_RAW},
		NATIVE_JSON_TYPE:          {dir: oracledb.BIND_OUT, type: oracledb.DB_TYPE_RAW}
	}

    result = await this.executeSQL(this.StatementLibrary.SQL_CONFIGURE_CONNECTION,args)

    this._DATABASE_VERSION                = parseFloat(result.outBinds.DATABASE_VERSION)
    this._MAX_STRING_SIZE           = result.outBinds.MAX_STRING_SIZE
    this._JSON_DB_STORAGE_MODEL     = result.outBinds.JSON_DB_STORAGE_MODEL
    this._XMLTYPE_DB_STORAGE_CLAUSE = result.outBinds.XMLTYPE_DB_STORAGE_CLAUSE
    this._EXTENDED_STRING           = YadamuLibrary.toBoolean(result.outBinds.EXTENDED_STRING_SUPPORTED)
	this._JSON_PARSING_SUPPORTED    = YadamuLibrary.toBoolean(result.outBinds.JSON_PARSING_SUPPORTED)
	this._NATIVE_JSON_TYPE          = YadamuLibrary.toBoolean(result.outBinds.NATIVE_JSON_TYPE)

    this.DATA_TYPES.storageOptions.JSON_TYPE = this.JSON_DATA_TYPE
	
	if (this.isManager()) {
      if (this.MAX_STRING_SIZE <= OracleConstants.VARCHAR_MAX_SIZE_EXTENDED) {
        this.yadamuLogger.info([this.DATABASE_VENDOR,this.DATABASE_VERSION,`Configuration`],`Maximum VARCHAR2 size for JSON operations is ${this.MAX_STRING_SIZE}.`)
      }

      if (!this.EXTENDED_STRING) {
        this.yadamuLogger.info([this.DATABASE_VENDOR,this.DATABASE_VERSION,`Configuration`],`VARCHAR MAX_SIZE set to ${this.VARCHAR_MAX_SIZE}.`)
      }

      if (this.XMLTYPE_STORAGE_CLAUSE !== this.XMLTYPE_STORAGE_MODEL ) {
        this.yadamuLogger.info([this.DATABASE_VENDOR,this.DATABASE_VERSION,`Configuration`],`XMLType storage model is ${this.XMLTYPE_STORAGE_CLAUSE}.`)
      }
	  this.yadamuLogger.info([this.DATABASE_VENDOR,this.DATABASE_VERSION,`Configuration`],`JSON storage model is ${this.DATA_TYPES.storageOptions.JSON_TYPE}.`)
    }
  }

  processLog(results,operation) {
    if (results.outBinds.log !== null) {
      const log = JSON.parse(results.outBinds.log.replace(/\\r/g,'\\n'))
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

    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH} , schema:this.CURRENT_SCHEMA}
    const results = await this.executeSQL(this.StatementLibrary.SQL_DISABLE_CONSTRAINTS,args)
    this.processLog(results,'Disable Constraints')

  }

  async enableConstraints() {

	try  {
      // this.checkConnectionState(this.latestError)
      const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH} , schema:this.CURRENT_SCHEMA}
      const results = await this.executeSQL(this.StatementLibrary.SQL_ENABLE_CONSTRAINTS,args)
      this.processLog(results,'Enable Constraints')
	} catch (e) {
      this.yadamuLogger.error(['DBA',this.DATABASE_VENDOR,'CONSTRAINTS'],`Unable to re-enable constraints.`)
      this.yadamuLogger.handleException(['MATERIALIZED VIEWS',this.DATABASE_VENDOR,],e)
    }

  }

  async refreshMaterializedViews() {

    try  {
      // this.checkConnectionState(this.latestError)
	  const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH} , schema:this.CURRENT_SCHEMA}
      const results = await this.executeSQL(this.StatementLibrary.SQL_REFRESH_MATERIALIZED_VIEWS,args)
      this.processLog(results,'Materialized View Refresh')
    } catch (e) {
      this.yadamuLogger.error(['DBA',this.DATABASE_VENDOR,'MATERIALIZED VIEWS'],`Unable to refresh materialzied views.`)
      this.yadamuLogger.handleException(['MATERIALIZED VIEWS',this.DATABASE_VENDOR,],e)
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
      // this.SQL_TRACE.trace))
      this.SQL_TRACE.traceSQL(sqlStatement,rows.length,lobCount)

  	  let stack
	  let results;
      while (true) {
        // Exit with result or exception.
        try {
          const sqlStartTime = performance.now()
          stack = new Error().stack
          results = await this.connection.executeMany(sqlStatement,rows,binds)
	      this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		  return results;
        } catch (e) {
		  const cause = new OracleError(this.DRIVER_ID,e,stack,sqlStatement,binds,{rows : rows.length})
		  if (attemptReconnect && cause.lostConnection()) {
            attemptReconnect = false;
		    // reconnect() throws cause if it cannot reconnect or if rows are lost (ABORT, SKIP)
            await this.reconnect(cause,'EXECUTE MANY')
            await this.setCurrentSchema(this.CURRENT_SCHEMA)
		    await this.setDateFormatMask(this.systemInformation.vendor)
			continue;
          }
		  throw this.trackExceptions(cause)
        }
      }
	}
  }

  dumpBinds(binds) {
	 binds.forEach((bind) => {
       switch (bind.type) {
			 case oracledb.DB_TYPE_BFILE:                    
			   console.log('DB_TYPE_BFILE',bind)
			   break
			 case oracledb.DB_TYPE_BINARY_DOUBLE:            
			   console.log('DB_TYPE_BINARY_DOUBLE',bind)
			   break
			 case oracledb.DB_TYPE_BINARY_FLOAT:             
			   console.log('DB_TYPE_BINARY_FLOAT',bind)
			   break
			 case oracledb.DB_TYPE_BINARY_INTEGER:           
			   console.log('DB_TYPE_BINARY_INTEGER',bind)
			   break
			 case oracledb.DB_TYPE_BLOB:                     
			   console.log('DB_TYPE_BLOB',bind)
			   break
			 case oracledb.DB_TYPE_BOOLEAN:                  
			   console.log('DB_TYPE_BOOLEAN',bind)
			   break
			 case oracledb.DB_TYPE_CHAR:                     
			   console.log('DB_TYPE_CHAR',bind)
			   break
			 case oracledb.DB_TYPE_CLOB:                     
			   console.log('DB_TYPE_CLOB',bind)
			   break
			   case oracledb.DB_TYPE_CURSOR:                   
			   console.log('DB_TYPE_CURSOR',bind)
			   break
			 case oracledb.DB_TYPE_DATE:                     
			   console.log('DB_TYPE_DATE',bind)
			   break
			 case oracledb.DB_TYPE_INTERVAL_DS:              
			   console.log('DB_TYPE_INTERVAL_DS',bind)
			   break
			 case oracledb.DB_TYPE_INTERVAL_YM:              
			   console.log('DB_TYPE_INTERVAL_YM',bind)
			   break
			 case oracledb.DB_TYPE_JSON:                     
			   console.log('DB_TYPE_JSON',bind)
			   break
			 case oracledb.DB_TYPE_LONG:                     
			   console.log('DB_TYPE_LONG',bind)
			   break
			 case oracledb.DB_TYPE_LONG_RAW:                 
			   console.log('DB_TYPE_LONG_RAW',bind)
			   break
			 case oracledb.DB_TYPE_NCHAR:                    
			   console.log('DB_TYPE_NCHAR',bind)
			   break
			 case oracledb.DB_TYPE_NCLOB:                    
			   console.log('DB_TYPE_NCLOB',bind)
			   break
			 case oracledb.DB_TYPE_NUMBER:                   
			   console.log('DB_TYPE_NUMBER',bind)
			   break
			 case oracledb.DB_TYPE_NVARCHAR:                 
			   console.log('DB_TYPE_NVARCHAR',bind)
			   break
			 case oracledb.DB_TYPE_OBJECT:                   
			   console.log('DB_TYPE_OBJECT',bind)
			   break
			 case oracledb.DB_TYPE_RAW:                      
			   console.log('DB_TYPE_RAW',bind)
			   break
			 case oracledb.DB_TYPE_ROWID:                    
			   console.log('DB_TYPE_ROWID',bind)
			   break
			 case oracledb.DB_TYPE_TIMESTAMP:                
			   console.log('DB_TYPE_TIMESTAMP',bind)
			   break
			 case oracledb.DB_TYPE_TIMESTAMP_LTZ:            
			   console.log('DB_TYPE_TIMESTAMP_LTZ',bind)
			   break
			 case oracledb.DB_TYPE_TIMESTAMP_TZ:             
			   console.log('DB_TYPE_TIMESTAMP_TZ',bind)
			   break
			 case oracledb.DB_TYPE_VARCHAR   :                
			   console.log('DB_TYPE_VARCHAR',bind)
	   }
	 })
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
    this.SQL_TRACE.traceSQL(sqlStatement)

    while (true) {
      // Exit with result or exception.
      try {
        const sqlStartTime = performance.now()
        stack = new Error().stack
        results = await this.connection.execute(sqlStatement,args,outputFormat)
        this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		return results;
      } catch (e) {
		const cause = new OracleError(this.DRIVER_ID,e,stack,sqlStatement,args,outputFormat)
        if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'SQL')
          await this.setCurrentSchema(this.CURRENT_SCHEMA)
		  await this.setDateFormatMask(this.systemInformation ? this.systemInformation.vendor : "oracle")
		  continue;
        }
        throw this.trackExceptions(cause)
      }
    }
  }

  async applyDDL(ddl,sourceSchema,targetSchema) {

    let sqlStatement = `declare V_ABORT BOOLEAN;begin V_ABORT := YADAMU_EXPORT_DDL.APPLY_DDL_STATEMENT(:statement,:sourceSchema,:targetSchema) :abort := case when V_ABORT then 1 else 0 end; end;`;
    let args = {abort:{dir: oracledb.BIND_OUT, type: oracledb.NUMBER} , statement:{type: oracledb.CLOB, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH, val:null}, sourceSchema:sourceSchema, targetSchema:this.CURRENT_SCHEMA};

	if ((this.DATABASE_VERSION < 12) && (this.XMLTYPE_STORAGE_CLAUSE === 'CLOB')) {
       // Force XMLType Store as CLOB ???
	   args.statement.value = `ALTER SESSION SET EVENTS = ''1050 trace name context forever,level 0x2000'`;
       const results = await this.executeSQL(sqlStatement,args)
    }

    for (const ddlStatement of ddl) {
      args.statement.val = ddlStatement
      const results = await this.executeSQL(sqlStatement,args)
      if (results.outBinds.abort === 1) {
        break;
      }
    }

    sqlStatement = `begin :log := YADAMU_EXPORT_DDL.GENERATE_LOG(); end;`;
    args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH}};
    const results = await this.executeSQL(sqlStatement,args)
    return this.processLog(results,'DDL Operation')
  }

  async convertDDL2XML(ddlStatements) {
    const ddl = ddlStatements.map((ddlStatement) => { return `<ddl>${ddlStatement.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}</ddl>`}).join('\n')
    return this.stringToBlob(`<ddlStatements>\n${ddl}\n</ddlStatements>`)
  }

  remapJSONColumns(jsonColumns,ddl) {

	 // Migrate JSON columns to Native JSON Datatype

     ddl.forEach((ddlStatement,idx) => {
       jsonColumns.forEach((json) => {
		  if (ddlStatement.indexOf(`ALTER TABLE "${json.owner}"."${json.tableName}" ADD CHECK (`) === 0) {
		    const constraintTokens = ddlStatement.substring(ddlStatement.indexOf('(')+1,ddlStatement.lastIndexOf(')')).split(' ')
			if ((constraintTokens.length > 2) && (constraintTokens[1].toUpperCase() === 'IS') &&  (constraintTokens[2].toUpperCase() === 'JSON')) {
			  ddl[idx] = null;
			}
		  }
		  if (ddlStatement.indexOf(`CREATE TABLE "${json.owner}"."${json.tableName}"`) === 0) {
		    const lines = ddlStatement.split('\n')
			lines.forEach((line,idx) => {
			  // Look for the line that defines the target column.
  		      const columnOffset = line.indexOf(`"${json.columnName}" ${json.dataType}`)
			  if (columnOffset > -1) {
				// Generate a new line.
				lines[idx] = `${line.trim().startsWith('(') ? '  (' : ''}\t"${json.columnName}" ${this.JSON_DATA_TYPE}${line.indexOf('NOT NULL ENABLE') > -1 ? ' NOT NULL ENABLE' : ''}${line.trim().endsWith(',') ? ',' : ''}`
			  }
		    })
            ddl[idx] = lines.join('\n')
		  }
		})
     })

	 // Strip NULL entries
     return ddl.filter((n) => {return n !== null})

  }

  remapObjectColumns(ddl) {

	 // Migrate Object Columns to JSON.

     ddl.forEach((ddlStatement,idx) => {
       if (ddlStatement.indexOf(`CREATE TABLE`) === 0) {
         const lines = ddlStatement.split('\n')
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
         ddl[idx] = lines.join('\n')
	   }
	 })

	 return ddl;
  }

  prepareDDLStatements(ddlStatements) {
	ddlStatements.unshift(JSON.stringify({jsonColumns:null}))
	return ddlStatements
  }


  async _executeDDL(ddl) {

	let results = []
	const jsonColumns = JSON.parse(ddl.shift())

	// Replace \r with \n.. Earlier database versions generate ddl statements with \r characters.

	ddl = ddl.map((ddlStatement) => {
      return ddlStatement.replace(/\r/g,'\n')
	})

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
      results = await this.applyDDL(ddl,this.systemInformation.schema,this.CURRENT_SCHEMA)
    }
    else {
      // ### OVERRIDE ### - Send Set of DDL operations to the server for execution
      const sqlStatement = `begin :log := YADAMU_EXPORT_DDL.APPLY_DDL_STATEMENTS(:ddl,:sourceSchema,:targetSchema); end;`;
      const ddlLob = await (this.DATABASE_VERSION < 12 ? this.convertDDL2XML(ddl) : this.jsonToBlob({ddl : ddl}))

      const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH} , ddl:ddlLob, sourceSchema:this.systemInformation.schema, targetSchema:this.CURRENT_SCHEMA};
      results = await this.executeSQL(sqlStatement,args)
	  await ddlLob.close()
      results = this.processLog(results,'DDL Execution')
    }

    this.yadamuLogger.ddl([this.DATABASE_VENDOR],`Errors: ${this.logSummary.errors}, Warnings: ${this.logSummary.warnings}, Ingnoreable ${this.logSummary.ignoreable}, Duplicates: ${this.logSummary.duplicates}, Unresolved: ${this.logSummary.reference}, Compilation: ${this.logSummary.recompilation}, Miscellaneous ${this.logSummary.aq}.`)
	return results

  }

  /*
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  async setLibraries() {
    switch (true) {
      case this.DATABASE_VERSION < 12:
	    this.StatementLibrary = (await import('./112/oracleStatementLibrary.js')).default
		this.StatementGenerator = (await import('./112/oracleStatementGenerator.js')).default
        break;
      case this.DATABASE_VERSION < 19:
	    this.StatementLibrary = (await import('./18/oracleStatementLibrary.js')).default
        break;
      default:
	}
  }
  
  async initialize() {
    await super.initialize(true)
  }
  
  async initializeExport() {
    super.initializeExport()
    await this.setCurrentSchema(this.CURRENT_SCHEMA)
  }

  async finalizeExport() {
	this.checkConnectionState(this.latestError)
    await this.setCurrentSchema(this.vendorProperties.user)
  }

  async initializeImport() {
	super.initializeImport()
    await this.setCurrentSchema(this.CURRENT_SCHEMA)
  }

  async initializeData() {
    await this.disableConstraints()
    await this.setDateFormatMask(this.systemInformation.vendor)
  }

  async finalizeData() {
	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.ROLE],`finalizeData()`)
    await this.refreshMaterializedViews()
    await this.enableConstraints()
  }

  async finalizeImport() {
    this.checkConnectionState(this.latestError)
	await this.setCurrentSchema(this.vendorProperties.user)
  }

  getCloseOptions(err) {
	const opts = super.getCloseOptions(err)
    if (err) { opts.drainTime  = 0 }	  
	return opts
  }

  async final() {
	// Oracle11g: Drop any wrappers that were created
	if (this.DATABASE_VERSION < 12) {
	  await Promise.all(this.dropWrapperStatements.map((sqlStatement) => {
	    return this.executeSQL(sqlStatement,{})
	  }))
	}
    await super.final()
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

    this.SQL_TRACE.traceSQL(`commit transaction`)

	let stack
    const sqlStartTime = performance.now()
	try {
	  super.commitTransaction()
      stack = new Error().stack
      await this.connection.commit()
  	  this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
	} catch (e) {
	  throw this.trackExceptions(new OracleError(this.DRIVER_ID,e,stack,`Oracledb.Transaction.commit()`))
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

    this.SQL_TRACE.traceSQL(`rollback transaction`)

	let stack
    const sqlStartTime = performance.now()
	try {
	  super.rollbackTransaction()
      stack = new Error().stack
      await this.connection.rollback()
  	  this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
	} catch (e) {
	  const newIssue = this.trackExceptions(new OracleError(this.DRIVER_ID,e,stack,`Oracledb.Transaction.rollback()`))
	  this.checkCause('ROLLBACK TRANSACTION',cause,newIssue)
	}
  }

  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)

    await this.executeSQL(this.StatementLibrary.SQL_CREATE_SAVE_POINT,[])
	super.createSavePoint()
  }

  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)

    this.checkConnectionState(cause)

	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.

	try {
	  await this.executeSQL(this.StatementLibrary.SQL_RESTORE_SAVE_POINT,[])
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
       const json = await this.fileToBlob(importFilePath)
       return json;
     }
     else {

       // Need to capture the SystemInformation and DDL objects of the export file to make sure the DDL can be processed on the RDBMS.
       // If any DDL statement exceeds MAX_STRING_SIZE then DDL will have to executed statement by statement from the client
       // 'Tee' the input stream used to create the temporary lob that contains the export file and pass it through the JSON Parser.
       // If any of the DDL operations exceed the maximum string size supported by server side JSON operations cache the ddl statements on the client


       const inputStream = await new Promise((resolve,reject) => {
         const inputStream = fs.createReadStream(importFilePath)
         inputStream.on('open',() => {resolve(inputStream)}).on('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(this.DRIVER_ID,err,stack,importFilePath) : new FileError(this.DRIVER_ID,err,stack,importFilePath) )})
       })

	   const multiplexor = new PassThrough()
       const jsonParser = new JSONParser(this.yadamuLogger,'DDL_ONLY',importFilePath)
	   const ddlCache = new DDLCache(this.yadamuLogger,multiplexor,jsonParser)
	   multiplexor.pipe(jsonParser).pipe(ddlCache)

       const blob = await this.createLob(oracledb.BLOB)
       await pipeline(inputStream,multiplexor,blob)

       const ddl = ddlCache.getDDL()
       if ((ddl.length > 0) && this.statementTooLarge(ddl)) {
         this.ddl = ddl
         this.systemInformation = ddlCache.getSystemInformation()
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
           await this.applyDDL(this.ddl)
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
          await his.applyDDL(this.ddl)
           settings = `YADAMU_IMPORT.DDL_ONLY_MODE(TRUE);\n  YADAMU_IMPORT.DATA_ONLY_MODE(TRUE);`;
         }
         else {
           settings = `YADAMU_IMPORT.DDL_ONLY_MODE(TRUE);\n  YADAMU_IMPORT.DATA_ONLY_MODE(FALSE);`;
         }
	     break;
    }

    const statementGenerator = new OracleStatementGenerator(this, this.systemInformation.vendor, this.CURRENT_SCHEMA, {}, this.yadamuLogger);
    const typeMappings = statementGenerator.getVendorTypeMappings()
	
	const options = {
	  booleanStorgeOption  : this.this.DATA_TYPES.storageOptions.BOOLEAN_TYPE
	, jsonDataType         : this.JSON_DATA_TYPE
	, xmlStorageModel      : this.XMLTYPE_STORAGE_CLAUSE
	}

	const sqlStatement = `begin\n  ${settings}\n  :log := YADAMU_IMPORT.IMPORT_JSON(:json, :schema, :options);\nend;`;
	const results = await this.executeSQL(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH}, json:hndl, typeMappings: typeMapping, schema:this.CURRENT_SCHEMA, options: JSON.stringify(options)})
	await this.typeMappings.close();
    return this.processLog(results,'JSON_TABLE')
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

    const typeMappings = super.getTypeMappings()
	typeMappings.objectFormat = this.DATA_TYPES.OBJECT_TYPE
    return typeMappings;
  }

  async getSystemInformation() {

	const results = await this.executeSQL(this.StatementLibrary.SQL_SYSTEM_INFORMATION,{sysInfo:{dir: oracledb.BIND_OUT, type: oracledb.DB_TYPE_VARCHAR, maxSize: this.VARCHAR_MAX_SIZE}})

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
	)

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
      case this.DATABASE_VERSION < 12.2:
        /*
        **
        ** The pipelined table approach used by YADAMU_EXPORT_DDL appears to fail starting with release 19c.
        ** Using Dynamic SQL appears to work correctly.
        **
        */
        bindVars = {v1 : this.CURRENT_SCHEMA, v2 : {dir : oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH}};
        results = await this.executeSQL(this.StatementLibrary.SQL_GET_DLL_STATEMENTS,bindVars)
        ddl = JSON.parse(results.outBinds.v2)
        break;
      case this.DATABASE_VERSION < 19:
        results = await this.executeSQL(this.StatementLibrary.SQL_GET_DLL_STATEMENTS,{schema: this.CURRENT_SCHEMA},{outFormat: oracledb.OBJECT,fetchInfo:{JSON:{type: oracledb.STRING}}})
        ddl = results.rows.map((row) => {
          return row.JSON;
        })
        break;
      default:
        /*
        **
        ** The pipelined table approach used by YADAMU_EXPORT_DDL appears to fail starting with release 19c.
        ** Using Dynamic SQL appears to work correctly.
        **
        */

        bindVars = {v1 : this.CURRENT_SCHEMA, v2 : {dir : oracledb.BIND_OUT, type: oracledb.STRING, maxSize: OracleConstants.LOB_STRING_MAX_LENGTH}};
        results = await this.executeSQL(this.StatementLibrary.SQL_GET_DLL_STATEMENTS,bindVars)
        ddl = JSON.parse(results.outBinds.v2)
    }
	return ddl;

  }

  async getSchemaMetadata() {
	  
    const options = (this.DATABASE_VERSION < 12) ? this.SCHEMA_METADATA_OPTIONS_XML : JSON.stringify(this.SCHEMA_METADATA_OPTIONS)
	
    const results = await this.executeSQL(this.StatementLibrary.SQL_SCHEMA_INFORMATION
 	                                     ,{P_OWNER_LIST: this.CURRENT_SCHEMA, P_OPTIONS: options}
										 ,{outFormat:
										    oracledb.OBJECT
										   ,fetchInfo: {
                                              TABLE_SCHEMA:          {type: oracledb.STRING}
                                             ,TABLE_NAME:            {type: oracledb.STRING}
                                             ,COLUMN_NAME_ARRAY:     {type: oracledb.STRING}
                                             ,DATA_TYPE_ARRAY:       {type: oracledb.STRING}
                                             ,SIZE_CONSTRAINT_ARRAY: {type: oracledb.STRING}
                                             ,CLIENT_SELECT_LIST:    {type: oracledb.STRING}
                                             ,EXPORT_SELECT_LIST:    {type: oracledb.STRING}
                                             ,WITH_CLAUSE:           {type: oracledb.STRING}
											 ,PARTITION_LIST:        {type: oracledb.STRING}
	                                        }
                                          }
    )
                                
    const schemaInformation = results.rows
	schemaInformation.forEach((tableInfo) => {
      const partitionList = JSON.parse(tableInfo.PARTITION_LIST)
	  if (partitionList.length > 0) {
        tableInfo.PARTITION_COUNT = partitionList.length 
		tableInfo.PARTITION_LIST = partitionList 
	  }
    })
    return schemaInformation
  }
  
  expandTaskList(schemaInformation) {
	return schemaInformation.flatMap((tableInformation) => {
	  if (tableInformation.hasOwnProperty('PARTITION_COUNT')) {
		const partitionList = tableInformation.PARTITION_LIST
  	    delete tableInformation.PARTITION_LIST
	    return partitionList.map((partitionName,idx) => {
		  const partitionInfo = Object.assign({},tableInformation)
		  partitionInfo.PARTITION_NUMBER = idx
		  partitionInfo.PARTITION_NAME = partitionName
		  return partitionInfo
	    })
	  }
      else {
	    return tableInformation
	  } 
    })
  }

  getPartitionMetadata(metadata) {
	const partitionMetadata = {}
    metadata.forEach((tableMetadata) => {
	  if (tableMetadata.PARTITION_COUNT > 0) {
        partitionMetadata[tableMetadata.TABLE_NAME] = tableMetadata.PARTITION_LIST
	  }
	})
	return partitionMetadata
  }
		
  createParser(queryInfo,parseDelay) {
	const parser = new OracleParser(this,queryInfo,this.yadamuLogger,parseDelay)
    this.inputStream.on('metadata',(resultSetMetadata) => {parser.setColumnMetadata(resultSetMetadata)})
	return parser;
  }


  async disableTriggers(schema,tableName) {
    // Parallel Trigger operations seem to generate Deadlocks. - Route the ALTER TABLE through the manager connection
    const sqlStatement = `ALTER TABLE "${schema}"."${tableName}" DISABLE ALL TRIGGERS`;
    return this.isManager() ? await this.executeSQL(sqlStatement,[]) : await this.manager.executeSQL(sqlStatement,[])
	return results
  }

  async enableTriggers(schema,tableName) {
    // Parallel Trigger operations seem to generate Deadlocks. - Route the ALTER TABLE through the manager connection
	try {
  	  this.checkConnectionState(this.latestError)
      const sqlStatement = `ALTER TABLE "${schema}"."${tableName}" ENABLE ALL TRIGGERS`;
	  return this.isManager() ? await this.executeSQL(sqlStatement,[]) : await this.manager.executeSQL(sqlStatement,[])
	} catch (e) {
	  this.yadamuLogger.error(['DBA',this.DATABASE_VENDOR,'TRIGGERS',tableName],`Unable to re-enable triggers.`)
      this.yadamuLogger.handleException(['TRIGGERS',this.DATABASE_VENDOR,],e)
    }
  }
  
  generateSQLQuery(tableMetadata) {

    // Generate a conventional relational select statement for this table
    const queryInfo = super.generateSQLQuery(tableMetadata)

    if (queryInfo.WITH_CLAUSE) {
      if (this.DATABASE_VERSION < 12) {
  	    // The "WITH_CLAUSE" is a create procedure statement that creates a stored procedure that wraps the required conversions
	    // The procedure needs to be dropped once the copy operation is complete.
	    // Procedures are created on on the fly but cleaned up once all copy operations are complete. 
	    // This ensures that the procedure can safely be used with mulitple partitions
	    const wrapperName = queryInfo.WITH_CLAUSE.substring(queryInfo.WITH_CLAUSE.indexOf('"."')+3,queryInfo.WITH_CLAUSE.indexOf('"('))
		const sqlStatement = this.StatementLibrary.SQL_DROP_WRAPPERS.replace(':1:',this.CURRENT_SCHEMA).replace(':2:',wrapperName)
	    this.dropWrapperStatements.push(sqlStatement)
	  }
	  else {
	   queryInfo.SQL_STATEMENT = `with\n${queryInfo.WITH_CLAUSE}\n${queryInfo.SQL_STATEMENT}`;
      }
	}

    // If the queryInfo has a PARTITION_NUMBER property assume this is a partition level operation, rather than a table level operation
	
	if (queryInfo.hasOwnProperty('PARTITION_NUMBER')) {
      queryInfo.SQL_STATEMENT = `${queryInfo.SQL_STATEMENT.slice(0,-1)} PARTITION("${queryInfo.PARTITION_NAME}") t`
    }
	
    queryInfo.jsonColumns = [];
    queryInfo.DATA_TYPE_ARRAY.forEach((dataType,idx) => {
      switch (dataType) {
        case 'JSON':
          queryInfo.jsonColumns.push(idx)
          break
        case "GEOMETRY":
        case "\"MDSYS\".\"SDO_GEOMETRY\"":
        case "XMLTYPE":
        case "ANYDATA":
		  break;
        case "BFILE":
		  if (this.OBJECTS_AS_JSON === true) {
            queryInfo.jsonColumns.push(idx)
	      }
		  break;
        default:
		  if ((this.OBJECTS_AS_JSON === true) && (dataType.indexOf('.') > -1)){
            queryInfo.jsonColumns.push(idx)
	      }
      }
    })

    return queryInfo
  }

  inputStreamError(cause,sqlStatement) {
	return this.trackExceptions(((cause instanceof OracleError) || (cause instanceof CopyOperationAborted)) ? cause : new OracleError(this.DRIVER_ID,cause,this.streamingStackTrace,sqlStatement))
  }

  async getInputStream(queryInfo) {
	
    if ((queryInfo.WITH_CLAUSE) && (this.DATABASE_VERSION < 12) && ((!queryInfo.hasOwnProperty('PARTITION_COUNT')) || (queryInfo.PARTITION_NUMBER === 0))) {
  	  // The "WITH_CLAUSE" is a create procedure statement that creates a stored procedure that wraps the required conversions
	  await this.executeSQL(queryInfo.WITH_CLAUSE,{})
	}

    let attemptReconnect = this.ATTEMPT_RECONNECTION;
    this.SQL_TRACE.traceSQL(queryInfo.SQL_STATEMENT)

    while (true) {
      // Exit with result or exception.
      try {
        const sqlStartTime = performance.now()
		this.streamingStackTrace = new Error().stack
        this.inputStream = await this.connection.queryStream(queryInfo.SQL_STATEMENT,[],{extendedMetaData: true})
	    this.SQL_TRACE.traceTiming(sqlStartTime,performance.now())
		this.inputStream.on('end',() => {
		})
	    return this.inputStream
	  } catch (e) {
		const cause = new OracleError(this.DRIVER_ID,e,this.streamingStackTrace ,queryInfo.SQL_STATEMENT,{},{})
        if (attemptReconnect && cause.lostConnection()) {
          attemptReconnect = false;
		  // reconnect() throws cause if it cannot reconnect...
          await this.reconnect(cause,'INPUT STREAM')
          await this.setCurrentSchema(this.CURRENT_SCHEMA)
		  await this.setDateFormatMask(this.systemInformation.vendor)
		  continue;
        }
        throw cause
      }
    }
  }

  async getDataRecoveryInputStream(queryInfo) {
	 queryInfo.SQL_STATEMENT = `select ROWID from "${queryInfo.TABLE_SCHEMA}"."${queryInfo.TABLE_NAME}" t`
	 return this.getInputStream(queryInfo)
  }

/*
  **
  ** The following methods are used by the YADAMU DBWriter class
  **
  */

  async generateStatementCache(schema) {
    const statementGenerator = new this.StatementGenerator(this,this.systemInformation.vendor,schema,this.metadata,this.yadamuLogger)
    this.statementCache = await statementGenerator.generateStatementCache()
	this.emit(YadamuConstants.CACHE_LOADED)
	return this.statementCache
  }

  getOutputManager(tableName,metrics) {
	 return super.getOutputManager(OracleOutputManager,tableName,metrics)
  }

  getOutputStream(tableName,metrics) {
	 return super.getOutputStream(OracleWriter,tableName,metrics)
  }

  classFactory(yadamu) {
	return new OracleDBI(yadamu,this,this.connectionParameters,this.parameters)
  }
    
  async initializeWorker(manager) {
	await super.initializeWorker(manager)
	await this.setCurrentSchema(this.CURRENT_SCHEMA)
	await this.setDateFormatMask(this.systemInformation.vendor)
  }

  cloneSettings() {
	
    super.cloneSettings()
	
	this.SQL_DIRECTORY_PATH = this.manager.SQL_DIRECTORY_PATH
	this.LOCAL_DIRECTORY_PATH = this.manager.LOCAL_DIRECTORY_PATH
	this.partitionLists = this.manager.partitionLists
	this.dropWrapperStatements = this.manager.dropWrapperStatements
  }

  async getConnectionID() {
	const results = await this.executeSQL(`SELECT SID, SERIAL# FROM V$SESSION WHERE AUDSID = Sys_Context('USERENV', 'SESSIONID')`)
	return {sid : results.rows[0][0], serial: results.rows[0][1]}
  }

  generateDatabaseMappings(metadata) {

    const dbMappings = {}

    if (this.DATABASE_VERSION < 12) {

      // ### TODO: Impliment better algorithm that truncation. Check for clashes. Add Unique ID

      Object.keys(metadata).forEach((table) => {
        const mappedTableName = metadata[table].tableName.length > 30 ? metadata[table].tableName.substring(0,30) : undefined
        if (mappedTableName) {
		  this.yadamuLogger.warning([this.DATABASE_VENDOR,this.ROLE,this.DATABASE_VERSION,'IDENTIFIER LENGTH',metadata[table].tableName],`Identifier Too Long (${metadata[table].tableName.length}). Identifier re-mapped as "${mappedTableName}".`)
          dbMappings[table] = {
			tableName : mappedTableName
		  }
        }
        const columnMappings = {}
        metadata[table].columnNames.forEach((columnName) => {
		  if (columnName.length > 30) {
			const mappedColumnName =  columnName.substring(0,30)
  		    this.yadamuLogger.warning([this.DATABASE_VENDOR,this.ROLE,this.DATABASE_VERSION,'IDENTIFIER LENGTH',metadata[table].tableName,columnName],`Identifier Too Long (${columnName.length}). Identifier re-mapped as "${mappedColumnName}".`)
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
  
  async serializeLob(lob) {
	switch (lob.type) {
      case oracledb.CLOB:
        // ### Cannot directly re-read content that has been written to local clob
        return await  this.clientClobToString(lob)
      case oracledb.BLOB:
        // ### Cannot directly re-read content that has been written to local blob
        return await this.clientBlobToBuffer(lob)
      default:
        return lob
    } 
  }   

  async serializeLobBinds(binds) {
    return await Promise.all(binds.map(async (bind,idx) => {
      if (bind.val instanceof oracledb.Lob) {
	    bind.val = this.stringToJSON(await this.serializeLob(bind.val))
      }
      return bind
    }))
  }
   
 async serializeLobColumns(tableInfo,row) {
	// Convert Lobs back to Strings or Buffers
	// row = await Promise.all(row)
    const newRow = await Promise.all(row.map((col,idx) => {
      if (col instanceof oracledb.Lob) {
	    return this.serializeLob(col)
      }
      return col
    }))
	// Put the Lobs back into the original order
	const columnOrderedRow = []
	tableInfo.bindOrdering.forEach((lidx,idx) => {
	  columnOrderedRow[lidx] = newRow[idx]
	})
	return columnOrderedRow 
  }   
       	  

  async initializeCopy(controlFile) {
	 this.SQL_DIRECTORY_PATH = path.join(this.REMOTE_STAGING_AREA,path.basename(controlFile.settings.baseFolder),'data').split(path.sep).join(path.posix.sep)
	 this.LOCAL_DIRECTORY_PATH = path.join(controlFile.settings.baseFolder,path.basename(this.SQL_DIRECTORY_PATH ))
	 await this.executeSQL(`create or replace directory ${this.SQL_DIRECTORY_NAME} as '${this.SQL_DIRECTORY_PATH}/'`)
	 await this.initializeData()
  }

  async copyOperation(tableName,copyOperation,metrics) {

    /*
    **
    ** Generic Basic Imementation - Override as required for error reporting etc
    **
    */
   
	metrics.sql = copyOperation.dml

    if (copyOperation.dml.startsWith('insert /*+ WITH_PLSQL */') && (this.DATABASE_VERSION < 12)) {
		/*
		// The "WITH_CLAUSE" is a create procedure statement that creates a stored procedure that wraps the required conversions
		await this.executeSQL(tableInfo.WITH_CLAUSE,{})
		// The procedure needs to be dropped once the operation is complete.
		const wrapperName = tableInfo.WITH_CLAUSE.substring(tableInfo.WITH_CLAUSE.indexOf('"."')+3,tableInfo.WITH_CLAUSE.indexOf('"('))
		const sqlStatement = this.StatementLibrary.SQL_DROP_WRAPPERS.replace(':1:',this.CURRENT_SCHEMA).replace(':2:',wrapperName)
		this.dropWrapperStatements.push(sqlStatement)
		*/
    }

	try {
	  metrics.writerStartTime = performance.now()
	  await this.disableTriggers(this.CURRENT_SCHEMA,tableName)
	  let results = await this.beginTransaction()
	  results = await this.executeSQL(copyOperation.ddl)
	  if (copyOperation.hasOwnProperty("createFunctions")) {
		const results = await Promise.all(copyOperation.createFunctions.map((func) => {
		  return this.executeSQL(func)
		}))
	  }
	  results = await this.executeSQL(copyOperation.dml)
	  metrics.read = results.rowsAffected
	  metrics.written = results.rowsAffected
	  results = await this.commitTransaction()
	  metrics.committed = metrics.written 
	  metrics.written = 0
	  metrics.writerEndTime = performance.now()
	  results = await this.executeSQL(copyOperation.drop)
	  if (copyOperation.hasOwnProperty("dropFunctions")) {
		const results = await Promise.all(copyOperation.dropFunctions.map((func) => {
		  return this.executeSQL(func)
		}))
	  }
	  await this.enableTriggers(this.CURRENT_SCHEMA,tableName)
	} catch(e) {
	  metrics.writerError = e
	  try {
        await this.enableTriggers(this.CURRENT_SCHEMA,tableName)
	    if (e.copyFileNotFoundError && e.copyFileNotFoundError()) {
		  e = new StagingFileError(this.DRIVER_ID,this.LOCAL_DIRECTORY_PATH,this.SQL_DIRECTORY_PATH,e)
	    }
	    this.yadamuLogger.handleException([this.DATABASE_VENDOR,this.ROLE,'COPY',tableName],e)
	    let results = await this.rollbackTransaction()
	  } catch (e) {
		e.cause = metrics.writerError
		metrics.writerError = e
	  }
	}
	return metrics
  }

  async finalizeCopy() {
	 await this.executeSQL(`drop directory ${this.SQL_DIRECTORY_NAME}`)
	 await this.finalizeData()
  }

}

class DDLCache extends Transform {

  constructor(yadamuLogger, passThrough, jsonParser) {
    super({objectMode: true })
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
		  this.passThrough.unpipe(this.jsonParser)
		  this.jsonParser.destroy()
          break;
      }
      callback()
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._transform()`],e)
      callback(e)
    }
  }

  getDDL() {
    return this.ddl;
  }

  getSystemInformation() {
    return this.systemInformation
  }

}

export {OracleDBI as default }
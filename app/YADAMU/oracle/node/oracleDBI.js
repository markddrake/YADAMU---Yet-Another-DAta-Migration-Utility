"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;
const Writable = require('stream').Writable;
const Transform = require('stream').Transform;
const { performance } = require('perf_hooks');

/* 
**
** Require Database Vendors API 
**
*/

const oracledb = require('oracledb');
oracledb.fetchAsString = [ oracledb.DATE ]

const YadamuLibrary = require('../../common/yadamuLibrary.js')
const YadamuDBI = require('../../common/yadamuDBI.js');
const StringWriter = require('../../common/stringWriter.js');
const BufferWriter = require('../../common/bufferWriter.js');
const HexBinToBinary = require('../../common/hexBinToBinary.js');
const JSONParser = require('../../file/node/jsonParser.js');
const OracleError = require('./oracleError.js')
const OracleParser = require('./oracleParser.js');
const OracleWriter = require('./oracleWriter.js');
const StatementGenerator = require('./statementGenerator.js');
const StatementGenerator11 = require('./statementGenerator11.js');

const dateFormatMasks = {
        Oracle      : 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
       ,MSSQLSERVER : 'YYYY-MM-DD"T"HH24:MI:SS.###"Z"'
       ,Postgres    : 'YYYY-MM-DD"T"HH24:MI:SS"+00:00"'
       ,MySQL       : 'YYYY-MM-DD"T"HH24:MI:SS.######"Z"'
       ,MariaDB     : 'YYYY-MM-DD"T"HH24:MI:SS.######"Z"'
       ,MongoDB     : 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
       
}

const timestampFormatMasks = {
        Oracle      : 'YYYY-MM-DD"T"HH24:MI:SS.FF9"Z"'
       ,MSSQLSERVER : 'YYYY-MM-DD"T"HH24:MI:SS.FF7"Z"'
       ,Postgres    : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"+00:00"'
       ,MySQL       : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"'
       ,MariaDB     : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"'
       ,MongoDB     : 'YYYY-MM-DD"T"HH24:MI:SS.FF9"Z"'
       ,SNOWFLAKE   : 'YYYY-MM-DD"T"HH24:MI:SS.FF9"+00:00"'
       
}

  
const LOB_STRING_MAX_LENGTH    = 16 * 1024 * 1024;
// const LOB_STRING_MAX_LENGTH    = 64 * 1024;
const BFILE_STRING_MAX_LENGTH  =  2 * 1024;
const STRING_MAX_LENGTH        =  4 * 1024;

const DATA_TYPE_STRING_LENGTH = {
  BLOB          : LOB_STRING_MAX_LENGTH
, CLOB          : LOB_STRING_MAX_LENGTH
, JSON          : LOB_STRING_MAX_LENGTH
, NCLOB         : LOB_STRING_MAX_LENGTH
, OBJECT        : LOB_STRING_MAX_LENGTH
, XMLTYPE       : LOB_STRING_MAX_LENGTH
, ANYDATA       : LOB_STRING_MAX_LENGTH
, BFILE         : BFILE_STRING_MAX_LENGTH
, DATE          : 24
, TIMESTAMP     : 30
, INTERVAL      : 16
}  

const sqlSystemInformation = `begin :sysInfo := YADAMU_EXPORT.GET_SYSTEM_INFORMATION(); end;`;

const sqlFetchDDL = 
`select COLUMN_VALUE JSON 
   from TABLE(YADAMU_EXPORT_DDL.FETCH_DDL_STATEMENTS(:schema))`;

const sqlFetchDDL11g = `declare
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


const sqlFetchDDL19c = `declare
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

const sqlTableInfo = 
`select * 
   from table(YADAMU_EXPORT.GET_DML_STATEMENTS(:schema,:tableName,:spatialFormat,:objectsAsJSON))`;

const sqlDropWrapper = `declare
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

const sqlCreateSavePoint = 
`SAVEPOINT BATCH_INSERT`;

const sqlRestoreSavePoint = 
`ROLLBACK TO BATCH_INSERT`;

  
class OracleDBI extends YadamuDBI {

  get DATABASE_VENDOR()     { return 'Oracle' };
  get SOFTWARE_VENDOR()     { return 'Oracle Corporation' };
  get SPATIAL_FORMAT()      { return this.spatialFormat };
  get DEFAULT_PARAMETERS()  { return this.yadamu.getYadamuDefaults().oracle }
  get STATEMENT_TERMINATOR() { return '/' }

  /*
  **
  ** Local methods 
  **
  */
  
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
	this.connectionProperties.poolMax = this.parameters.PARALLEL ? parseInt(this.parameters.PARALLEL) + 1 : 3
	try {
      stack = new Error().stack
      // this.yadamuLogger.trace([this.DATABASE_VENDOR],'Creating Pool');
	  this.pool = await oracledb.createPool(this.connectionProperties);
      // this.yadamuLogger.trace([this.DATABASE_VENDOR],'Pool Created');
      this.traceTiming(sqlStartTime,performance.now())
    } catch (e) {
	  const err = new OracleError(e,stack,'Oracledb.createPool()')
	  throw err;
	}
  }
  
  async getConnectionFromPool() {
	
	// this.yadamuLogger.trace([this.DATABASE_VENDOR,this.getWorkerNumber()],`getConnectionFromPool()`)
	
	//  Do not Configure Connection here. 
	
	let stack;
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceComment(`Gettting Connection From Pool.`));
    }
	try {
      stack = new Error().stack
      const sqlStartTime = performance.now();
      // this.yadamuLogger.trace([this.DATABASE_VENDOR],'Requestng Connection From Pool');
	  const connection = await this.pool.getConnection();
      // this.yadamuLogger.trace([this.DATABASE_VENDOR],'Connection Assigned');
      this.traceTiming(sqlStartTime,performance.now())
	  return connection
    } catch (e) {
	  const err = new OracleError(e,stack,'Oracledb.Pool.getConnection()')
	  throw err;
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
  	    throw new OracleError(e,stack,'Oracledb.Connection.close()')
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
	    throw new OracleError(e,stack,'Oracledb.Pool.close()')
      }
    }
  }  

  async reconnectImpl() {
    this.connection = this.isManager() ? await this.getConnectionFromPool() : await this.connectionProvider.getConnectionFromPool()
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
	  const err = new OracleError(e,stack,`Oracledb.Connection.createLob()`,{},{})
	  throw err
    }
  }

  stringFromClob(clob) {
     
	const stack = new Error().stack;
    return new Promise(async (resolve,reject) => {
      try {
        const stringWriter = new  StringWriter();
        clob.setEncoding('utf8');  // set the encoding so we get a 'string' not a 'buffer'
        
        clob.on('error',async (err) => {
		  try {
            await clob.close(); 
		  } catch (e) {
  			const cause = new OracleError(e,stack,'Oracledb.lob.close(CLOB)')
			this.yadamuLogger.handleException(cause);
		  }
          reject(err instanceof OracleError ? err : new OracleError(err,stack,'Oracledb.Lob(CLOB).onError()',{},{}));
        });
        
        stringWriter.on('finish',async () => {
		  try {
            await clob.close(); 
		  } catch (e) {
 	        const cause = new OracleError(e,stack,'Oracledb.lob.close(CLOB)')
			this.yadamuLogger.handleException(cause);
		  }
          resolve(stringWriter.toString());
        });
		
        clob.pipe(stringWriter);
      } catch (err) {
        reject(err instanceof OracleError ? err : new OracleError(err,stack,'Oracledb.Lob(CLOB).pipe()',{},{}));
      }
    });
  };

  async stringFromLocalClob(clob) {
     // ### Ugly workaround due to the fact it does not appear possible to directly re-read a local CLOB 
     const sql = `select :tempClob "newClob" from dual`;
     const results = await this.executeSQL(sql,{tempClob:clob});
     return await this.stringFromClob(results.rows[0][0])
  }

  hexBinaryFromBlob(blob) {
  
    const stack = new Error().stack;
    return new Promise(async (resolve,reject) => {
      try {
        const bufferWriter = new  BufferWriter();
          
        blob.on('error',async (err) => {
		  try {
	        await blob.close();
		  } catch (e) {
			const cause = new OracleError(e,stack,'Oracledb.lob.close(BLOB)')
			this.yadamuLogger.handleException(cause);
		  }
          reject(err instanceof OracleError ? err : new OracleError(err,stack,'Oracledb.Lob(BLOB).onError()',{},{}));
        });
          
        bufferWriter.on('finish',async () => {
		  try {
            await blob.close();
		  } catch (e) {
			const cause = new OracleError(e,stack,'Oracledb.lob.close(BLOB)')
			this.yadamuLogger.handleException(cause);
		  }
          resolve(bufferWriter.toHexBinary());
        });
        
        blob.pipe(bufferWriter);
      } catch (err) {
        reject(e instanceof OracleError ? e : new OracleError(e,stack,'Oracledb.Lob(BLOB).pipe()',{},{}))
      }
    });
  };
  
  async hexBinaryFromLocalBlob(blob) {
     // ### Ugly workaround due to the fact it does not appear possible to directly re-read a local BLOB 
     const sql = `select :tempBlob "newBlob" from dual`;
     const results = await this.executeSQL(sql,{tempBlob:blob});
     return await this.hexBinaryFromBlob(results.rows[0][0])
  }

  blobFromStream (stream) {
    
    const stack = new Error().stack
    return new Promise(async (resolve,reject) => {
      try {
        const blob =  await this.createLob(oracledb.BLOB);
        
		blob.on('error', (err) => {
			reject(e instanceof OracleError ? e : new OracleError(e,stack,'Oracledb.Lob(BLOB).pipe()',{},{}));
	    });
		
        blob.on('finish', 
		  () => {resolve(blob);
		});
		
        stream.on('error',
		  (err) =>{reject(err);
		});
		
	    stream.pipe(blob);  // copies the text to the temporary LOB
	  } catch (e) {
	    reject(e instanceof OracleError ? e : new OracleError(e,stack,'Oracledb.Lob(BLOB).pipe()',{},{}))	  
	  }
    });  
  };

  blobFromFile (filename) {
     const stream = fs.createReadStream(filename);
     return this.blobFromStream(stream);
  };
  
  blobFromString(string) {
    const stream = new Readable();
    stream.push(string);
    stream.push(null);
    return this.blobFromStream(stream);
  };

  blobFromBuffer(buffer) {
     let stream = new Readable ();
     stream.push(buffer);
     stream.push(null);
     return this.blobFromStream(stream);
  }

  async blobFromJSON(json) { 
    return this.blobFromString(JSON.stringify(json))
  };
      
  blobFromStringReader(r) {
      
    const hexBinToBinary = new HexBinToBinary()
  
    const stack = new Error().stack;        
    return new Promise(async (resolve,reject) => {
      try {
        const blob = await this.createLob(oracledb.BLOB);
        blob.on('error',(err) =>{reject(new OracleError(err,stack,'Oracledb.Lob(BLOB).onError()',{},{}));});
        blob.on('finish', () => {resolve(blob)});
        r.on('error', (err) =>{reject(err);});
        r.pipe(hexBinToBinary).pipe(blob);  // copies the text to the temporary LOB
      }
      catch (e) {
        reject(e instanceof OracleError ? e : new OracleError(e,stack,'Oracledb.Lob(BLOB).pipe()',{},{}))	  
      }
    });  
  }

  blobFromHexBinary(str) {  
    const r = new Readable({encoding : 'utf8'});
    r.push(str);
    r.push(null);    
    return this.blobFromStringReader(r);    
  }
     
  clobFromStringReader(s) {
      
	const stack = new Error().stack;
    return new Promise(async (resolve,reject) => {
      try {
        const clob = await this.createLob(oracledb.CLOB);
		clob.on('error',(err) =>{reject(new OracleError(err,stack,'Oracledb.Lob(BLOB).onError()',{},{}));});
		clob.on('finish',() => {resolve(clob)});
		s.on('error',(err) =>{reject(err);});
		s.pipe(clob);  // copies the text to the temporary LOB
      }
      catch (e) {
	    reject(e instanceof OracleError ? e : new OracleError(e,stack,'Oracledb.Lob(CLOB).pipe()',{},{}))	  
	  }
    });  
  }

  clobFromString(str) {  
    const s = new Readable();
    s.push(str);
    s.push(null);
    return this.clobFromStringReader(s);
    
  }

  clobFromJSON(json) {  
    const s = new Readable();
    s.push(JSON.stringify(json));
    s.push(null);
    return this.lobFromStringReader(s);
    
  }
  
  getDateFormatMask(vendor) {
    
    return dateFormatMasks[vendor] ? dateFormatMasks[vendor] : dateFormatMasks.Oracle
 
  }
  
  getTimeStampFormatMask(vendor) {
    
    return timestampFormatMasks[vendor] ? timestampFormatMasks[vendor] : timestampFormatMasks.Oracle
 
  }
  
  statementTooLarge(sql) {

    return sql.some((sqlStatement) => {
      return sqlStatement.length > this.maxStringSize
    })      
  }
  
  async setDateFormatMask(conn,status,vendor) {

    let sqlStatement = `ALTER SESSION SET NLS_DATE_FORMAT = '${this.getDateFormatMask(vendor)}' NLS_TIMESTAMP_FORMAT = '${this.getTimeStampFormatMask(vendor)}' NLS_TIMESTAMP_TZ_FORMAT = '${this.getTimeStampFormatMask(vendor)}'`
    if (status.sqlTrace) {
      status.sqlTrace.write(this.traceSQL(sqlStatement));
    }
    let result = await conn.execute(sqlStatement);
  }
   
  async configureConnection() {
	  
    let sqlStatement = `ALTER SESSION SET TIME_ZONE = '+00:00' NLS_DATE_FORMAT = '${this.getDateFormatMask('Oracle')}' NLS_TIMESTAMP_FORMAT = '${this.getTimeStampFormatMask('Oracle')}' NLS_TIMESTAMP_TZ_FORMAT = '${this.getTimeStampFormatMask('Oracle')}' NLS_LENGTH_SEMANTICS = 'CHAR'`
    let result = await this.executeSQL(sqlStatement,{});

    sqlStatement = 
`begin 
   :version := YADAMU_EXPORT.DATABASE_RELEASE(); 
   :size := YADAMU_FEATURE_DETECTION.C_MAX_STRING_SIZE; 
   :jsonStorageModel := YADAMU_IMPORT.C_JSON_STORAGE_MODEL; 
   :xmlStorageModel := YADAMU_IMPORT.C_XML_STORAGE_MODEL; 
   if YADAMU_FEATURE_DETECTION.JSON_PARSING_SUPPORTED then :jsonParser := 'TRUE'; else :jsonParser := 'FALSE'; end if;
   if YADAMU_FEATURE_DETECTION.JSON_DATA_TYPE_SUPPORTED then :nativeDataType := 'TRUE'; else :nativeDataType := 'FALSE'; end if;
 end;`;
 
    let args = {
		version:{dir: oracledb.BIND_OUT, type: oracledb.STRING}, 
		size:{dir: oracledb.BIND_OUT, type: oracledb.NUMBER}, 
		jsonStorageModel:{dir: oracledb.BIND_OUT, type: oracledb.STRING}, 
    	xmlStorageModel:{dir: oracledb.BIND_OUT, type: oracledb.STRING}, 
		jsonParser:{dir: oracledb.BIND_OUT, type: oracledb.STRING}, 
		nativeDataType:{dir: oracledb.BIND_OUT, type: oracledb.STRING}
	}
	result = await this.executeSQL(sqlStatement,args);
  
    this.dbVersion = parseFloat(result.outBinds.version);
    this.maxStringSize = result.outBinds.size;
	this.jsonStorageModel = result.outBinds.jsonStorageModel;
	this.xmlStorageModel = this.parameters.XML_STORAGE_MODEL ? this.parameters.XML_STORAGE_MODEL : result.outBinds.xmlStorageModel;
    this.nativeDataType = result.outBinds.nativeDataType === 'TRUE';
	this.jsonParser = result.outBinds.jsonParser === 'TRUE';
	
    if ((this.isManager()) && (this.maxStringSize < 32768)) {
      this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`${this.dbVersion}`,`Configuration`],`Maximum VARCHAR2 size for JSON operations is ${this.maxStringSize}.`)
    }    
	
	if ((this.isManager()) && (this.dbVersion < 12)) {
	  if (this.parameters.LOB_MIN_SIZE > 4000) {
		this.parameters.LOB_MIN_SIZE=4000
        this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`${this.dbVersion}`,`Configuration`],`Parameter LOB_MIN_SIZE reset to ${this.parameters.LOB_MIN_SIZE}.`)
	  }
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

    const sqlStatement = `begin :log := YADAMU_IMPORT.SET_CURRENT_SCHEMA(:schema); end;`;
    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 1024} , schema:schema}
    const results = await this.executeSQL(sqlStatement,args)
    this.processLog(results,'Set Current Schema')
    this.currentSchema = schema;
  }
  
  /*
  ** 
  ** The following methods are used by the YADAMU DBwriter class
  **
  */

  async disableConstraints() {
  
    const sqlStatement = `begin :log := YADAMU_IMPORT.DISABLE_CONSTRAINTS(:schema); end;`;
    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: LOB_STRING_MAX_LENGTH} , schema:this.parameters.TO_USER}
    const results = await this.executeSQL(sqlStatement,args)
    this.processLog(results,'Disable Constraints')

  }
    
  async enableConstraints() {
	  
    const sqlStatement = `begin :log := YADAMU_IMPORT.ENABLE_CONSTRAINTS(:schema); end;`;
    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: LOB_STRING_MAX_LENGTH} , schema:this.parameters.TO_USER} 
    const results = await this.executeSQL(sqlStatement,args)
    this.processLog(results,'Enable Constraints')
    
  }
  
  async refreshMaterializedViews() {
      
    const sqlStatement = `begin :log := YADAMU_IMPORT.REFRESH_MATERIALIZED_VIEWS(:schema); end;`;
    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: LOB_STRING_MAX_LENGTH} , schema:this.parameters.TO_USER}     
    const results = await this.executeSQL(sqlStatement,args)
    this.processLog(results,'Materialized View Refresh')

  }
    
  async executeMany(sqlStatement,rows,binds) {
 
    let attemptReconnect = this.attemptReconnection;

    if (rows.length > 0) {
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(this.traceComment(`Bulk Operation: ${rows.length} records.`))
		this.status.sqlTrace.write(this.traceSQL(sqlStatement));
      }
	  
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
          throw cause
        }      
      } 
	}
  }

  async executeSQL(sqlStatement,args,outputFormat) {
     
    let attemptReconnect = this.attemptReconnection;

	args = args === undefined ? {} : args
	outputFormat = outputFormat === undefined ? {} : outputFormat
	
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(sqlStatement));
    }    
	
    let stack
	let results
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
		  await this.setDateFormatMask(this.connection,this.status,this.systemInformation.vendor);
		  continue;
        }
        throw cause		  
      }      
    } 
  }  

  /*
  **
  ** Overridden Methods
  **
  */

  constructor(yadamu) {
	  
    super(yadamu,yadamu.getYadamuDefaults().oracle);
	
	// make oracledb constants available to decendants of OracleDBI
	
	this.oracledb = oracledb
	
    this.ddl = [];
    this.systemInformation = undefined;
    this.dbVersion = undefined;
    this.maxStringSize = undefined;
	this.wrapperList = [];
	
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
  
  async applyDDL(ddl,sourceSchema,targetSchema) {
	  
    let sqlStatement = `declare V_ABORT BOOLEAN;begin V_ABORT := YADAMU_EXPORT_DDL.APPLY_DDL_STATEMENT(:statement,:sourceSchema,:targetSchema); :abort := case when V_ABORT then 1 else 0 end; end;`; 
    let args = {abort:{dir: oracledb.BIND_OUT, type: oracledb.NUMBER} , statement:{type: oracledb.CLOB, maxSize: LOB_STRING_MAX_LENGTH, val:null}, sourceSchema:sourceSchema, targetSchema:this.parameters.TO_USER};
	
	if ((this.dbVersion < 12) && (this.parmeters.XML_STORAGE_MODEL === 'CLOB')) {
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
    args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: LOB_STRING_MAX_LENGTH}};
    const results = await this.executeSQL(sqlStatement,args);   
    return this.processLog(results,'DDL Operation');
  }

  async convertDDL2XML(ddlStatements) {
    const ddl = ddlStatements.map((ddlStatement) => { return `<ddl>${ddlStatement.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}</ddl>`}).join('\n')
    return this.blobFromString(`<ddlStatements>\n${ddl}\n</ddlStatements>`);
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
				lines[idx] = `${line.trim().startsWith('(') ? '  (' : ''}\t"${json.columnName}" ${this.jsonDataType}${line.indexOf('NOT NULL ENABLE') > -1 ? ' NOT NULL ENABLE' : ''}${line.trim().endsWith(',') ? ',' : ''}`
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
		       tokens[1] = this.jsonDataType;
               lines[idx] = `${line.substr(0,line.indexOf('\t'))}${tokens.join(' ')}`;
			   if (this.jsonParser && (this.jsonDataType !== 'JSON')) {
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
	  
  
  async executeDDLImpl(ddl) {
	const jsonColumns = JSON.parse(ddl.shift())
	
	// Replace \r with \n.. Earlier database versions generate ddl statements with \r characters.
	
	ddl = ddl.map((ddlStatement) => {
      return ddlStatement.replace(/\r/g,'\n')
	});	
	
    if (jsonColumns.jsonColumns !== null) {
	  if ((this.parameters.MIGRATE_JSON_STORAGE === true))   {
		 //### Do not remap JSON columns during export. Leave it until import to leave open the possiblility roundtripping objects via JSON.
	    ddl = this.remapJSONColumns(jsonColumns.jsonColumns,ddl)
	  } 
	}
	
	if (this.systemInformation.objectFormat === 'JSON') {
	  ddl = this.remapObjectColumns(ddl)
    }
	
    if ((this.maxStringSize < 32768) && (this.statementTooLarge(ddl))) {
      // DDL statements are too large send for server based execution (JSON Extraction will fail)
      await this.applyDDL(ddl,this.systemInformation.schema,this.parameters.TO_USER);
    }
    else {
      // ### OVERRIDE ### - Send Set of DDL operations to the server for execution   
      const sqlStatement = `begin :log := YADAMU_EXPORT_DDL.APPLY_DDL_STATEMENTS(:ddl,:sourceSchema,:targetSchema); end;`;
      const ddlLob = await (this.dbVersion < 12 ? this.convertDDL2XML(ddl) : this.blobFromJSON({ddl : ddl}))
     
      const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: LOB_STRING_MAX_LENGTH} , ddl:ddlLob, sourceSchema:this.systemInformation.schema, targetSchema:this.parameters.TO_USER};
      const results = await this.executeSQL(sqlStatement,args);
      await ddlLob.close();
      const log = this.processLog(results,'DDL Execution')
    }
	
    this.yadamuLogger.ddl([`${this.DATABASE_VENDOR}`],`Errors: ${this.logSummary.errors}, Warnings: ${this.logSummary.warnings}, Ingnoreable ${this.logSummary.ignoreable}, Duplicates: ${this.logSummary.duplicates}, Unresolved: ${this.logSummary.reference}, Compilation: ${this.logSummary.recompilation}, Miscellaneous ${this.logSummary.aq}.`)

  }
  
  jsonDataType() {
	  
	switch (this.parameters.JSON_STORAGE_FORMAT) {
      case 'JSON':
	    if (!this.nativeStorageModel) {
		  return this.jsonStorageModel;
	    }
		break;
	  case 'BLOB':
	  case 'CLOB':
	    if (!this.jsonParser) {
		  return this.jsonStorageModel;
	    }
        break;
	  case 'VARCHAR2':
	    if (!this.jsonParser) {
		  return this.jsonStorageModel;
	    }
		return `VARCHAR2(${this.size})`;
		break
	  default:
	    return this.jsonStogaeModel;
	} 
    return this.JSON_STORAGE_FORMAT
  }
  
  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
    
  async initialize() {
	// this.yadamuLogger.trace([this.DATABASE_VENDOR],`Initialize()`);
    await super.initialize(true);
    this.spatialFormat = this.parameters.SPATIAL_FORMAT ? this.parameters.SPATIAL_FORMAT : super.SPATIAL_FORMAT
	this.jsonDataType = this.jsonDataType();
	if (this.isManager()) {
      this.yadamuLogger.info([`${this.DATABASE_VENDOR}`,`${this.dbVersion}`,`Configuration`],`Default storage model for JSON data is ${this.jsonDataType}.`)
	}
	// this.yadamuLogger.trace([this.DATABASE_VENDOR],'Initialize Complete');
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
	if (this.dbVersion < 12) {
      await this.dropWrappers();
    }      
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
  }
  
  /*
  **
  ** Commit the current transaction
  **
  */
  async commitTransaction() {

    // this.yadamuLogger.trace([`${this.constructor.name}.commitTransaction()`,this.getWorkerNumber()],``)

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(`commit transaction`));
    }    

	let stack
    const sqlStartTime = performance.now();
	try {
      stack = new Error().stack
      await this.connection.commit();
  	  this.traceTiming(sqlStartTime,performance.now())
	  super.commitTransaction()
	} catch (e) {
	  const err = new OracleError(e,stack,`Oracledb.Transaction.commit()`,{},{})
	  throw err;
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

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(`rollback transaction`));
    }    

	let stack
    const sqlStartTime = performance.now();
	try {
      stack = new Error().stack
      await this.connection.rollback();
  	  this.traceTiming(sqlStartTime,performance.now())
	  super.rollbackTransaction()
	} catch (e) {
	  let newIssue = new OracleError(e,stack,`Oracledb.Transaction.rollback()`,{},{})
	  this.checkCause(cause,newIssue)
	}	
  }
  
  async createSavePoint() {

    // this.yadamuLogger.trace([`${this.constructor.name}.createSavePoint()`,this.getWorkerNumber()],``)

    await this.executeSQL(sqlCreateSavePoint,[]);
	super.createSavePoint()
  }

  async restoreSavePoint(cause) {

    // this.yadamuLogger.trace([`${this.constructor.name}.restoreSavePoint()`,this.getWorkerNumber()],``)

    this.checkConnectionState(cause)
	
	// If restoreSavePoint was invoked due to encounterng an error and the restore operation results in a second exception being raised, log the exception raised by the restore operation and throw the original error.
	// Note the underlying error is not thrown unless the restore itself fails. This makes sure that the underlying error is not swallowed if the restore operation fails.

	try {
	  await this.executeSQL(sqlRestoreSavePoint,[]);
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
      
     if (this.maxStringSize > 32767) {
       const json = await this.blobFromFile(importFilePath);
       return json;
     }
     else {
         
       // Need to cature the SystemInformation and DDL objects of the export file to make sure the DDL can be processed on the RDBMS.
       // If any DDL statement exceeds maxStringSize then DDL will have to executed statement by statement from the client
       // 'Tee' the input stream used to create the temporary lob that contains the export file and pass it through the JSON Parser.
       // If any of the DDL operations exceed the maximum string size supported by server side JSON operations cache the ddl statements on the client
       
       const jsonParser  = new JSONParser(this.yadamuLogger)  
       const ddlCache = new DDLCache();
       jsonParser.pipe(ddlCache);
       const inputStream = fs.createReadStream(importFilePath);         
       const multiplexor = new Multiplexor(jsonParser,ddlCache)
       const jsonTempLob = await this.blobFromStream(inputStream.pipe(multiplexor))
       const ddl = ddlCache.getDDL();
       if ((ddl.length > 0) && this.statementTooLarge(ddl)) {
         this.ddl = ddl
         this.systemInformation = ddlCache.getSystemInformation();
       }
       return jsonTempLob
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
    switch (this.parameters.MODE) {
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
	 
	const sqlStatement = `begin\n  ${settings}\n  :log := YADAMU_IMPORT.IMPORT_JSON(:json, :schema, :jsonStorageModel, :xmlStorageModel);\nend;`;
	const results = await this.executeSQL(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: LOB_STRING_MAX_LENGTH}, json:hndl, schema:this.parameters.TO_USER, jsonStorageModel: this.jsonStorageModel, xmlStorageModel: this.xmlStorageModel})
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

	const results = await this.executeSQL(sqlSystemInformation,{sysInfo:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: LOB_STRING_MAX_LENGTH}})

    return Object.assign({
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT 
	 ,objectFormat       : this.parameters.OBJECTS_AS_JSON === true ? 'JSON' : 'NATIVE'
     ,schema             : this.parameters.FROM_USER ? this.parameters.FROM_USER : this.parameters.TO_USER
     ,softwareVendor     : this.SOFTWARE_VENDOR
     ,exportVersion      : this.EXPORT_VERSION
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
      case this.dbVersion < 12.2:
        /*
        **
        ** The pipelined table approach used by YADAMU_EXPORT_DDL appears to fail starting with release 19c. 
        ** Using Dynamic SQL appears to work correctly.
        ** 
        */     
        bindVars = {v1 : this.parameters.FROM_USER, v2 : {dir : oracledb.BIND_OUT, type: oracledb.STRING, maxSize: LOB_STRING_MAX_LENGTH}};
        results = await this.executeSQL(sqlFetchDDL11g,bindVars)
        ddl = JSON.parse(results.outBinds.v2);
        break;
      case this.dbVersion < 19:
        results = await this.executeSQL(sqlFetchDDL,{schema: this.parameters.FROM_USER},{outFormat: oracledb.OBJECT,fetchInfo:{JSON:{type: oracledb.STRING}}})
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
     
        bindVars = {v1 : this.parameters.FROM_USER, v2 : {dir : oracledb.BIND_OUT, type: oracledb.STRING, maxSize: LOB_STRING_MAX_LENGTH}};
        results = await this.executeSQL(sqlFetchDDL19c,bindVars)
        ddl = JSON.parse(results.outBinds.v2);
    }
    return ddl;    

  }

  async getSchemaInfo(schema) {

    const objectsAsJSON = this.parameters.OBJECTS_AS_JSON === true ? 'TRUE' : 'FALSE';

    const results = await this.executeSQL(sqlTableInfo
	                                     ,{schema: this.parameters[schema], tableName: null, spatialFormat: this.spatialFormat, objectsAsJSON : objectsAsJSON}
										 ,{outFormat: 
										    oracledb.OBJECT
										   ,fetchInfo: {
                                              COLUMN_LIST:          {type: oracledb.STRING}
                                             ,DATA_TYPE_LIST:       {type: oracledb.STRING}
                                             ,SIZE_CONSTRAINTS:     {type: oracledb.STRING}
                                             ,EXPORT_SELECT_LIST:   {type: oracledb.STRING}
                                             ,NODE_SELECT_LIST:     {type: oracledb.STRING}
                                             ,WITH_CLAUSE:          {type: oracledb.STRING}
                                             ,SQL_STATEMENT:        {type: oracledb.STRING}
											 ,PARTITION_LIST:       {type: oracledb.STRING}
	                                        }
                                          }
										 );
    return results.rows;
  }

  generateMetadata(tableInfo,server) {    
    const metadata = {}
    for (let table of tableInfo) {
      metadata[table.TABLE_NAME] = {
        owner                    : table.OWNER
       ,tableName                : table.TABLE_NAME
       ,columns                  : table.COLUMN_LIST
       ,dataTypes                : JSON.parse(table.DATA_TYPE_LIST)
       ,sizeConstraints          : JSON.parse(table.SIZE_CONSTRAINTS)
       ,exportSelectList         : (server) ? table.EXPORT_SELECT_LIST : table.NODE_SELECT_LIST 
      }
	  // 11.2 Build a list of PL/SQL Wrapper functions that need to be dropped when the export operation is complete.
	  if ((this.dbVersion < 12)  && (table.WITH_CLAUSE !== null)) {
        this.wrapperList.push(table.WITH_CLAUSE.substring(table.WITH_CLAUSE.indexOf('"."')+3,table.WITH_CLAUSE.indexOf('"(')))
	  }
    }
	return metadata
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
      const columnNames = JSON.parse('[' + metadata[table].columns + ']')
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
        metadata[table].columns = '"' + columnNames.join('","')  + '"'
        if (tableMappings[table]) {
          tableMappings[table].columns = columnMappings;
        }
        else {
          tableMappings[table] = {tableName : tableName, columns : columnMappings}
        }
      }
    })        
	
	return mappingRequired ? tableMappings : undefined

  }    

  validateIdentifiers(metadata) {     
     return this.dbVersion < 12 ? this.mapLongIdentifers(metadata) : undefined
  }
  
  generateSelectStatement(tableMetadata) {
     
    // Generate a conventional relational select statement for this table
    
    const tableInfo = Object.assign({},tableMetadata,{
      fetchInfo   : {}
     ,jsonColumns : []
     ,rawColumns  : []
    });   
    
    let selectList = '';
    const columnList = JSON.parse('[' + tableMetadata.COLUMN_LIST + ']');
    
    const dataTypeList = JSON.parse(tableMetadata.DATA_TYPE_LIST);
    dataTypeList.forEach((dataType,idx) => {
      switch (dataType) {
        case 'JSON':
          tableInfo.jsonColumns.push(idx);
          break
        case 'RAW': 
          tableInfo.rawColumns.push(idx);
          break;
        case "GEOMETRY":
        case "\"MDSYS\".\"SDO_GEOMETRY\"":
        case "XMLTYPE":
        case "ANYDATA":
		  break;
        case "BFILE":
		  if (this.parameters.OBJECTS_AS_JSON === true) { 
            tableInfo.jsonColumns.push(idx);
	      }
		  break;
        default:
		  if ((this.parameters.OBJECTS_AS_JSON === true) && (dataType.indexOf('.') > -1)){ 
            tableInfo.jsonColumns.push(idx);
	      }
      }
    })
    
	tableInfo.SQL_STATEMENT = `select ${tableMetadata.NODE_SELECT_LIST} from "${tableMetadata.OWNER}"."${tableMetadata.TABLE_NAME}" t`; 
    tableMetadata.SQL_STATEMENT = tableInfo.SQL_STATEMENT
    return tableInfo
  }
  
  createParser(tableInfo,objectMode) {
    return new OracleParser(tableInfo,objectMode,this.yadamuLogger); 
  }  
  
  streamingError(e,sqlStatement) {
	return new OracleError(e,this.streamingStackTrace,sqlStatement,{},{})
  }
  
  async getInputStream(tableInfo,parser) {

    if (tableInfo.WITH_CLAUSE !== null) {
      if (this.dbVersion < 12) {
        await this.executeSQL(tableInfo.WITH_CLAUSE,{})
      }
      else {
        tableInfo.SQL_STATEMENT = `with\n${tableInfo.WITH_CLAUSE}\n${tableInfo.SQL_STATEMENT}`;
      }
	}

    let attemptReconnect = this.attemptReconnection;

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(this.traceSQL(tableInfo.SQL_STATEMENT))
    }

    while (true) {
      // Exit with result or exception.  
      try {
        const sqlStartTime = performance.now();
        this.streamingStackTrace = new Error().stack
        const is = await this.connection.queryStream(tableInfo.SQL_STATEMENT,[],{extendedMetaData: true})
        this.traceTiming(sqlStartTime,performance.now())
	    is.on('metadata',(metadata) => {parser.setColumnMetadata(metadata)})
	    return is;
	  } catch (e) {
		const cause = OracleError(e,this.streamingStackTrace ,tableInfo.SQL_STATEMENT,{},{})
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
    if (this.dbVersion < 12) {
      statementGenerator = new StatementGenerator11(this,schema,this.metadata,this.systemInformation.spatialFormat,this.batchSize,this.commitSize)
    }
    else {
      statementGenerator = new StatementGenerator(this,schema,this.metadata,this.systemInformation.spatialFormat,this.batchSize,this.commitSize)
    }
    this.statementCache = await statementGenerator.generateStatementCache(executeDDL,this.systemInformation.vendor)
  }

  getOutputStream(tableName) {
	 return super.getOutputStream(OracleWriter,tableName)
  }
    
  async dropWrappers() {

    for (const functionName of this.wrapperList) {
      const sqlStatment = sqlDropWrapper.replace(':1:',this.parameters.FROM_USER).replace(':2:',functionName);
      await this.executeSQL(sqlStatment,{})
    }

  }    
    	  
  async workerDBI(workerNumber) {
	const dbi = new OracleDBI(this.yadamu)
    await super.workerDBI(workerNumber,dbi)
    await dbi.setCurrentSchema(this.currentSchema);
    await dbi.setDateFormatMask(dbi.connection,this.status,this.systemInformation.vendor);
    return dbi;
  }

  async getConnectionID() {
	const results = await this.executeSQL(`SELECT SID, SERIAL# FROM V$SESSION WHERE AUDSID = Sys_Context('USERENV', 'SESSIONID')`)
	return {sid : results.rows[0][0], serial: results.rows[0][1]}
  }
}

class DDLCache extends Writable {
  
  constructor() {
    super({objectMode: true });
    this.systemInformation = undefined;
    this.ddl = undefined 
  }

  async _write(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
        case 'systemInformation':
          this.systemInformation = obj.systemInformation
          break;
        case 'ddl':
          this.ddl = obj.ddl;
          break;
        case 'metadata':
          this.ddl = []
          break;
      }
      callback();
    } catch (e) {
      this.yadamuLogger.logException([`${this.constructor.name}._write()`,`"${this.tableInfo.tableName}"`],e);
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
 
class Multiplexor extends Transform {
  
  constructor(jsonParser,ddlWriter) {
    super();   
    this.jsonParser = jsonParser;
    this.ddlWriter = ddlWriter;  
  }

  // Push Data to jsonParser to find ddl object.
  
  async _transform (data,encoding,callback) {
    this.push(data)
    if (this.ddlWriter.getDDL() === undefined) {
      // ### Shouldn't be calling transform directly ?????
      // Rely on callback() being invoked by the jsonParser.
      this.jsonParser._transform(data,encoding,callback)
    }
    else {
      callback();
    }
  }

}

module.exports = OracleDBI
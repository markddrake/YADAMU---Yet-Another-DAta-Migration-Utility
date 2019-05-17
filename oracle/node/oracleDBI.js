"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;

/* 
**
** Require Database Vendors API 
**
*/

const oracledb = require('oracledb');
oracledb.fetchAsString = [ oracledb.DATE ]

const Yadamu = require('../../common/yadamu.js').Yadamu;
const YadamuDBI = require('../../common/yadamuDBI.js');
const DBParser = require('./dbParser.js');
const TableWriter = require('./tableWriter.js');
const StatementGenerator = require('./statementGenerator.js');

const defaultParameters = {
  BATCHSIZE         : 10000
, COMMITSIZE        : 10000
, LOBCACHESIZE      : 512
}

const dateFormatMasks = {
        Oracle      : 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
       ,MSSQLSERVER : 'YYYY-MM-DD"T"HH24:MI:SS.###"Z"'
       ,Postgres    : 'YYYY-MM-DD"T"HH24:MI:SS"+00:00"'
       ,MySQL       : 'YYYY-MM-DD"T"HH24:MI:SS.######"Z"'
       ,MariaDB     : 'YYYY-MM-DD"T"HH24:MI:SS.######"Z"'
}

const timestampFormatMasks = {
        Oracle      : 'YYYY-MM-DD"T"HH24:MI:SS.FF9"Z"'
       ,MSSQLSERVER : 'YYYY-MM-DD"T"HH24:MI:SS.FF7"Z"'
       ,Postgres    : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"+00:00"'
       ,MySQL       : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"'
       ,MariaDB     : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"'
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

const sqlSystemInformation = 
`select YADAMU_EXPORT.JSON_FEATURES() JSON_FEATURES, 
        YADAMU_EXPORT.DATABASE_RELEASE() DATABASE_RELEASE, 
        SYS_CONTEXT('USERENV','SESSION_USER') SESSION_USER, 
        SYS_CONTEXT('USERENV','DB_NAME') DATABASE_NAME, 
        SYS_CONTEXT('USERENV','SERVER_HOST') SERVER_HOST,
        SESSIONTIMEZONE SESSION_TIME_ZONE,
        JSON_OBJECTAGG(parameter, value) NLS_PARAMETERS
        from NLS_DATABASE_PARAMETERS`;

const sqlFetchDDL = 
`select COLUMN_VALUE JSON 
   from TABLE(YADAMU_EXPORT_DDL.FETCH_DDL_STATEMENTS(:schema))`;;

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

begin

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
    V_RESULT.APPEND('begin DBMS_XDBZ.ENABLE_HIERARCHY(SYS_CONTEXT(''USERENV'',''CURRENT_SCHEMA''),''' || t.TABLE_NAME  || '''); END;');
  end loop;

  for i in indexedColumnList(V_SCHEMA) loop
    V_RESULT.APPEND('BEGIN YADAMU_EXPORT_DDL.RENAME_INDEX(''' || i.TABLE_NAME  || ''',''' || i.INDEXED_EXPORT_SELECT_LIST || ''',''' || i.INDEX_NAME || '''); END;');
  end loop;

  :V2 :=  V_RESULT.to_CLOB();
  
end;`;

const sqlTableInfo =
`select * 
   from table(YADAMU_EXPORT.GET_DML_STATEMENTS(:schema))`;

class OracleDBI extends YadamuDBI {

  /*
  **
  ** Local methods 
  **
  */
  
  static parseConnectionString(connectionString) {
    
    const user = Yadamu.convertQuotedIdentifer(connectionString.substring(0,connectionString.indexOf('/')));
    let password = connectionString.substring(connectionString.indexOf('/')+1);
    let connectString = '';
    if (password.indexOf('@') > -1) {
	  connectString = password.substring(password.indexOf('@')+1);
	  password = password.substring(password,password.indexOf('@'));
    }
    return {
      user          : user,
      password      : password,
      connectString : connectString
    }
  }     

  lobFromJSON(json) {
  
    const s = new Readable();
    s.push(JSON.stringify(json));
    s.push(null);
   
    return OracleDBI.lobFromStream(this.connection,s);
  };
    
  static lobFromStream (conn,inStream) {

    return new Promise(async function(resolve,reject) {
      const tempLob =  await conn.createLob(oracledb.BLOB);
      tempLob.on('error',function(err) {reject(err);});
      tempLob.on('finish', function() {resolve(tempLob);});
      inStream.on('error', function(err) {reject(err);});
      inStream.pipe(tempLob);  // copies the text to the temporary LOB
    });  
  };
  
  lobFromFile (conn,filename) {
     const inStream = fs.createReadStream(filename);
     return OracleDBI.lobFromStream(conn,inStream);
  };
  
  trackClobFromStringReader(conn,s,list) {
      
    return new Promise(async function(resolve,reject) {
      try {
        const tempLob = await conn.createLob(oracledb.CLOB);
        list.push(tempLob)
        tempLob.on('error',function(err) {reject(err);});
        tempLob.on('finish', function() {resolve(tempLob)});
        s.on('error', function(err) {reject(err);});
        s.pipe(tempLob);  // copies the text to the temporary LOB
      }
      catch (e) {
        reject(e);
      }
    });  
  }

  trackClobFromString(str,list) {  
    const s = new Readable();
    s.push(str);
    s.push(null);

    return this.trackClobFromStringReader(this.connection,s,list);
    
  }
     
  getDateFormatMask(vendor) {
    
    return dateFormatMasks[vendor]
 
  }
  
  getTimeStampFormatMask(vendor) {
    
    return timestampFormatMasks[vendor]
 
  }
  
  async setDateFormatMask(conn,status,vendor) {
   
    let sqlStatement = `ALTER SESSION SET NLS_DATE_FORMAT = '${dateFormatMasks[vendor]}'`
    if (status.sqlTrace) {
      status.sqlTrace.write(`${sqlStatement}\n/\n`);
    }
    let result = await conn.execute(sqlStatement);
  
    sqlStatement = `ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '${timestampFormatMasks[vendor]}'`
    if (status.sqlTrace) {
      status.sqlTrace.write(`${sqlStatement}\n/\n`);
    }
    result = await conn.execute(sqlStatement);
  
  }
   
  async configureConnection(conn,status) {
    let sqlStatement = `ALTER SESSION SET TIME_ZONE = '+00:00'`
    if (status.sqlTrace) {
       status.sqlTrace.write(`${sqlStatement}\n/\n`);
    }
    let result = await conn.execute(sqlStatement);
  
    await this.setDateFormatMask(conn,status,'Oracle');
    
    sqlStatement = `ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM'`
    if (status.sqlTrace) {
       status.sqlTrace.write(`${sqlStatement}\n/\n`);
    }
    result = await conn.execute(sqlStatement);
  
    sqlStatement = `ALTER SESSION SET NLS_LENGTH_SEMANTICS = 'CHAR'`
    if (status.sqlTrace) {
       status.sqlTrace.write(`${sqlStatement}\n/\n`);
    }
    result = await conn.execute(sqlStatement);
    
    sqlStatement = `BEGIN :size := JSON_FEATURE_DETECTION.C_MAX_STRING_SIZE; END;`;
    const args = {size:{dir: oracledb.BIND_OUT, type: oracledb.NUMBER}}
    if (status.sqlTrace) {
       status.sqlTrace.write(`${sqlStatement}\n/\n`);
    }
    
    result = await conn.execute(sqlStatement,args);
    this.maxStringSize = result.outBinds.size;
    
    if (this.maxStringSize < 32768) {
      this.logWriter.write(`${new Date().toISOString()}[OracleDBI.configureConnection()]: Maximum VARCHAR2 size is ${this.maxStringSize}.\n`)
    }    
  }    
  
  static async getConnectionPool(connectionProperties) {
    
    const pool = await oracledb.createPool(connectionProperties)
    return pool;
  }

  async getConnectionFromPool(pool,status) {

    const conn = pool.getConnection();
    await this.configureConnection(conn,status);
    return conn;
  
  }

  async getConnection(connectionProperties,status) {
	const conn = await oracledb.getConnection(connectionProperties)
    await this.configureConnection(conn,status);
   
   return conn;
  }
  
  async releaseConnection(conn,logWriter) {
    if (conn !== undefined) {
      try {
        await conn.close();
      } catch (e) {
        this.logWriter.write(`${new Date().toISOString()}[OracleDBI.releaseConnection()]: ${e}\n${e.stack}\n`);
      }
    }
  };

  processLog(results) {
    const log = JSON.parse(results.outBinds.log);
    if (log !== null) {
      super.processLog(log, this.status, this.logWriter)
    }
    return log
  }

  async setCurrentSchema(schema) {

    const sqlStatement = `begin :log := YADAMU_IMPORT.SET_CURRENT_SCHEMA(:schema); end;`;
    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 1024} , schema:schema}
    const results = await this.executeSQL(sqlStatement,args)
    this.processLog(results)
    
  }
  
  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */

  async executeMany(sqlStatement,args,binds) {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement}\n/\n`);
    }
    
    const results = await this.connection.executeMany(sqlStatement,args,binds);
    return results;
  }

  async disableConstraints() {
  
    const sqlStatement = `begin :log := YADAMU_IMPORT.DISABLE_CONSTRAINTS(:schema); end;`;
    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , schema:this.parameters.TOUSER}
    const results = await this.executeSQL(sqlStatement,args)
    this.processLog(results)

  }
  
  async enableConstraints() {
  
    const sqlStatement = `begin :log := YADAMU_IMPORT.ENABLE_CONSTRAINTS(:schema); end;`;
    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , schema:this.parameters.TOUSER} 
    const results = await this.executeSQL(sqlStatement,args)
    this.processLog(results)
    
  }
  
  async refreshMaterializedViews() {
  
    const sqlStatement = `begin :log := YADAMU_IMPORT.REFRESH_MATERIALIZED_VIEWS(:schema); end;`;
    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , schema:this.parameters.TOUSER}     
    const results = await this.executeSQL(sqlStatement,args)
    this.processLog(results)

  }

  async executeSQL(sqlStatement,args) {
      
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement}\n/\n`);
    }    

    const results = await this.connection.execute(sqlStatement,args);
    return results;
  }

  /*
  **
  ** Overridden Methods
  **
  */
  
  get DATABASE_VENDOR()     { return 'Oracle' };
  get SOFTWARE_VENDOR()     { return 'Oracle Corporation' };
  get SPATIAL_FORMAT()      { return 'WKT' };
  get DEFAULT_PARAMETERS()  { return defaultParameters }
  get STATEMENT_SEPERATOR() { return '/' }

  constructor(yadamu) {
    super(yadamu,defaultParameters);
  }

  getConnectionProperties() {
    
    if (this.parameters.USERID) {
      return OracleDBI.parseConnectionString(this.parameters.USERID)
    }
    else {
     return{
       user             : this.parameters.USER
     , password         : this.parameters.PASSWORD
     , connectionString : this.parameters.CONNECT_STRING
     }
    }
  }

  async executeDDL(ddl) {
      
    /* ### OVERRIDE ### - Send DDL to server for execution ### */
    
    const sqlStatement = `begin :log := YADAMU_EXPORT_DDL.APPLY_DDL_STATEMENTS(:ddl, :schema); end;`;
    const ddlLob = await this.lobFromJSON({ systemInformation : this.systemInformation, ddl : ddl});  
    const args = {log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , ddl:ddlLob, schema:this.parameters.TOUSER};
    const results = await this.executeSQL(sqlStatement,args);
    await ddlLob.close();
    const log = this.processLog(results)
    // console.log(JSON.stringify(log,null,2))
    if (this.status.errorRaised === true) {
      throw new Error(`Oracle DDL Execution Failure`);
    }
  }
  
  /*  
  **
  **  Connect to the database. Set global setttings
  **
  */
  
  async initialize() {
    super.initialize();
    this.connection = await this.getConnection(this.connectionProperties,this.status)
  }
    
  /*
  **
  **  Gracefully close down the database connection.
  **
  */
 
  async finalize() {
    await this.setCurrentSchema(this.connectionProperties.user);
    await this.releaseConnection(this.connection, this.logWriter);
  }
   
  /*
  **
  **  Abort the database connection.
  **
  */

  async abort() {
    await this.releaseConnection(this.connection, this.logWriter);
  }

  /*
  **
  ** Commit the current transaction
  **
  */
  
  async commitTransaction() {
    await this.connection.commit();
  }

  /*
  **
  ** Abort the current transaction
  **
  */
  
  async rollbackTransaction() {
    await this.connection.rollback();

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
     const json = await this.lobFromFile(this.connection,importFilePath);
     return json;
  }

  /*
  **
  **  Process a JSON File that has been uploaded to the server. 
  **
  */
  
  async processFile(hndl) {

    let sqlStatement = "BEGIN" + "\n";
    
    if (this.maxStringSize < 32768) {
      console.log(hndl.ddl);
      let serverDDL = true
      if (hndl.ddl) {
        hndl.ddl.forEach(function (ddlOperation) {
           console.log(ddlOPeration, ddlOperation.length);
           if (ddlOperation.length > this.maxtringSize) {
             serverDDL = false;
           }
        },this);
      }
      console.log(serverDDL);
    }
           
    
    switch (this.parameters.MODE) {
	   case 'DDL_AND_DATA':
         sqlStatement = `${sqlStatement}  YADAMU_IMPORT.DATA_ONLY_MODE(FALSE);\n  YADAMU_IMPORT.DDL_ONLY_MODE(FALSE);\n`;
	     break;	   break
	   case 'DATA_ONLY':
         sqlStatement = `${sqlStatement}  YADAMU_IMPORT.DATA_ONLY_MODE(TRUE);\n  YADAMU_IMPORT.DDL_ONLY_MODE(FALSE);\n`;
         break;
	   case 'DDL_ONLY':
         sqlStatement = `${sqlStatement}  YADAMU_IMPORT.DDL_ONLY_MODE(TRUE);\n  YADAMU_IMPORT.DATA_ONLY_MODE(FALSE);\n`;
	     break;
    }	 
	 
    sqlStatement = `${sqlStatement}    :log := YADAMU_IMPORT.IMPORT_JSON(:json, :schema);\nEND;`;

    const results = await this.connection.execute(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024}, json:hndl, schema:this.parameters.TOUSER});
    return this.processLog(results);
  
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

  async getSystemInformation(EXPORT_VERSION) {     

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlSystemInformation}\n\/\n`)
    }

    const results = await this.connection.execute(sqlSystemInformation,[],{outFormat: oracledb.OBJECT ,})
    const sysInfo = results.rows[0];
    return {
      date               : new Date().toISOString()
     ,timeZoneOffset     : new Date().getTimezoneOffset()
     ,sessionTimeZone    : sysInfo.SESSION_TIME_ZONE
     ,vendor             : this.DATABASE_VENDOR
     ,spatialFormat      : this.SPATIAL_FORMAT 
     ,schema             : this.parameters.OWNER
     ,exportVersion      : EXPORT_VERSION
     ,sessionUser        : sysInfo.SESSION_USER
     ,dbName             : sysInfo.DATABASE_NAME
     ,databaseVersion    : sysInfo.DATABASE_RELEASE
     ,softwareVendor     : this.SOFTWARE_VENDOR
     ,hostname           : sysInfo.SERVER_HOST
     ,jsonFeatures       : JSON.parse(sysInfo.JSON_FEATURES)
     ,nlsParameters      : JSON.parse(sysInfo.NLS_PARAMETERS)
    }
    
  }

  /*
  **
  **  Generate a set of DDL operations from the metadata generated by an Export operation
  **
  */
  
  async getDDLOperations() {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlFetchDDL}\n\/\n`)
    }

    let ddl;
        
    if (this.systemInformation.databaseVersion < 19) {
      const results = await this.connection.execute(sqlFetchDDL,{schema: this.parameters.OWNER},{outFormat: oracledb.OBJECT,fetchInfo:{JSON:{type: oracledb.STRING}}})
      ddl = results.rows.map(function(row) {
        return row.JSON;
      },this);
    }
    else {

      /*
      **
      ** The pipelined table approach used by YADAMU_EXPORT_DDL appears to fail starting with release 19c. 
      ** Using Dynamic SQL appears to work correctly.
      **
      */
      
      const bindVars = {v1 : this.parameters.OWNER, v2 : {dir : oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 *1024 *1024}};
      const results = await this.connection.execute(sqlFetchDDL19c,bindVars)
      ddl = JSON.parse(results.outBinds.v2);
    }
    return ddl;    

  }

  async getSchemaInfo(schema) {
     
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlTableInfo}\n\/\n`)
    }

    const results = await this.connection.execute(sqlTableInfo,{schema: this.parameters[schema]},{outFormat: oracledb.OBJECT , fetchInfo:{
                                                                                                     COLUMN_LIST:          {type: oracledb.STRING}
                                                                                                    ,DATA_TYPE_LIST:       {type: oracledb.STRING}
                                                                                                    ,SIZE_CONSTRAINTS:     {type: oracledb.STRING}
                                                                                                    ,EXPORT_SELECT_LIST:   {type: oracledb.STRING}
                                                                                                    ,NODE_SELECT_LIST:     {type: oracledb.STRING}
                                                                                                    ,WITH_CLAUSE:          {type: oracledb.STRING}
                                                                                                    ,SQL_STATEMENT:        {type: oracledb.STRING}
                                                                                                  }
    });
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
    }
    return metadata;    
  }  
  
  generateSelectStatement(tableMetadata) {
     
    // Generate a conventional relational select statement for this table
    
    const query = {
      fetchInfo   : {}
     ,jsonColumns : []
     ,rawColumns  : []
    }   
    
    let selectList = '';
    const columnList = JSON.parse('[' + tableMetadata.COLUMN_LIST + ']');
    
    const dataTypeList = JSON.parse(tableMetadata.DATA_TYPE_LIST);
    dataTypeList.forEach(function(dataType,idx) {
      switch (dataType) {
        case 'JSON':
          query.jsonColumns.push(idx);
          break
        case 'RAW': 
          query.rawColumns.push(idx);
          break;
        default:
      }
    })
    
    query.sqlStatement = `select ${tableMetadata.NODE_SELECT_LIST} from "${tableMetadata.OWNER}"."${tableMetadata.TABLE_NAME}" t`; 
    
    if (tableMetadata.WITH_CLAUSE !== null) {
       query.sqlStatement = `with\n${tableMetadata.WITH_CLAUSE}\n${query.sqlStatement}`;
    }
    
    return query
  }
      
  createParser(query,objectMode) {
    return new DBParser(query,objectMode,this.logWriter);
  }  

  async getInputStream(query,parser) {

    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${query.sqlStatement}\n\/\n`)
    }
    
    const is = await this.connection.queryStream(query.sqlStatement,[],{extendedMetaData: true})
    is.on('metadata',function(metadata) {parser.setColumnMetadata(metadata)})
    return is;
  }
  
  /*
  **
  ** The following methods are used by the YADAMU DBwriter class
  **
  */
  
  async initializeDataLoad() {
    await this.disableConstraints();
    await this.setDateFormatMask(this.connection,this.status,this.systemInformation.vendor);
    await this.setCurrentSchema(this.parameters.TOUSER)
  }
  
  async generateStatementCache(schema,executeDDL) {
    await super.generateStatementCache(StatementGenerator,schema,executeDDL)
  }

  getTableWriter(table) {
    return super.getTableWriter(TableWriter,table)
  }
  
  async finalizeDataLoad() {
    await this.enableConstraints();
    await this.refreshMaterializedViews();
  }  

}

module.exports = OracleDBI
"use strict" 

import {
  setTimeout 
}                      from "timers/promises"

import MsSQLDBI        from '../../../node/dbi//mssql/mssqlDBI.js';
import MsSQLError      from '../../../node/dbi//mssql/mssqlException.js'
import MsSQLConstants  from '../../../node/dbi//mssql/mssqlConstants.js';

import YadamuTest      from '../../core/yadamu.js';
import YadamuQALibrary from '../../lib/yadamuQALibrary.js'

class MsSQLQA extends YadamuQALibrary.qaMixin(MsSQLDBI) {

    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }

    static #_YADAMU_DBI_PARAMETERS
    
    static get YADAMU_DBI_PARAMETERS()  { 
       this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,MsSQLConstants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[MsSQLConstants.DATABASE_KEY] || {},{RDBMS: MsSQLConstants.DATABASE_KEY}))
       return this.#_YADAMU_DBI_PARAMETERS
    }
   
    get YADAMU_DBI_PARAMETERS() {
      return MsSQLQA.YADAMU_DBI_PARAMETERS
    }   
        
    constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters)
    }
     
    async initialize() {
                
      // Must (re) create the database before attempting to connection. initialize() will fail if the database does not exist.
      if (this.options.recreateSchema === true) {
        await this.recreateDatabase();
        this.options.recreateSchema = false
      }
      await super.initialize();
      
    }      
    
    /*
    **
    ** The "Recreate Schema" option is problematic with SQL Server. 
    ** In SQL Server testing Schemas are mapped to databases, since there is no simple mechanism for dropping a schema cleanly in SQL Server.
    ** This means we have to deal with two scenarios when recreating a schema. First the required database may not exist, second it exists and needs to be dropped and recreated.
    ** Connect attempts fail if the target database does exist. This means that it necessary to connect to a known good database while the target database is recreated.
    ** After creating the database the connection must be closed and a new connection opened to the target database.
    **
    */    

    async recreateDatabase() {

      try { 
        const connectionProperties = Object.assign({},this.vendorProperties)
        const dbi = new MsSQLDBMgr(this.yadamuLogger,this.status, connectionProperties)
        await dbi.recreateDatabase(this.parameters.YADAMU_DATABASE)
      } catch (e) {
        this.yadamu.LOGGER.handleException([this.DATABASE_VENDOR,'RECREATE DATABASE',this.parameters.YADAMU_DATABASE],e);
        throw e
      }
    }

    async useDatabase(databaseName) {     
      const statement = `use ${databaseName}`
      const results = await this.executeSQL(statement);
    } 

   async getRowCounts(connectInfo) {
        
     await this.useDatabase(connectInfo.database);
     const results = await this.pool.request().input('SCHEMA',this.sql.VarChar,connectInfo.owner).query(MsSQLQA.SQL_SCHEMA_TABLE_ROWS);
      
     return results.recordset.map((row,idx) => {          
       return [connectInfo.owner === 'dbo' ? connectInfo.database : connectInfo.owner,row.TableName,parseInt(row.RowCount)]
     })
   }

   async compareSchemas(source,target,rules) {
       
      const report = {
        successful : []
       ,failed     : []
      }

      await this.useDatabase(source.database);
      
      let compareRules = this.yadamu.getCompareRules(rules)   
      compareRules = this.DB_VERSION  > 12 ? JSON.stringify(compareRules) : this.yadamu.makeXML(compareRules)

      
      let args = 
`--
-- declare @FORMAT_RESULTS         bit           = 0;
-- declare @SOURCE_DATABASE        varchar(128)  = '${source.database}';
-- declare @SOURCE_SCHEMA          varchar(128)  = '${source.owner}';
-- declare @TARGET_DATABASE        varchar(128)  = '${target.database}';
-- declare @TARGET_SCHEMA          varchar(128)  = '${target.owner}';
-- declare @COMMENT                varchar(128)  = '';
-- declare @RULES                  narchar(4000) = '${compareRules}';
--`;
            
      this.SQL_TRACE.trace(`${args}\nexecute sp_COMPARE_SCHEMA(@FORMAT_RESULTS,@SOURCE_DATABASE,@SOURCE_SCHEMA,@TARGET_DATABASE,@TARGET_SCHEMA,@COMMENT,@EMPTY_STRING_IS_NULL,@SPATIAL_PRECISION,@DATE_TIME_PRECISION)\ngo\n`)

      const request = this.getRequest();
      
      let results = await request
                          .input('FORMAT_RESULTS',this.sql.Bit,false)
                          .input('SOURCE_DATABASE',this.sql.VarChar,source.database)
                          .input('SOURCE_SCHEMA',this.sql.VarChar,source.owner)
                          .input('TARGET_DATABASE',this.sql.VarChar,target.database)
                          .input('TARGET_SCHEMA',this.sql.VarChar,target.owner)
                          .input('COMMENT',this.sql.VarChar,'')
                          .input('RULES',this.sql.VarChar,compareRules)
                          .execute(MsSQLQA.SQL_COMPARE_SCHEMAS,{},{resultSet: true});

      // Use length-2 and length-1 to allow Debugging info to be included in the output
      
      // console.log(results.recordsets[0])
      
      const successful = results.recordsets[results.recordsets.length-2]      
      report.successful = successful.map((row,idx) => {          
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.TARGET_ROW_COUNT,]
      })
        
      const failed = results.recordsets[results.recordsets.length-1]
      report.failed = failed.map((row,idx) => {
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.SOURCE_ROW_COUNT,row.TARGET_ROW_COUNT,row.MISSING_ROWS,row.EXTRA_ROWS,(row.SQLERRM !== null ? row.SQLERRM : '')]
      })

      return report
    }

    classFactory(yadamu) {
      return new MsSQLQA(yadamu,this,this.connectionSettings,this.parameters)
    }
       
    async scheduleTermination(pid,workerId) {
      const tags = this.getTerminationTags(workerId,pid)
      this.yadamuLogger.qa(tags,`Termination Scheduled.`);
      setTimeout(this.yadamu.KILL_DELAY,pid,{ref: false}).then(async (pid) => {
        if (this.pool !== undefined) {
          this.yadamuLogger.log(tags,`Killing connection.`);
          // Do not use getRequest() as it will fail with "There is a request in progress during write opeations. Get a non pooled request
          // const request = new this.sql.Request(this.pool);
          const request = await this.sql.connect(this.vendorProperties);
          let stack
          const sqlStatement = `kill ${pid}`
          try {
            stack = new Error().stack
            const res = await request.query(sqlStatement);
          } catch (e) {
            if (e.number && (e.number === 6104)) {
              // Msg 6104, Level 16, State 1, Line 1 Cannot use KILL to kill your own process
              this.yadamuLogger.log(tags,`Worker finished prior to termination.`)
            }
            else if (e.number && (e.number === 6106)) {
              // Msg 6106, Level 16, State 2, Line 1 Process ID 54 is not an active process ID.
              this.yadamuLogger.log(tags,`Worker finished prior to termination.`)
            }
            else {
              const cause = new MsSQLError(this.DRIVER_ID,e,stack,sqlStatement)
              this.yadamuLogger.handleException(tags,cause)
            }
          } 
        }
        else {
          this.yadamuLogger.log(tags,`Unable to Kill Connection: Connection Pool no longer available.`);
        }
	  })
    }

    
}

class MsSQLDBMgr extends MsSQLQA {
    
    constructor(logger,status,vendorProperties) {
      super({activeConnections: new Set(), STATUS: status},undefined,{},{})
      this.yadamuLogger = logger;
      this.status = status
      this.vendorProperties = vendorProperties
      this.vendorProperties.database = 'master';
    }
    
    async initialize() {
      await this._getDatabaseConnection()
    }
  
    async recreateDatabase(database) {

       const SINGLE_USER_MODE = `if DB_ID('${database}') IS NOT NULL alter database [${database}] set single_user with rollback immediate` 
       const DROP_DATABASE = `if DB_ID('${database}') IS NOT NULL drop database [${database}]`
  
    
      try {
        await this.initialize()
        // Create a connection pool using a well known database that must exist   
        this.vendorProperties.database = 'master';
        // await super.initialize();

        let results;       
        
        results =  await this.executeSQL(SINGLE_USER_MODE);      
        results =  await this.executeSQL(DROP_DATABASE);      
        const CREATE_DATABASE = `create database "${database}" COLLATE ${this.DB_COLLATION}`;
        results =  await this.executeSQL(CREATE_DATABASE);      
		
		await this.final()

      } catch (e) {
        console.log([this.DATABASE_VENDOR,'recreateDatabase()'],e);
		await this.destroy(e)
        throw e
      }
      
    }   
}  

export { MsSQLQA as default }

const _SQL_SCHEMA_TABLE_ROWS = `SELECT sOBJ.name AS [TableName], SUM(sPTN.Rows) AS [RowCount] 
   FROM sys.objects AS sOBJ 
  INNER JOIN sys.partitions AS sPTN ON sOBJ.object_id = sPTN.object_id 
  WHERE sOBJ.type = 'U' 
    AND sOBJ.schema_id = SCHEMA_ID(@SCHEMA) 
    AND sOBJ.is_ms_shipped = 0x0
    AND index_id < 2
 GROUP BY sOBJ.schema_id, sOBJ.name`;

const _SQL_COMPARE_SCHEMAS = `sp_COMPARE_SCHEMA`


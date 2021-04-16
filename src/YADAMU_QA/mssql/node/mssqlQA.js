"use strict" 

const MsSQLDBI = require('../../../YADAMU/mssql/node/mssqlDBI.js');
const MsSQLError = require('../../../YADAMU/mssql/node/mssqlException.js')
const MsSQLConstants = require('../../../YADAMU/mssql/node/mssqlConstants.js');

const YadamuTest = require('../../common/node/yadamuTest.js');

class MsSQLQA extends MsSQLDBI {
    
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
		
    constructor(yadamu) {
       super(yadamu)
    }

    setMetadata(metadata) {
      super.setMetadata(metadata)
    }
	 
	async initialize() {
				
	  if (this.options.recreateSchema === true) {
		await this.recreateDatabase();
	  }

	  await super.initialize();
	  if (this.terminateConnection()) {
        const pid = await this.getConnectionID();
	    this.scheduleTermination(pid,this.getWorkerNumber());
	  }
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

        // Cache the current value of YADAMU_DATABASE and remove it from this.parameters. This prevents the call to setTargetDatabase() from overriding the value of this.connectionProperties.database in createConnectionPool()
		
  	    const YADAMU_DATABASE = this.parameters.YADAMU_DATABASE;
		const database = this.connectionProperties.database
        delete this.parameters.YADAMU_DATABASE;
      
	    // Create a connection pool using a well known database that must exist	  
	    this.connectionProperties.database = 'master';
        await super.initialize();

	    this.parameters.YADAMU_DATABASE = YADAMU_DATABASE
	    this.connectionProperties.database = database;

        let results;       
        const dropDatabase = this.statementLibrary.DROP_DATABASE
        results =  await this.executeSQL(dropDatabase);      
      
        const createDatabase = `create database "${this.parameters.YADAMU_DATABASE}" COLLATE ${this.DB_COLLATION}`;
        results =  await this.executeSQL(createDatabase);      

        await this.finalize()
	  } catch (e) {
		this.yadamuLogger.qa([this.DATABASE_VENDOR,'recreateDatabase()'],e.message);
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
        return [connectInfo.owner === 'dbo' ? connectInfo.database : connectInfo.owner,row.TableName,row.RowCount]
      })
    }
	
    async workerDBI(idx)  {
	  const workerDBI = await super.workerDBI(idx);
      // Manager needs to schedule termination of worker.
	  if (this.terminateConnection(idx)) {
        const pid = await workerDBI.getConnectionID();
	    this.scheduleTermination(pid,idx);
	  }
	  return workerDBI
    }

   async compareSchemas(source,target,rules) {
	   
      const report = {
        successful : []
       ,failed     : []
      }

      await this.useDatabase(source.database);
	  
	  let compareParams
	  if (this.DB_VERSION > 12) {
		
	    compareParams = JSON.stringify({
  	      emptyStringIsNull    : rules.EMPTY_STRING_IS_NULL 
        , spatialPrecision     : rules.SPATIAL_PRECISION || 18
	   	, doublePrecision      : rules.DOUBLE_PRECISION || 18
	    , timestampPrecision   : rules.TIMESTAMP_PRECISION || 9
	    , orderedJSON          : Boolean(rules.ORDERED_JSON).toString().toLowerCase()
	    , xmlRule              : rules.XML_COMPARISSON_RULE || null
        });
	  }  
	  else {
	    compareParams = 
`<rules>
   <emptyStringIsNull>${Boolean(rules.EMPTY_STRING_IS_NULL).toString().toLowerCase()}</emptyStringIsNull>
   <spatialPrecision>${rules.SPATIAL_PRECISION || 18}</spatialPrecision>
   <doublePrecision>${rules.DOUBLE_PRECISION || 18}</doublePrecision>
   <timestampPrecision>${rules.TIMESTAMP_PRECISION || 9}</timestampPrecision>
   <orderedJSON>${Boolean(rules.ORDERED_JSON).toString().toLowerCase()}</orderedJSON>
   <xmlRule>${rules.XML_COMPARISSON_RULE || ''}</xmlRule>
</rules>`;
	  }
	  

      let args = 
`--
-- declare @FORMAT_RESULTS         bit           = 0;
-- declare @SOURCE_DATABASE        varchar(128)  = '${source.database}';
-- declare @SOURCE_SCHEMA          varchar(128)  = '${source.owner}';
-- declare @TARGET_DATABASE        varchar(128)  = '${target.database}';
-- declare @TARGET_SCHEMA          varchar(128)  = '${target.owner}';
-- declare @COMMENT                varchar(128)  = '';
-- declare @RULES                  narchar(4000) = '${compareParams}';
--`;
            
      this.status.sqlTrace.write(`${args}\nexecute sp_COMPARE_SCHEMA(@FORMAT_RESULTS,@SOURCE_DATABASE,@SOURCE_SCHEMA,@TARGET_DATABASE,@TARGET_SCHEMA,@COMMENT,@EMPTY_STRING_IS_NULL,@SPATIAL_PRECISION,@DATE_TIME_PRECISION)\ngo\n`)

      const request = this.getRequest();
	  
      let results = await request
                          .input('FORMAT_RESULTS',this.sql.Bit,false)
                          .input('SOURCE_DATABASE',this.sql.VarChar,source.database)
                          .input('SOURCE_SCHEMA',this.sql.VarChar,source.owner)
                          .input('TARGET_DATABASE',this.sql.VarChar,target.database)
                          .input('TARGET_SCHEMA',this.sql.VarChar,target.owner)
                          .input('COMMENT',this.sql.VarChar,'')
                          .input('RULES',this.sql.VarChar,compareParams)
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
   
	async scheduleTermination(pid,workerId) {
      this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Termination Scheduled.`);
      const timer = setTimeout(
        async (pid) => {
		  if (this.pool !== undefined) {
		     this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Killing connection.`);
			 // Do not use getRequest() as it will fail with "There is a request in progress during write opeations. Get a non pooled request
		     // const request = new this.sql.Request(this.pool);
			 const request = await this.sql.connect(this.connectionProperties);
			 let stack
			 const sqlStatement = `kill ${pid}`
			 try {
			   stack = new Error().stack
  		       const res = await request.query(sqlStatement);
			 } catch (e) {
			   if (e.number && (e.number === 6104)) {
				 // Msg 6104, Level 16, State 1, Line 1 Cannot use KILL to kill your own process
			     this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Worker finished prior to termination.`)
 			   }
			   else if (e.number && (e.number === 6106)) {
				 // Msg 6106, Level 16, State 2, Line 1 Process ID 54 is not an active process ID.
			     this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Worker finished prior to termination.`)
 			   }
			   else {
				 const cause = new MsSQLError(e,stack,sqlStatement)
			     this.yadamuLogger.handleException(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],cause)
			   }
			 } 
		   }
		   else {
		     this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Unable to Kill Connection: Connection Pool no longer available.`);
		   }
		},
		this.killConfiguration.delay,
	    pid
      )
	  timer.unref()
	}

}
module.exports = MsSQLQA

const _SQL_SCHEMA_TABLE_ROWS = `SELECT sOBJ.name AS [TableName], SUM(sPTN.Rows) AS [RowCount] 
   FROM sys.objects AS sOBJ 
  INNER JOIN sys.partitions AS sPTN ON sOBJ.object_id = sPTN.object_id 
  WHERE sOBJ.type = 'U' 
    AND sOBJ.schema_id = SCHEMA_ID(@SCHEMA) 
    AND sOBJ.is_ms_shipped = 0x0
    AND index_id < 2
 GROUP BY sOBJ.schema_id, sOBJ.name`;

const _SQL_COMPARE_SCHEMAS = `sp_COMPARE_SCHEMA`


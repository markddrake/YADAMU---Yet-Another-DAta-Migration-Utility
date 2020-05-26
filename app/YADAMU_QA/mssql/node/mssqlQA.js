"use strict" 

const MsSQLDBI = require('../../../YADAMU/mssql/node/mssqlDBI.js');
const {MsSQLError} = require('../../../YADAMU/common/yadamuError.js')

const sqlSchemaTableRows = `SELECT sOBJ.name AS [TableName], SUM(sPTN.Rows) AS [RowCount] 
   FROM sys.objects AS sOBJ 
  INNER JOIN sys.partitions AS sPTN ON sOBJ.object_id = sPTN.object_id 
  WHERE sOBJ.type = 'U' 
    AND sOBJ.schema_id = SCHEMA_ID(@SCHEMA) 
    AND sOBJ.is_ms_shipped = 0x0
    AND index_id < 2
 GROUP BY sOBJ.schema_id, sOBJ.name`;

const sqlCompareSchema = `sp_COMPARE_SCHEMA`

class MsSQLQA extends MsSQLDBI {
    
    constructor(yadamu) {
       super(yadamu)
    }

	async useDatabase(databaseName) {     
      const statement = `use ${databaseName}`
      const results = await this.executeSQL(statement);
    } 
	
	async recreateDatabase() {
		
      // Connect to 'master', drop and recreate the target database before establishing the connection pool;

	  const database = this.connectionProperties.database;
      const MSSQL_SCHEMA_DB = this.parameters.MSSQL_SCHEMA_DB;
      delete this.parameters.MSSQL_SCHEMA_DB;
      
	  this.connectionProperties.database = 'master';
      await this.createConnectionPool(); 

      try {
        let results;       
        const dropDatabase = `drop database if exists "${MSSQL_SCHEMA_DB}"`;
        results =  await this.executeSQL(dropDatabase);      
      
        const createDatabase = `create database "${MSSQL_SCHEMA_DB}" COLLATE ${this.defaultCollation}`;
        results =  await this.executeSQL(createDatabase);      
        await this.finalize()
	  } catch (e) {
		this.yadamuLogger.qa([this.DATABASE_VENDOR,'recreateDatabase()'],e.message);
	  }
	  
	  this.parameters.MSSQL_SCHEMA_DB = MSSQL_SCHEMA_DB
	  this.connectionProperties.database = database;
	  
    }
	
	async scheduleTermination(pid) {
	  const self = this
	  const killOperation = this.parameters.KILL_READER_AFTER ? 'Reader'  : 'Writer'
	  const killDelay = this.parameters.KILL_READER_AFTER ? this.parameters.KILL_READER_AFTER  : this.parameters.KILL_WRITER_AFTER
	  const timer = setTimeout(
	    async function(pid) {
		  if (self.pool !== undefined) {
		     self.yadamuLogger.qa(['KILL',self.DATABASE_VENDOR,killOperation,killDelay,pid,self.getSlaveNumber()],`Killing connection.`);
		     const request = await self.getRequest();
			 let stack
			 const sqlStatement = `kill ${pid}`
			 try {
			   stack = new Error().stack
  		       const res = await request.query(sqlStatement);
			 } catch (e) {
			   if (e.number && (e.number === 6104)) {
				 // The Slave has finished and it's SID and SERIAL# appears to have been assigned to the connection being used to issue the KILLL SESSION and you can't kill yourself (Error 27)
			     self.yadamuLogger.qa(['KILL',self.DATABASE_VENDOR,killOperation,killDelay,pid,self.getSlaveNumber()],`Slave finished prior to termination.`)
 			   }
			   else {
				 const cause = new MsSQLError(e,stack,sqlStatement)
			     self.yadamuLogger.handleException(['KILL',self.DATABASE_VENDOR,killOperation,killDelay,pid,self.getSlaveNumber()],cause)
			   }
			 } 
		   }
		   else {
		     self.yadamuLogger.qa(['KILL',self.DATABASE_VENDOR,killOperation,killDelay,pid,self.getSlaveNumber()],`Unable to Kill Connection: Connection Pool no longer available.`);
		   }
		},
		killDelay,
	    pid
      )
	  timer.unref()
	}
	
	async initialize() {
	  if (this.options.recreateSchema === true) {
		await this.recreateDatabase();
	  }
	  await super.initialize();
	  if (this.testLostConnection()) {
        const dbiID = await this.getConnectionID();
		this.scheduleTermination(dbiID);
	  }
    }
	   
   async compareSchemas(source,target) {
	   
      const report = {
        successful : []
       ,failed     : []
      }

      await this.useDatabase(source.database);

      let args = 
`--
-- declare @FORMAT_RESULTS         bit          = 0;
-- declare @SOURCE_DATABASE        varchar(128) = '${source.database}';
-- declare @SOURCE_SCHEMA          varchar(128) = '${source.owner}';
-- declare @TARGET_DATABASE        varchar(128)  = '${target.database}';
-- declare @TARGET_SCHEMA          varchar(128) = '${target.owner}';
-- declare @COMMENT                varchar(128) = '';
-- declare @EMPTY_STRING_IS_NULL   bit = ${this.parameters.EMPTY_STRING_IS_NULL === true};
-- declare @SPATIAL_PRECISION      varchar(128) = ${this.parameters.hasOwnProperty('SPATIAL_PRECISION') ? this.parameters.SPATIAL_PRECISION : 18};
-- declare @DATE_TIME_PRECISION    varchar(128)  = ${this.parameters.TIMESTAMP_PRECISION};
--`;
            
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(`${args}\nexecute sp_COMPARE_SCHEMA(@FORMAT_RESULTS,@SOURCE_DATABASE,@SOURCE_SCHEMA,@TARGET_DATABASE,@TARGET_SCHEMA,@COMMENT,@EMPTY_STRING_IS_NULL,@SPATIAL_PRECISION,@DATE_TIME_PRECISION)\ngo\n`)
      }

      const request = this.getRequest();
      let results = await request
                          .input('FORMAT_RESULTS',this.sql.Bit,false)
                          .input('SOURCE_DATABASE',this.sql.VarChar,source.database)
                          .input('SOURCE_SCHEMA',this.sql.VarChar,source.owner)
                          .input('TARGET_DATABASE',this.sql.VarChar,target.database)
                          .input('TARGET_SCHEMA',this.sql.VarChar,target.owner)
                          .input('COMMENT',this.sql.VarChar,'')
                          .input('EMPTY_STRING_IS_NULL',this.sql.Bit,(this.parameters.EMPTY_STRING_IS_NULL === true ? 1 : 0))
                          .input('SPATIAL_PRECISION',this.sql.Int,(this.parameters.hasOwnProperty('SPATIAL_PRECISION') ? this.parameters.SPATIAL_PRECISION : 18))
                          .input('DATE_TIME_PRECISION',this.sql.Int,this.parameters.TIMESTAMP_PRECISION)
                          .execute(sqlCompareSchema,{},{resultSet: true});

      // Use length-2 and length-1 to allow Debugging info to be included in the output
	  
      const successful = results.recordsets[results.recordsets.length-2]
      
      report.successful = successful.map(function(row,idx) {          
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.TARGET_ROW_COUNT,]
      },this)
        
      const failed = results.recordsets[results.recordsets.length-1]

      report.failed = failed.map(function(row,idx) {
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.SOURCE_ROW_COUNT,row.TARGET_ROW_COUNT,row.MISSING_ROWS,row.EXTRA_ROWS,(row.SQLERRM !== null ? row.SQLERRM : '')]
      },this)

      return report
    }
   
    async getRowCounts(connectInfo) {
        
      await this.useDatabase(connectInfo.database);
      const results = await this.pool.request().input('SCHEMA',this.sql.VarChar,connectInfo.owner).query(sqlSchemaTableRows);
      
      return results.recordset.map(function(row,idx) {          
        return [connectInfo.owner === 'dbo' ? connectInfo.database : connectInfo.owner,row.TableName,row.RowCount]
      },this)
    }
	
  async slaveDBI(idx)  {
	const slaveDBI = await super.slaveDBI(idx);
	if (slaveDBI.testLostConnection()) {
	  const dbiID = await slaveDBI.getConnectionID();
	  this.scheduleTermination(dbiID);
    }
	return slaveDBI
  }
    
}
module.exports = MsSQLQA
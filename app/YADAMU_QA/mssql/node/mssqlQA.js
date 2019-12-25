"use strict" 

const MsSQLDBI = require('../../../YADAMU/mssql/node/mssqlDBI.js');

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
    
	async initialize() {
	  if (this.options.recreateSchema === true) {
		await this.recreateDatabase();
	  }
	  await super.initialize();
	}
	
	async useDatabase(databaseName) {     
      const statement = `use ${databaseName}`
      const results = await this.executeBatch(statement,this.generateRequest());
    } 
	
	async recreateDatabase() {
		
      // Connect to 'master', drop and recreate the target database before establishing the connection pool;

	  const database = this.connectionProperties.database;
      const MSSQL_SCHEMA_DB = this.parameters.MSSQL_SCHEMA_DB;
      delete this.parameters.MSSQL_SCHEMA_DB;
      
	  this.connectionProperties.database = 'master';
      await this.createConnectionPool(); 

      let results;       
      const dropDatabase = `drop database if exists "${MSSQL_SCHEMA_DB}"`;
      results =  await this.executeBatch(dropDatabase,this.generateRequest());      
      
      const createDatabase = `create database "${MSSQL_SCHEMA_DB}" COLLATE Latin1_General_100_CS_AS_SC`;
      results =  await this.executeBatch(createDatabase,this.generateRequest());      
      await this.pool.close();
	  
	  this.parameters.MSSQL_SCHEMA_DB = MSSQL_SCHEMA_DB
	  this.connectionProperties.database = database;
       
   }
   
   async compareSchemas(source,target) {
	   
      const report = {
        successful : []
       ,failed     : []
      }

      await this.useDatabase(source.database);

      let args = 
`--
--@FORMAT_RESULTS         = false
--@SOURCE_DATABASE        = '${source.database}'
--@SOURCE_SCHEMA          = '${source.owner}'
--@TARGET_DATABASE        = '${target.database}'
--@TARGET_SCHEMA          = '${target.owner}'
--@COMMENT                = ''
--@EMPTY_STRING_IS_NULL   = ${this.parameters.EMPTY_STRING_IS_NULL === true}
--@SPATIAL_PRECISION      = ${this.parameters.hasOwnProperty('SPATIAL_PRECISION') ? this.parameters.SPATIAL_PRECISION : 18}
--@DATE_TIME_PRECISION    = ${this.parameters.TIMESTAMP_PRECISION}
--`;
            
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(`${args}\nexecute sp_COMPARE_SCHEMA(@FORMAT_RESULTS,@SOURCE_DATABASE,@SOURCE_SCHEMA,@TARGET_DATABASE,@TARGET_SCHEMA,@COMMENT,@EMPTY_STRING_IS_NULL,@SPATIAL_PRECISION)\ngo\n`)
      }

      const request = this.generateRequest();
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
    
}
module.exports = MsSQLQA
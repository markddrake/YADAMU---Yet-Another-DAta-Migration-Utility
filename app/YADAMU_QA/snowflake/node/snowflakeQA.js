"use strict" 
const SnowflakeDBI = require('../../../YADAMU/snowflake/node/snowflakeDBI.js');

class SnowflakeQA extends SnowflakeDBI {
    
    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }

    constructor(yadamu) {
       super(yadamu)
    }
	
    async initialize() {
	  if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	  }
	  await super.initialize();
	}

    async recreateSchema() {
		
	  const database = this.connectionProperties.database;
      const SNOWFLAKE_SCHEMA_DB = this.parameters.SNOWFLAKE_SCHEMA_DB;
      delete this.parameters.SNOWFLAKE_SCHEMA_DB;
      
	  this.connectionProperties.database = '';
      await this.createConnectionPool(); 
	  this.connection = await this.getConnectionFromPool();
		
	  let results
      try {
        const createDatabase = `create transient database if not exists "${SNOWFLAKE_SCHEMA_DB}" DATA_RETENTION_TIME_IN_DAYS = 0;`;
        results =  await this.executeSQL(createDatabase,[]);      
        const useDatabase = `USE DATABASE "${SNOWFLAKE_SCHEMA_DB}";`;
        results =  await this.executeSQL(useDatabase,[]);      
        const dropSchema = `drop schema if exists "${SNOWFLAKE_SCHEMA_DB}"."${this.parameters.TO_USER}";`;
        results =  await this.executeSQL(dropSchema,[]);      
        const createSchema = `create transient schema "${SNOWFLAKE_SCHEMA_DB}"."${this.parameters.TO_USER}" DATA_RETENTION_TIME_IN_DAYS=0;`;
        results =  await this.executeSQL(createSchema,[]);      
      } catch (e) {
		console.log(e)
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
	  
	  await this.finalize()
	  this.parameters.SNOWFLAKE_SCHEMA_DB = SNOWFLAKE_SCHEMA_DB
	  this.connectionProperties.database = database;
    }   

    async getRowCounts(target) {
      
	  const results = await this.executeSQL(SnowflakeQA.SQL_SCHEMA_TABLE_ROWS,[target.schema]); 
	  return results.map((row,idx) => {          
        return [target.schema,row.TABLE_NAME,row.ROW_COUNT]
      })
	}
	
    async compareSchemas(source,target) {
     
      const useDatabase = `USE DATABASE "${source.database}";`;
      let results =  await this.executeSQL(useDatabase,[]);      
         
      const report = {
        successful : []
       ,failed     : []
      }

      results = await this.executeSQL(SnowflakeQA.SQL_COMPARE_SCHEMAS,[source.database,source.schema,target.schema]);
      
      let compare = JSON.parse(results[0].COMPARE_SCHEMAS)
	  compare.forEach((result) => {
		if ((result[3] === result[4]) && (result[5] === 0) && (result[6] === 0)) {
		  report.successful.push([result[0],result[1],result[2],result[4]]);
		} 
		else {
	      report.failed.push(result)
		}
	  })
	 
	  // console.log(report);
      return report
    }
      
}

module.exports = SnowflakeQA

const _SQL_COMPARE_SCHEMAS = `call YADAMU_SYSTEM.PUBLIC.COMPARE_SCHEMAS(:1,:2,:3);`
const _SQL_SCHEMA_TABLE_ROWS = `select TABLE_NAME, ROW_COUNT from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = 'BASE TABLE' and TABLE_SCHEMA = ?`;


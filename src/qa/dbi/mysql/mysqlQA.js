
import {
  setTimeout 
}                      from "timers/promises"

import MySQLDBI        from '../../../node/dbi//mysql/mysqlDBI.js';
import MySQLError      from '../../../node/dbi//mysql/mysqlException.js'
import MySQLConstants  from '../../../node/dbi//mysql/mysqlConstants.js';

import Yadamu          from '../../core/yadamu.js';
import YadamuQALibrary from '../../lib/yadamuQALibrary.js'

class MySQLQA extends YadamuQALibrary.qaMixin(MySQLDBI) {

    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }

	static #_DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,MySQLConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[MySQLConstants.DATABASE_KEY] || {},{RDBMS: MySQLConstants.DATABASE_KEY}))
	   return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return MySQLQA.DBI_PARAMETERS
    }	
			
    constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters)
    }
   
    async recreateSchema() {
        
      try {
        const dropSchema = `drop schema if exists "${this.parameters.TO_USER}"`;
        await this.executeSQL(dropSchema,{});      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
      const createSchema = `create schema "${this.parameters.TO_USER}"`;
      await this.executeSQL(createSchema,{});      
    }  

    // ### Hack to avoid missng rows when using a new connection to read previously written rows using new connection immediately after closing current connection.....'

	async finalizeImport() {
	  await setTimeout(2500);
	  super.finalizeImport()
    }
	
  
	
    async getRowCounts(target) {

      let results = await this.executeSQL(MySQLQA.SQL_SCHEMA_TABLE_ROWS,[target.schema]);
      
	  // const ACURATE_ROW_COUNT = results.map((row) => {return `select '${target.schema}', '${row.TABLE_NAME}', count(*) from "${target.schema}"."{row.TABLE}"`}).join('\nunion all\n');
      // results = await this.executeSQL(MySQLQA.ACURATE_ROW_COUNT);
	  
      return results.map((row,idx) => {          
        return [target.schema,row.TABLE_NAME,parseInt(row.TABLE_ROWS)]
      })
	  

    }
    
    async compareSchemas(source,target,rules) {     

      const report = {
        successful : []
       ,failed     : []
      }
      const compareRules =  this.yadamu.getCompareRules(rules)
	  
      let results = await this.executeSQL(MySQLQA.SQL_COMPARE_SCHEMAS,[source.schema,target.schema,JSON.stringify(compareRules)]);

      const successful = await this.executeSQL(MySQLQA.SQL_SUCCESS,{})
      report.successful = successful.map((row,idx) => {          
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,parseInt(row.TARGET_ROW_COUNT)]
      })

      const failed = await this.executeSQL(MySQLQA.SQL_FAILED,{})
      report.failed = failed.map((row,idx) => {
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,parseInt(row.SOURCE_ROW_COUNT),parseInt(row.TARGET_ROW_COUNT),parseInt(row.MISSING_ROWS),parseInt(row.EXTRA_ROWS),(row.NOTES !== undefined ? row.NOTES : '')]
      })
      
      return report
    }
   
   classFactory(yadamu) {
      return new MySQLQA(yadamu,this,this.connectionParameters,this.parameters)
    }
	
	async scheduleTermination(pid,workerId) {
	  let stack
	  const operation = `kill ${pid}`
	  const tags = this.getTerminationTags(workerId,pid)
	  this.yadamuLogger.qa(tags,`Termination Scheduled.`);
	  setTimeout(this.yadamu.KILL_DELAY,pid,{ref : false}).then(async (pid) => {
   	    if (this.pool !== undefined && this.pool.end) {
		  stack = new Error().stack
    	  this.yadamuLogger.log(tags,`Killing connection.`);
     	  const conn = await this.getConnectionFromPool();
		  
		  const res = await conn.query(operation);
		  await conn.release()
		}
		else {
		  this.yadamuLogger.log(tags,`Unable to Kill Connection: Connection Pool no longer available.`);
		}
      }).catch((e) => {
        this.yadamu.LOGGER.handleException(tags,new MySQLError(this.DRIVER_ID,e,stack,operation));
      })
	}	
	
}

export { MySQLQA as default }

const _SQL_SUCCESS =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'SUCCESSFUL' as "RESULTS", TARGET_ROW_COUNT
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
   and MISSING_ROWS = 0
   and EXTRA_ROWS = 0
   and NOTES is NULL
order by TABLE_NAME`;

const _SQL_FAILED = 
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'FAILED' as "RESULTS", SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, NOTES
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
    or MISSING_ROWS <> 0
    or EXTRA_ROWS <> 0
    or NOTES is NOT NULL
 order by TABLE_NAME`;

const _SQL_SCHEMA_TABLE_ROWS = `select TABLE_NAME, TABLE_ROWS from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = ?`;

const _SQL_COMPARE_SCHEMAS =  `CALL COMPARE_SCHEMAS(?,?,?);`;

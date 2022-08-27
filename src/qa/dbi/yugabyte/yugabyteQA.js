"use strict" 

import {
  setTimeout 
}                        from "timers/promises"

import YugabyteError     from '../../../node/dbi/postgres/postgresException.js'
import YugabyteConstants from '../../../node/dbi/postgres/postgresConstants.js';

import Yadamu            from '../../core/yadamu.js';
import YadamuQALibrary   from '../../lib/yadamuQALibrary.js'

import YugabyteDBI       from '../../../node/dbi/yugabyte/yugabyteDBI.js';


class YugabyteQA extends YadamuQALibrary.qaMixin(YugabyteDBI) {
    
    static get SQL_SCHEMA_TABLE_NAMES()    { return _SQL_SCHEMA_TABLE_NAMES }
    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_GENERATE_COMPARE()      { return _SQL_GENERATE_COMPARE }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }

	static #_DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,YugabyteConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[YugabyteConstants.DATABASE_KEY] || {},{RDBMS: YugabyteConstants.DATABASE_KEY}))
	   return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return YugabyteQA.DBI_PARAMETERS
    }	
		
    constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters);
    }

 	async recreateSchema() {
      try {
        const dropSchema = `drop schema if exists "${this.parameters.TO_USER}" cascade`;
        const results = await this.executeSQL(dropSchema);      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
      await this.createSchema(this.parameters.TO_USER);    
    }      

	async getRowCounts(target) {
		

	  let results = await this.executeSQL(`select table_name from information_schema.tables where table_schema = '${this.CURRENT_SCHEMA}'`)
	  results = results.rows.map((result) => { return Object.values(result)})
	  if (results.length > 0 ) {
	    const sqlCountRows = results.map((row) => { return `select cast('${this.CURRENT_SCHEMA}' as VARCHAR(128)), cast('${row[0]}' as VARCHAR(128)), count(*) from "${this.CURRENT_SCHEMA}"."${row[0]}"`}).join('\nunion all \n')
	    results = await this.executeSQL(sqlCountRows)
	    results = results.rows.map((result) => { return Object.values(result)})
	  }
	  return results
    }    

    async compareSchemas(source,target,rules) {

      const report = {
        successful : []
       ,failed     : []
      }

      const compareRules = this.yadamu.getCompareRules(rules)	  

      // Fails :  Query error: Restart read required at: { read: { physical: 1657219918659873 logical: 1 } local_limit: { physical: 1657219918659873 logical: 1 } global_limit: <min> in_txn_limit: <max> serial_no: 0 }	  
      
	  let results = await this.executeSQL(YugabyteQA.SQL_GENERATE_COMPARE,[source.schema,compareRules])      
    	  
      const tableList = (results.rows).filter((tableInfo) => {
         return ((this.TABLE_FILTER.length === 0) || (this.TABLE_FILTER.includes(tableInfo[0])))
	   })

      const timeout_period = 30 * 60 * 1000;
      results = await this.executeSQL(`set statement_timeout=${timeout_period}`);
	  
	  const compareResults = [] 
      for (const table of tableList) {
        const sqlStatement =
`select 
  '${table[0]}' "TABLE_NAME",
  (select count(*) from "${source.schema}"."${table[0]}") SOURCE_ROWS,
  (select count(*) from "${target.schema}"."${table[0]}") TARGET_ROWS,
  (select count(*) from (select ${table[1]} from "${source.schema}"."${table[0]}" except select ${table[1]} from "${target.schema}"."${table[0]}") T1) EXTRA_ROWS,
  (select count(*) from (select ${table[1]} from "${target.schema}"."${table[0]}" except select ${table[1]} from "${source.schema}"."${table[0]}") T2) MISSING_ROWS`
  
        // console.log(sqlStatement)
		
        try {
          let results = await this.executeSQL(sqlStatement);
  		  results = Object.values(results.rows[0])
          compareResults.push(results)
		} catch (e) {
		  this.yadamuLogger.handleException([this.DATABASE_VENDOR,'QA','COMPARE_SCHEMA'],e)  
		  compareResults.push([table[0],-1,-1,-1,-1,e.message])
		}
	  }
	 
	 compareResults.forEach((results,idx) => {
        const compareResult =  results
        if ((parseInt(compareResult[1]) === parseInt(compareResult[2])) && (parseInt(compareResult[3]) === 0) && (parseInt(compareResult[4])  === 0)) {
          report.successful.push(new Array(source.schema,target.schema,compareResult[0],compareResult[2]))
        }
        else {
          report.failed.push(new Array(source.schema,target.schema,...compareResult,''))
        }
      })
     
      return report
    }  
 
    classFactory(yadamu) {
      return new DB2QA(yadamu,this,this.connectionParameters,this.parameters)
    }
	
	async scheduleTermination(pid,workerId) {
	  let stack
	  const operation = `select pg_terminate_backend(${pid})`
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
        this.yadamu.LOGGER.handleException(tags,new YugabyteError(this.DRIVER_ID,e,stack,operation));
      })
	}
		
    classFactory(yadamu) {
      return new YugabyteQA(yadamu,this,this.connectionParameters,this.parameters)
    }
	
	async scheduleTermination(pid,workerId) {
	  let stack
	  const operation = `select pg_terminate_backend(${pid})`
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
        this.yadamu.LOGGER.handleException(tags,new YugabyteError(this.DRIVER_ID,e,stack,operation));
      })
	}

}

export { YugabyteQA as default }

const _SQL_SUCCESS =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT
   from SCHEMA_COMPARE_RESULTS 
  where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
    and MISSING_ROWS = 0
    and EXTRA_ROWS = 0
    and SQLERRM is NULL
 order by TABLE_NAME`;

const _SQL_FAILED = 
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS,  SQLERRM
   from SCHEMA_COMPARE_RESULTS 
  where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
     or MISSING_ROWS <> 0
      or EXTRA_ROWS <> 0
    or SQLERRM is NOT NULL
  order by TABLE_NAME`;
  
const _SQL_SCHEMA_TABLE_NAMES = `select relname from pg_stat_user_tables where schemaname = $1`;

// const _SQL_SCHEMA_TABLE_ROWS  = `select schemaname, relname, n_live_tup from pg_stat_user_tables where schemaname = $1`;

const _SQL_COMPARE_SCHEMAS    = `call YADAMU.COMPARE_SCHEMA($1,$2,$3)`

const _SQL_GENERATE_COMPARE   = `select * from YADAMU.GENERATE_COMPARE_COLUMNS($1,$2)`
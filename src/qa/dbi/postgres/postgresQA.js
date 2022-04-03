"use strict" 

import {
  setTimeout 
}                        from "timers/promises"

import PostgresDBI       from '../../../node/dbi//postgres/postgresDBI.js';
import PostgresError     from '../../../node/dbi//postgres/postgresException.js'
import PostgresConstants from '../../../node/dbi//postgres/postgresConstants.js';

import Yadamu            from '../../core/yadamu.js';
import YadamuQALibrary   from '../../lib/yadamuQALibrary.js'


class PostgresQA extends YadamuQALibrary.qaMixin(PostgresDBI) {
    
    static get SQL_SCHEMA_TABLE_NAMES()    { return _SQL_SCHEMA_TABLE_NAMES }
    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }

	static #_DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,PostgresConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[PostgresConstants.DATABASE_KEY] || {},{RDBMS: PostgresConstants.DATABASE_KEY}))
	   return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return PostgresQA.DBI_PARAMETERS
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
        
      const results = await this.executeSQL(PostgresQA.SQL_SCHEMA_TABLE_ROWS,[target.schema]);
      return results.rows
    }    

    async compareSchemas(source,target,rules) {

      const report = {
        successful : []
       ,failed     : []
      }

      const compareRules = this.yadamu.getCompareRules(rules)	  

      await this.executeSQL(PostgresQA.SQL_COMPARE_SCHEMAS,[source.schema,target.schema,compareRules])      
      
      const successful = await this.executeSQL(PostgresQA.SQL_SUCCESS)            
      report.successful = successful.rows
      
      const failed = await this.executeSQL(PostgresQA.SQL_FAILED)
      report.failed = failed.rows

      return report
    }
		
    classFactory(yadamu) {
      return new PostgresQA(yadamu,this,this.connectionParameters,this.parameters)
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
        this.yadamu.LOGGER.handleException(tags,new PostgresError(this.DRIVER_ID,e,stack,operation));
      })
	}

}

export { PostgresQA as default }

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

const _SQL_SCHEMA_TABLE_ROWS = 
`with ROW_COUNTS as (
select table_schema, table_name,  query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
  from information_schema.tables
 where table_schema = $1 
)
select table_schema, table_name, (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
  from ROW_COUNTS`

const _SQL_COMPARE_SCHEMAS    = `call COMPARE_SCHEMA($1,$2,$3)`
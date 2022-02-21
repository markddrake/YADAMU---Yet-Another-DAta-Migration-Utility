"use strict" 

import MariadbDBI       from '../../../YADAMU/mariadb/node/mariadbDBI.js';
import MariadbError     from '../../../YADAMU/mariadb/node/mariadbException.js'
import MariadbConstants from '../../../YADAMU/mariadb/node/mariadbConstants.js';

import YadamuTest       from '../../common/node/yadamuTest.js';
import YadamuQALibrary  from '../../common/node/yadamuQALibrary.js'

class MariadbQA extends YadamuQALibrary.qaMixin(MariadbDBI) {

    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }

	static #_YADAMU_DBI_PARAMETERS
	
	static get YADAMU_DBI_PARAMETERS()  { 
	   this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,MariadbConstants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[MariadbConstants.DATABASE_KEY] || {},{RDBMS: MariadbConstants.DATABASE_KEY}))
	   return this.#_YADAMU_DBI_PARAMETERS
    }
   
    get YADAMU_DBI_PARAMETERS() {
      return MariadbQA.YADAMU_DBI_PARAMETERS
    }	
			    
	constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters)
    }

    async recreateSchema() {
        
      try {
        const dropUser = `drop schema if exists "${this.parameters.TO_USER}"`;
        await this.executeSQL(dropUser,{});      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
      const createUser = `create schema "${this.parameters.TO_USER}"`;
      await this.executeSQL(createUser,{});      
    }   
	
    async compareSchemas(source,target,rules) {

      const report = {
        successful : []
       ,failed     : []
      }

      const compareRules =  JSON.stringify(this.yadamu.getCompareRules(rules))
      let results = await this.executeSQL(MariadbQA.SQL_COMPARE_SCHEMAS,[source.schema,target.schema,compareRules])

      const successful = await this.executeSQL(MariadbQA.SQL_SUCCESS,{})
      report.successful = successful
     
      const failed = await this.executeSQL(MariadbQA.SQL_FAILED,{})
      report.failed = failed

      return report
    }

    async getRowCounts(target) {

      const results = await this.executeSQL(MariadbQA.SQL_SCHEMA_TABLE_ROWS,[target.schema]);
      return results
      
    }
    
	classFactory(yadamu) {
      return new MariadbQA(yadamu,this)
    }
	
	async scheduleTermination(pid,workerId) {
	  const tags = this.getTerminationTags(workerId,pid)
	  this.yadamuLogger.qa(tags,`Termination Scheduled.`);
	  const timer = setTimeout(
        async (pid) => {
          if (this.pool !== undefined && this.pool.end) {
		    this.yadamuLogger.log(tags,`Killing connection.`);
	        const conn = await this.getConnectionFromPool();
			const sqlStatement = `kill hard ${pid}`
			const res = await conn.query(sqlStatement);
			await conn.release()
		  }
		  else {
		    this.yadamuLogger.log(tags,`Unable to Kill Connection: Connection Pool no longer available.`);
		  }
		},
		this.killConfiguration.delay,
	    pid
      )
	  timer.unref()
	}	

    verifyStagingSource(source) {  
      super.verifyStagingSource(MariadbConstants.STAGED_DATA_SOURCES,source)
    } 
}

export { MariadbQA as default }

const _SQL_SUCCESS =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
   and MISSING_ROWS = 0
   and EXTRA_ROWS = 0
   and NOTES is NULL
order by TABLE_NAME`;

const _SQL_FAILED = 
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, NOTES
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
    or MISSING_ROWS <> 0
    or EXTRA_ROWS <> 0
    or NOTES is NOT NULL
 order by TABLE_NAME`;

const _SQL_SCHEMA_TABLE_ROWS = `select TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = ?`;

const _SQL_COMPARE_SCHEMAS =  `CALL COMPARE_SCHEMAS(?,?,?);`;

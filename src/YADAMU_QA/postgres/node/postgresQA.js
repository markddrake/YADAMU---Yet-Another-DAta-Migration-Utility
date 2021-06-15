"use strict" 

const PostgresDBI = require('../../../YADAMU/postgres/node/postgresDBI.js');
const PostgresError = require('../../../YADAMU/postgres/node/postgresException.js')
const PostgresConstants = require('../../../YADAMU/postgres/node/postgresConstants.js');

const YadamuTest = require('../../common/node/yadamuTest.js');


class PostgresQA extends PostgresDBI {
    
    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }

	static #_YADAMU_DBI_PARAMETERS
	
	static get YADAMU_DBI_PARAMETERS()  { 
	   this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,PostgresConstants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[PostgresConstants.DATABASE_KEY] || {},{RDBMS: PostgresConstants.DATABASE_KEY}))
	   return this.#_YADAMU_DBI_PARAMETERS
    }
   
    get YADAMU_DBI_PARAMETERS() {
      return PostgresQA.YADAMU_DBI_PARAMETERS
    }	
		
    constructor(yadamu,settings,parameters) {
       super(yadamu,settings,parameters);
    }
    
    setMetadata(metadata) {
      super.setMetadata(metadata)
    }
	 
	async initialize() {
	  await super.initialize();
	  if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	  }
	  if (this.terminateConnection()) {
        const pid = await this.getConnectionID();
	    this.scheduleTermination(pid,this.getWorkerNumber());
	  }
	}
	
	async recreateSchema() {
      try {
        const dropSchema = `drop schema if exists "${this.parameters.TO_USER}" cascade`;
        this.status.sqlTrace.write(`${dropSchema};\n--\n`)
        await this.executeSQL(dropSchema);      
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
		
    async workerDBI(idx)  {
	  const workerDBI = await super.workerDBI(idx);
      // Manager needs to schedule termination of worker.
	  if (this.terminateConnection(idx)) {
        const pid = await workerDBI.getConnectionID();
	    this.scheduleTermination(pid,idx);
	  }
	  return workerDBI
    }

	async scheduleTermination(pid,workerId) {
      this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Termination Scheduled.`);
	  const timer = setTimeout(
        async (pid) => {
          try {
		    if (this.pool !== undefined && this.pool.end) {
		      this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Killing connection.`);
	          const conn = await this.getConnectionFromPool();
		      const res = await conn.query(`select pg_terminate_backend(${pid})`);
		      await conn.release()
		    }
		    else {
		      this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Unable to Kill Connection: Connection Pool no longer available.`);
		    }
           } catch (e) {
             console.log(e);
           }
		},
		this.killConfiguration.delay,
	    pid
      )
	  timer.unref()
	}
}

module.exports = PostgresQA

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

const _SQL_SCHEMA_TABLE_ROWS = `select schemaname, relname, n_live_tup from pg_stat_user_tables where schemaname = $1`;

const _SQL_COMPARE_SCHEMAS = `call COMPARE_SCHEMA($1,$2,$3)`
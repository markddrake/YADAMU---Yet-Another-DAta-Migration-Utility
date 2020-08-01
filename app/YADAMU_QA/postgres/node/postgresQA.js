"use strict" 

const PostgresDBI = require('../../../YADAMU/postgres/node/postgresDBI.js');

class PostgresQA extends PostgresDBI {
    
    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }

    constructor(yadamu) {
       super(yadamu);
    }
    
    async recreateSchema() {
      try {
        const dropSchema = `drop schema if exists "${this.parameters.TO_USER}" cascade`;
        if (this.status.sqlTrace) {
          this.status.sqlTrace.write(`${dropSchema};\n--\n`)
        }
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

	async scheduleTermination(pid) {
	  const killOperation = this.parameters.KILL_READER_AFTER ? 'Reader'  : 'Writer'
	  const killDelay = this.parameters.KILL_READER_AFTER ? this.parameters.KILL_READER_AFTER  : this.parameters.KILL_WRITER_AFTER
	  const timer = setTimeout(async (pid) => {
		   if (this.pool !== undefined && this.pool.end) {
		     this.yadamuLogger.qa(['KILL',this.yadamu.parameters.ON_ERROR,this.DATABASE_VENDOR,killOperation,killDelay,pid,this.getWorkerNumber()],`Killing connection.`);
	         const conn = await this.getConnectionFromPool();
		     const res = await conn.query(`select pg_terminate_backend(${pid})`);
		     await conn.release()
		   }
		   else {
		     this.yadamuLogger.qa(['KILL',this.yadamu.parameters.ON_ERROR,this.DATABASE_VENDOR,killOperation,killDelay,pid,this.getWorkerNumber()],`Unable to Kill Connection: Connection Pool no longer available.`);
		   }
		},
		killDelay,
	    pid
      )
	  timer.unref()
	}
	
	async initialize() {
	  await super.initialize();
	  if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	  }
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

      await this.executeSQL(PostgresQA.SQL_COMPARE_SCHEMAS,[source.schema,target.schema,this.parameters.EMPTY_STRING_IS_NULL === true,this.parameters.STRIP_XML_DECLARATION === true, this.parameters.hasOwnProperty('SPATIAL_PRECISION') ? this.parameters.SPATIAL_PRECISION : 18])      
      
      const successful = await this.executeSQL(PostgresQA.SQL_SUCCESS)            
      report.successful = successful.rows
      
      const failed = await this.executeSQL(PostgresQA.SQL_FAILED)
      report.failed = failed.rows

      return report
    }
	
	async getRowCounts(target) {
        
      const results = await this.executeSQL(PostgresQA.SQL_SCHEMA_TABLE_ROWS,[target.schema]);
      return results.rows
    }    
	
  async workerDBI(idx)  {
	const workerDBI = await super.workerDBI(idx);
	if (workerDBI.testLostConnection()) {
	  const dbiID = await workerDBI.getConnectionID();
	  this.scheduleTermination(dbiID);
    }
	return workerDBI
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

const _SQL_COMPARE_SCHEMAS = `call COMPARE_SCHEMA($1,$2,$3,$4,$5)`
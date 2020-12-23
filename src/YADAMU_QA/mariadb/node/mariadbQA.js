"use strict" 
const MariadbDBI = require('../../../YADAMU/mariadb/node/mariadbDBI.js');

class MariadbQA extends MariadbDBI {
    
    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }

	constructor(yadamu) {
       super(yadamu)
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
	
	async scheduleTermination(pid,workerId) {
	  const killOperation = this.parameters.KILL_READER_AFTER ? 'Reader'  : 'Writer'
	  const killDelay = this.parameters.KILL_READER_AFTER ? this.parameters.KILL_READER_AFTER  : this.parameters.KILL_WRITER_AFTER
	  const timer = setTimeout(async (pid) => {
          if (this.pool !== undefined && this.pool.end) {
		    this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,killOperation,workerId,killDelay,pid],`Killing connection.`);
	        const conn = await this.getConnectionFromPool();
			const sqlStatement = `kill hard ${pid}`
		    const res = await conn.query(sqlStatement);
			if (res.affectedRows === 0) {
 		      this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,killOperation,workerId,killDelay,pid],`Results sent prior to termination.`)
            }
		    await conn.release()
		  }
		  else {
		    this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,killOperation,workerId,killDelay,pid],`Unable to Kill Connection: Connection Pool no longer available.`);
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
	  if (this.enableLostConnectionTest()) {
		const dbiID = await this.getConnectionID();
		this.scheduleTermination(dbiID,this.getWorkerNumber());
	  }
	}
 
    async compareSchemas(source,target) {

      const report = {
        successful : []
       ,failed     : []
      }

      let results = await this.executeSQL(MariadbQA.SQL_COMPARE_SCHEMAS,[source.schema,target.schema,this.parameters.EMPTY_STRING_IS_NULL === true, this.parameters.hasOwnProperty('SPATIAL_PRECISION') ? this.parameters.SPATIAL_PRECISION : 18]);

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
    
  async workerDBI(idx)  {
	const workerDBI = await super.workerDBI(idx);
	if (workerDBI.enableLostConnectionTest()) {
	  const dbiID = await workerDBI.getConnectionID();
	  this.scheduleTermination(dbiID,workerDBI.getWorkerNumber());
    }
	return workerDBI
  }
      
}

module.exports = MariadbQA

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

const _SQL_COMPARE_SCHEMAS =  `CALL COMPARE_SCHEMAS(?,?,?, ?);`;

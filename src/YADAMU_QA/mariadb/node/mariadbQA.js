"use strict" 

const MariadbDBI = require('../../../YADAMU/mariadb/node/mariadbDBI.js');
const MariadbError = require('../../../YADAMU/mariadb/node/mariadbException.js')
const MariadbConstants = require('../../../YADAMU/mariadb/node/mariadbConstants.js');

const YadamuTest = require('../../common/node/yadamuTest.js');

class MariadbQA extends MariadbDBI {

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
			    
	constructor(yadamu) {
       super(yadamu)
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

      const compareRules = {
	    emptyStringisNull   : rules.EMPTY_STRING_IS_NULL 
	  ,	spatialPrecision    : rules.SPATIAL_PRECISION || 18
	  , orderedJSON         : rules.ORDERED_JSON
      }
	 
      let results = await this.executeSQL(MariadbQA.SQL_COMPARE_SCHEMAS,[source.schema,target.schema,JSON.stringify(compareRules)])

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
          if (this.pool !== undefined && this.pool.end) {
		    this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Killing connection.`);
	        const conn = await this.getConnectionFromPool();
			const sqlStatement = `kill hard ${pid}`
			const res = await conn.query(sqlStatement);
			await conn.release()
		  }
		  else {
		    this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Unable to Kill Connection: Connection Pool no longer available.`);
		  }
		},
		this.killConfiguration.delay,
	    pid
      )
	  timer.unref()
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

const _SQL_COMPARE_SCHEMAS =  `CALL COMPARE_SCHEMAS(?,?,?);`;

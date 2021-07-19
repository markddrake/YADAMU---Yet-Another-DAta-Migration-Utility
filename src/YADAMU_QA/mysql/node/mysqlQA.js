"use strict" 

const MySQLDBI = require('../../../YADAMU/mysql/node/mysqlDBI.js');
const MySQLError = require('../../../YADAMU/mysql/node/mysqlException.js')
const MySQLConstants = require('../../../YADAMU/mysql/node/mysqlConstants.js');

const YadamuTest = require('../../common/node/yadamuTest.js');

class MySQLQA extends MySQLDBI {

    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }

	static #_YADAMU_DBI_PARAMETERS
	
	static get YADAMU_DBI_PARAMETERS()  { 
	   this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,MySQLConstants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[MySQLConstants.DATABASE_KEY] || {},{RDBMS: MySQLConstants.DATABASE_KEY}))
	   return this.#_YADAMU_DBI_PARAMETERS
    }
   
    get YADAMU_DBI_PARAMETERS() {
      return MySQLQA.YADAMU_DBI_PARAMETERS
    }	
			
    constructor(yadamu,settings,parameters) {
       super(yadamu,settings,parameters)
    }
	
    setMetadata(metadata) {
      super.setMetadata(metadata)
    }

    doTimeout(milliseconds) {
		
	  // Overdide YadamuDBI. No Messages
      
	  return new Promise((resolve,reject) => {
        setTimeout(
          () => {
           resolve();
          },
          milliseconds
        )
      })  
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
	
    // ### Hack to avoid missng rows when using a new connection to read previously written rows using new connection immediately after closing current connection.....'

	async finalizeImport() {
	  await this.doTimeout(1000);
	  super.finalizeImport()
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
		    const res = await conn.query(`kill ${pid}`);
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

    verifyStagingSource(source) {  
      super.verifyStagingSource(MySQLConstants.STAGED_DATA_SOURCES,source)
    } 
	
}

module.exports = MySQLQA

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

"use strict" 

const SnowflakeDBI = require('../../../YADAMU/snowflake/node/snowflakeDBI.js');
const SnowflakeConstants = require('../../../YADAMU/snowflake/node/snowflakeConstants.js');
const SnowflakeException = require('../../../YADAMU/snowflake/node/snowflakeException.js')

const YadamuTest = require('../../common/node/yadamuTest.js');

class SnowflakeQA extends SnowflakeDBI {
    
    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }

	static #_YADAMU_DBI_PARAMETERS
	
	static get YADAMU_DBI_PARAMETERS()  { 
	   this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,SnowflakeConstants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[SnowflakeConstants.DATABASE_KEY] || {},{RDBMS: SnowflakeConstants.DATABASE_KEY}))
	   return this.#_YADAMU_DBI_PARAMETERS
    }
   
    get YADAMU_DBI_PARAMETERS() {
      return SnowflakeQA.YADAMU_DBI_PARAMETERS
    }	
		
    constructor(yadamu) {
       super(yadamu)
    }
    
	execute(conn,sqlStatement) {
    
    return new Promise((resolve,reject) => {

      this.status.sqlTrace.write(this.traceSQL(sqlStatement));

	  const stack = new Error().stack;
	  conn.execute({
        sqlText        : sqlStatement
      , binds          : []
	  , fetchAsString  : ['Number','Date','JSON']
      , complete       : async (err,statement,rows) => {
		                   if (err) {
              		         const cause = this.trackExceptions(new SnowflakeException(err,stack,sqlStatement))
    		                 reject(cause);
                           }
					       // this.traceTiming(sqlStartTime,sqlEndTime)
                           resolve(rows);
				         }    
      })
    })
  }  

    
    async scheduleTermination(pid,workerId) {
      this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Termination Scheduled.`);
      const timer = setTimeout(async (pid) => {
          this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Killing connection.`);
          const conn = await this.getConnectionFromPool();
          const sqlStatement = `select SYSTEM$ABORT_SESSION( ${pid} );`
          let stack
          try {
            stack = new Error().stack
            const res = await this.execute(conn,sqlStatement)
            await conn.destroy()
          } catch (e) {
            const cause = new SnowflakeException(e,stack,sqlStatement)
            this.yadamuLogger.handleException(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],cause)
          }
	    },
        this.killConfiguration.delay,
        pid
      )
      timer.unref()
    }
    
    
    async initialize() {
      if (this.options.recreateSchema === true) {
        await this.recreateSchema();
      }
      await super.initialize();
	  if (this.terminateConnection()) {
        const pid = await this.getConnectionID();
	    this.scheduleTermination(pid,this.getWorkerNumber());
	  }
    }

    async recreateSchema() {
        
      const database = this.connectionProperties.database;
      const YADAMU_DATABASE = this.parameters.YADAMU_DATABASE;
      delete this.parameters.YADAMU_DATABASE;
      
      this.connectionProperties.database = '';
      await this.createConnectionPool(); 
      this.connection = await this.getConnectionFromPool();
        
      let results
      try {
        const createDatabase = `create transient database if not exists "${YADAMU_DATABASE}" DATA_RETENTION_TIME_IN_DAYS = 0;`;
        results =  await this.executeSQL(createDatabase,[]);      
        const useDatabase = `USE DATABASE "${YADAMU_DATABASE}";`;
        results =  await this.executeSQL(useDatabase,[]);      
        const dropSchema = `drop schema if exists "${YADAMU_DATABASE}"."${this.parameters.TO_USER}";`;
        results =  await this.executeSQL(dropSchema,[]);      
        const createSchema = `create transient schema "${YADAMU_DATABASE}"."${this.parameters.TO_USER}" DATA_RETENTION_TIME_IN_DAYS=0;`;
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
      this.parameters.YADAMU_DATABASE = YADAMU_DATABASE
      this.connectionProperties.database = database;
    }   

    async getRowCounts(target) {
      
      const results = await this.executeSQL(SnowflakeQA.SQL_SCHEMA_TABLE_ROWS,[target.schema]); 
      return results.map((row,idx) => {          
        return [target.schema,row.TABLE_NAME,row.ROW_COUNT]
      })
    }
    
    async compareSchemas(source,target,rules) {
     
      const useDatabase = `USE DATABASE "${source.database}";`;
      let results =  await this.executeSQL(useDatabase,[]);      
         
      const report = {
        successful : []
       ,failed     : []
      }

      results = await this.executeSQL(SnowflakeQA.SQL_COMPARE_SCHEMAS,[source.database,source.schema,target.schema,rules.EMPTY_STRING_IS_NULL === true,rules.TIMESTAMP_PRECISION || 9]);
	 
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

   async workerDBI(idx)  {
	  const workerDBI = await super.workerDBI(idx);
      // Manager needs to schedule termination of worker.
	  if (this.terminateConnection(idx)) {
        const pid = await workerDBI.getConnectionID();
	    this.scheduleTermination(pid,idx);
	  }
	  return workerDBI
    }      
}

module.exports = SnowflakeQA

const _SQL_COMPARE_SCHEMAS = `call YADAMU_SYSTEM.PUBLIC.COMPARE_SCHEMAS(:1,:2,:3,:4,:5);`
const _SQL_SCHEMA_TABLE_ROWS = `select TABLE_NAME, ROW_COUNT from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = 'BASE TABLE' and TABLE_SCHEMA = ?`;


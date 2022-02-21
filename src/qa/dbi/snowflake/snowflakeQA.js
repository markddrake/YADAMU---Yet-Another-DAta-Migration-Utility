"use strict" 

import SnowflakeDBI       from '../../../YADAMU/snowflake/node/snowflakeDBI.js';
import SnowflakeConstants from '../../../YADAMU/snowflake/node/snowflakeConstants.js';
import SnowflakeException from '../../../YADAMU/snowflake/node/snowflakeException.js'

import YadamuTest         from '../../common/node/yadamuTest.js';
import YadamuQALibrary    from '../../common/node/yadamuQALibrary.js'

class SnowflakeQA extends YadamuQALibrary.qaMixin(SnowflakeDBI) {
    
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
		
    constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters)
    }

    async initializeImport() {
	  if (this.options.recreateSchema === true) {
		await this.recreateDatabase();
		this.options.recreateSchema = false
	  }
	  await super.initializeImport();
    }	

	async recreateDatabase() {

      try {	
	    const connectionProperties = Object.assign({},this.vendorProperties)
	    const dbi = new SnowflakeMgr(this.yadamuLogger,this.status, connectionProperties)
	    await dbi.recreateDatabase(this.parameters.YADAMU_DATABASE,this.parameters.TO_USER)
	  }	catch (e) {
		console.log(e)
      }
	}

    async getRowCounts(target) {

      const useDatabase = `USE DATABASE "${target.database}";`;	  
      let results =  await this.executeSQL(useDatabase,[]);      
         
      results = await this.executeSQL(SnowflakeQA.SQL_SCHEMA_TABLE_ROWS,[target.schema]); 
      return results.map((row,idx) => {          
        return [target.schema,row.TABLE_NAME,row.ROW_COUNT]
      })
    }
    
    async compareSchemas(source,target,rules) {
    
      const compareRules = JSON.stringify(this.yadamu.getCompareRules(rules))  
 	
      const useDatabase = `USE DATABASE "${source.database}";`;
      let results =  await this.executeSQL(useDatabase,[]);      
         
      const report = {
        successful : []
       ,failed     : []
      }

      results = await this.executeSQL(SnowflakeQA.SQL_COMPARE_SCHEMAS,[source.database,source.schema,target.schema,compareRules]);

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

    classFactory(yadamu) {
      return new SnowflakeQA(yadamu,this)
    }
	
    async scheduleTermination(pid,workerId) {
	  const tags = this.getTerminationTags(workerId,pid)
	  this.yadamuLogger.qa(tags,`Termination Scheduled.`);
      const timer = setTimeout(async (pid) => {
          this.yadamuLogger.log(tags,`Killing connection.`);
          const conn = await this.getConnectionFromPool();
          const sqlStatement = `select SYSTEM$ABORT_SESSION( ${pid} );`
          let stack
          try {
            stack = new Error().stack
            const res = await this.execute(conn,sqlStatement)
            await conn.destroy()
          } catch (e) {
            const cause = new SnowflakeError(this.DRIVER_ID,e,stack,sqlStatement)
            this.yadamuLogger.handleException(tags,cause)
          }
	    },
        this.killConfiguration.delay,
        pid
      )
      timer.unref()
    }

    verifyStagingSource(source) {  
      super.verifyStagingSource(SnowflakeConstants.STAGED_DATA_SOURCES,source)
    }       
}

class SnowflakeMgr extends SnowflakeDBI {
	
	constructor(logger,status,vendorProperties) {
	  super({})
	  this.yadamuLogger = logger;
  	  this.status = status
	  this.vendorProperties = vendorProperties
      this.vendorProperties.database = '';
	}
	
    async initialize() {
      await this._getDatabaseConnection()
    }
  
    async recreateDatabase(database,schema) {
		     
      this.vendorProperties.database = '';
      await this.createConnectionPool(); 
      this.connection = await this.getConnectionFromPool();
        
      let results
      try {
        const createDatabase = `create transient database if not exists "${database}" DATA_RETENTION_TIME_IN_DAYS = 0`;
        results =  await this.executeSQL(createDatabase,[]);      
        const useDatabase = `USE DATABASE "${database}"`;
        results =  await this.executeSQL(useDatabase,[]);      
        const dropSchema = `drop schema if exists "${database}"."${schema}"`;
        results =  await this.executeSQL(dropSchema,[]);      
        const createSchema = `create transient schema "${database}"."${schema}" DATA_RETENTION_TIME_IN_DAYS=0`;
        results =  await this.executeSQL(createSchema,[]);      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
	}
}  

export { SnowflakeQA as default }

const _SQL_COMPARE_SCHEMAS = `call YADAMU_SYSTEM.PUBLIC.COMPARE_SCHEMAS(:1,:2,:3,:4);`
const _SQL_SCHEMA_TABLE_ROWS = `select TABLE_NAME, ROW_COUNT from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = 'BASE TABLE' and TABLE_SCHEMA = ?`;


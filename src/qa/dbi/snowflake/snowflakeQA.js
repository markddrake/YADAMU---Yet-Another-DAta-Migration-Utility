
import {
  setTimeout 
}                         from "timers/promises"

import SnowflakeDBI       from '../../../node/dbi//snowflake/snowflakeDBI.js';
import SnowflakeConstants from '../../../node/dbi//snowflake/snowflakeConstants.js';
import SnowflakeException from '../../../node/dbi//snowflake/snowflakeException.js'

import Yadamu             from '../../core/yadamu.js';
import YadamuQALibrary    from '../../lib/yadamuQALibrary.js'

class SnowflakeQA extends YadamuQALibrary.qaMixin(SnowflakeDBI) {
    
	static #_DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,SnowflakeConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[SnowflakeConstants.DATABASE_KEY] || {},{RDBMS: SnowflakeConstants.DATABASE_KEY}))
	   return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return SnowflakeQA.DBI_PARAMETERS
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
	    const vendorProperties = Object.assign({},this.vendorProperties)
	    const dbi = new SnowflakeMgr(this.yadamu, vendorProperties)
	    await dbi.recreateDatabase(this.parameters.YADAMU_DATABASE,this.parameters.TO_USER)
	  }	catch (e) {
        this.yadamu.LOGGER.handleException([this.DATABASE_VENDOR,'RECREATE DATABASE',this.parameters.TO_USER],e);
      }
	}


   execute(conn,sqlStatement) {
    
    return new Promise((resolve,reject) => {

      this.SQL_TRACE.traceSQL(sqlStatement);

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
      return new SnowflakeQA(yadamu,this,this.connectionSettings,this.parameters)
    }
	
    async scheduleTermination(pid,workerId) {
      let stack
	  const operation = `select SYSTEM$ABORT_SESSION( ${pid} );`
	  const tags = this.getTerminationTags(workerId,pid)
	  this.LOGGER.qa(tags,`Termination Scheduled.`);
      setTimeout(this.yadamu.KILL_DELAY,pid,{ref : false}).then(async (pid) => {
        this.LOGGER.log(tags,`Killing connection.`);
        stack = new Error().stack
        const conn = await this.getConnectionFromPool();
        const res = await this.execute(conn,operation)
        await this.final()
      }).catch(async (e) => {
        this.yadamu.LOGGER.handleException(tags,new SnowflakeError(this.DRIVER_ID,e,stack,operation));
		await this.destroy(e)
      })
    }
	
}

class SnowflakeMgr extends SnowflakeDBI {
	
	constructor(yadamu,vendorProperties) {
	  super(yadamu)
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
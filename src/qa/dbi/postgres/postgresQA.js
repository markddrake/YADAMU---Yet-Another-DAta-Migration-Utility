
import {
  setTimeout 
}                        from "timers/promises"

import PostgresDBI       from '../../../node/dbi//postgres/postgresDBI.js';
import PostgresError     from '../../../node/dbi//postgres/postgresException.js'
import PostgresConstants from '../../../node/dbi//postgres/postgresConstants.js';

import Yadamu            from '../../core/yadamu.js';
import YadamuQALibrary   from '../../lib/yadamuQALibrary.js'


class PostgresQA extends YadamuQALibrary.qaMixin(PostgresDBI) {
    
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

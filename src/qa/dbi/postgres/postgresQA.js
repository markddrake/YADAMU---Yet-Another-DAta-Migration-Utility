
import {
  setTimeout 
}                        from "timers/promises"

import PostgresDBI       from '../../../node/dbi//postgres/postgresDBI.js';
import PostgresError     from '../../../node/dbi//postgres/postgresException.js'
import PostgresConstants from '../../../node/dbi//postgres/postgresConstants.js';

import Yadamu            from '../../core/yadamu.js';
import YadamuQALibrary   from '../../lib/yadamuQALibrary.js'


class PostgresQA extends YadamuQALibrary.qaMixin(PostgresDBI) {
    
	static #DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,PostgresConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[PostgresConstants.DATABASE_KEY] || {},{RDBMS: PostgresConstants.DATABASE_KEY}))
	   return this.#DBI_PARAMETERS
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
	  this.LOGGER.qa(tags,`Termination Scheduled.`);
	  setTimeout(this.yadamu.KILL_DELAY,pid,{ref : false}).then(async (pid) => {
        if (this.pool !== undefined && this.pool.end) {
	      stack = new Error().stack
		  this.LOGGER.log(tags,`Killing connection.`);
	      const conn = await this.getConnectionFromPool();
		  // The pool hands back an idle client, which is the target's own connection once its worker has finished and released it.
		  // Killing it would kill this connection, and the kill would never reach the worker. Release the client whatever happens,
		  // or pool.end() waits for it forever.
		  try {
		    const self = await conn.query(`select pg_backend_pid()`)
		    if (Number(Object.values(self.rows[0])[0]) === Number(pid)) {
		      this.LOGGER.log(tags,`Target connection already returned to the pool. Kill skipped.`);
		      return
		    }
		    const res = await conn.query(operation);
		  } finally {
		    await conn.release()
		  }
		}
		else {
		  this.LOGGER.log(tags,`Unable to Kill Connection: Connection Pool no longer available.`);
		}
      }).catch((e) => {
		const cause = this.createDatabaseError(e,stack,operation)
        this.LOGGER.handleException(tags,cause)
      })
	}

}

export { PostgresQA as default }

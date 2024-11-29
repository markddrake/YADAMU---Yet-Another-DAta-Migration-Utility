"use strict" 

import {
  setTimeout 
}                         from "timers/promises"

import CockroachError     from '../../../node/dbi/postgres/postgresException.js'

import CockroachDBI       from '../../../node/dbi/cockroach/cockroachDBI.js';
import CockroachConstants from '../../../node/dbi/cockroach/cockroachConstants.js';

import Yadamu             from '../../core/yadamu.js';
import YadamuQALibrary    from '../../lib/yadamuQALibrary.js'


class CockroachQA extends YadamuQALibrary.qaMixin(CockroachDBI) {
    
	static #DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,CockroachConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[CockroachConstants.DATABASE_KEY] || {},{RDBMS: CockroachConstants.DATABASE_KEY}))
	   return this.#DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return CockroachQA.DBI_PARAMETERS
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
      return new CockroachQA(yadamu,this,this.connectionParameters,this.parameters)
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
		  const res = await conn.query(operation);
		  await conn.release()
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

export { CockroachQA as default }


import {
  setTimeout 
}                       from "timers/promises"

import MariadbDBI       from '../../../node/dbi//mariadb/mariadbDBI.js';
import MariadbError     from '../../../node/dbi//mariadb/mariadbException.js'
import MariadbConstants from '../../../node/dbi//mariadb/mariadbConstants.js';

import Yadamu           from '../../core/yadamu.js';
import YadamuQALibrary  from '../../lib/yadamuQALibrary.js'

class MariadbQA extends YadamuQALibrary.qaMixin(MariadbDBI) {

	static #_DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,MariadbConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[MariadbConstants.DATABASE_KEY] || {},{RDBMS: MariadbConstants.DATABASE_KEY}))
	   return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return MariadbQA.DBI_PARAMETERS
    }	
			    
	constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters)
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
	
	classFactory(yadamu) {
      return new MariadbQA(yadamu,this,this.connectionParameters,this.parameters)
    }
	
	async scheduleTermination(pid,workerId) {
	  let stack
	  const operation = `kill hard ${pid}`
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
        this.yadamu.LOGGER.handleException(tags,new MariadbError(this.DRIVER_ID,e,stack,operation));
      })
	}	

}

export { MariadbQA as default }

"use strict" 

import {
  setTimeout 
}                        from "timers/promises"

import DB2DBI       from '../../../node/dbi//db2/db2DBI.js';
import DB2Error     from '../../../node/dbi//db2/db2Exception.js'
import DB2Constants from '../../../node/dbi//db2/db2Constants.js';

import Yadamu            from '../../core/yadamu.js';
import YadamuQALibrary   from '../../lib/yadamuQALibrary.js'


class DB2QA extends YadamuQALibrary.qaMixin(DB2DBI) {

	static #DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,DB2Constants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[DB2Constants.DATABASE_KEY] || {},{RDBMS: DB2Constants.DATABASE_KEY}))
	   return this.#DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return DB2QA.DBI_PARAMETERS
    }	
		
    constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters);
    }

 	async recreateSchema() {
      try {
        const dropSchema = `BEGIN DECLARE V_STATEMENT VARCHAR(300) DEFAULT 'drop schema "${this.parameters.TO_USER}" RESTRICT'; DECLARE CONTINUE HANDLER FOR SQLSTATE '42704' BEGIN  END;  FOR D AS SELECT 'DROP TABLE "${this.parameters.TO_USER}"."' || TABNAME || '"' AS DROP_TABLE_STATEMENT FROM SYSCAT.TABLES WHERE TABSCHEMA = '${this.parameters.TO_USER}' AND TYPE = 'T' DO EXECUTE IMMEDIATE D.DROP_TABLE_STATEMENT; END FOR; EXECUTE IMMEDIATE V_STATEMENT; END;`
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
      return new DB2QA(yadamu,this,this.connectionParameters,this.parameters)
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
		const cause = this.createDatabaseError(this.DRIVER_ID,e,stack,operation)
        this.LOGGER.handleException(tags,cause)
      })
	}

}

export { DB2QA as default }

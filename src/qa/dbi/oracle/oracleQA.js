
import {
  setTimeout 
}                      from "timers/promises"

import oracledb        from 'oracledb';

import OracleDBI       from '../../../node/dbi//oracle/oracleDBI.js';

import { 
  OracleError 
}                      from '../../../node/dbi//oracle/oracleException.js'
import OracleConstants from '../../../node/dbi//oracle/oracleConstants.js';

import Yadamu              from '../../core/yadamu.js';
import YadamuQALibrary from '../../lib/yadamuQALibrary.js'

class OracleQA extends YadamuQALibrary.qaMixin(OracleDBI) {
    			
    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }
    static get SQL_GATHER_SCHEMA_STATS()   { return _SQL_GATHER_SCHEMA_STATS }

	static #_DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,OracleConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[OracleConstants.DATABASE_KEY] || {},{RDBMS: OracleConstants.DATABASE_KEY}))
	   return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
	  return OracleQA.DBI_PARAMETERS
    }	
	
    constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters)
    }

 	async recreateSchema() {
        
	 let retryCount = 5
	 while (true) {
        try {
          const dropUser = `drop user "${this.parameters.TO_USER}" cascade`;
          await this.executeSQL(dropUser,{});      
		  break;
        } catch (e) {
		  if ((e instanceof OracleError) && e.lockingError() && (retryCount > 0)) {
			await setTimeout(1000)
			retryCount--
			continue
		 }
         if (e.nonexistentUser()) {
     		break;
		 }
		 throw e
       }
	 }
     const createUser = `grant connect, resource, unlimited tablespace to "${this.parameters.TO_USER}" identified by ${this.vendorProperties.password}`;
     await this.executeSQL(createUser,{});      
   }  
     
   classFactory(yadamu) {
     return new OracleQA(yadamu,this,this.connectionParameters,this.parameters)
   }
	
   async configureConnection() {

      await super.configureConnection()
	  
	  const configOptions = []

	  if (this.isManager()) {
		  
		configOptions.push(['VARCHAR2 length',this.EXTENDED_STRING ? 32767 : 4000])
        
        if (this.MAX_STRING_SIZE <= OracleConstants.VARCHAR_MAX_SIZE_EXTENDED) {
		  configOptions.push(['JSON String Length',this.MAX_STRING_SIZE])
        }
        
        if (this.XMLTYPE_STORAGE_CLAUSE !== this.XMLTYPE_STORAGE_MODEL ) {
 		  configOptions.push(['XMLType',this.XMLTYPE_STORAGE_CLAUSE])
        }
	  
		configOptions.push(['JSON',this.DATA_TYPES.storageOptions.JSON_TYPE])
		configOptions.push(['BOOLEAN',this.DATA_TYPES.storageOptions.BOOLEAN_TYPE])
	    
		this.LOGGER.info([this.DATABASE_VENDOR,this.DATABASE_VERSION,`Configuration`],`Storage Options: ${configOptions.map((option) => { return `"${option[0]}":"${option[1]}"`}).join(', ')}`)
      }
    }
  
    async scheduleTermination(pid,workerId) {
	  const tags = this.getTerminationTags(workerId,`${pid.sid},${pid.serial}`)
	  this.LOGGER.qa(tags,`Termination Scheduled.`);
      setTimeout(this.yadamu.KILL_DELAY,pid,{ref: false}).then(async (pid) => {
        if ((this.pool instanceof this.oracledb.Pool) && (this.pool.status === this.oracledb.POOL_STATUS_OPEN)) {
	      this.LOGGER.log(tags,`Killing connection.`);
		  const conn = await this.getConnection();
		  const sqlStatement = `ALTER SYSTEM KILL SESSION '${pid.sid}, ${pid.serial}'`
		  let stack
		  try {
			stack = new Error().stack
	        const res = await conn.execute(sqlStatement);
 		    await conn.close()
		  } catch (e) {
			if ((e.errorNum && ((e.errorNum === 27) || (e.errorNum === 31))) || (e.message.startsWith('DPI-1010'))) {
			  // The Worker has finished and it's SID and SERIAL# appears to have been assigned to the connection being used to issue the KILLL SESSION and you can't kill yourself (Error 27)
			  this.LOGGER.log(tags,`Worker finished prior to termination.`)
 		    }
			else {
			  const cause = this.createDatabaseError(this.DRIVER_ID,e,stack,sqlStatement)
			  this.LOGGER.handleException(tags,cause)
			}
            if (!e.message.startsWith('DPI-1010')) {
              try {
     		    await conn.close()
	          } catch (closeError) {
                closeError.cause = e;
 			    this.LOGGER.handleException(tags,cause)
              }
		    }    
	      }
		}
		else {
		  this.LOGGER.log(tags,`Unable to Kill Connection: Connection Pool no longer available.`);
		}
      })
    }
}
	
export { OracleQA as default }

const _SQL_COMPARE_SCHEMAS = `begin YADAMU_COMPARE.COMPARE_SCHEMAS(:P_SOURCE_SCHEMA, :P_TARGET_SCHEMA, :P_RULES); end;`;

const _SQL_SUCCESS =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'SUCCESSFUL', TARGET_ROW_COUNT
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
   and MISSING_ROWS = 0
   and EXTRA_ROWS = 0
   and SQLERRM is NULL
order by TABLE_NAME`;

const _SQL_FAILED = 
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'FAILED', SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, SQLERRM
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
    or MISSING_ROWS <> 0
    or EXTRA_ROWS <> 0
    or SQLERRM is NOT NULL
 order by TABLE_NAME`;

const _SQL_GATHER_SCHEMA_STATS = `begin dbms_stats.gather_schema_stats(ownname => :target); end;`;

// LEFT Join works in 11.x databases where 'EXTERNAL' column does not exist in ALL_TABLES

const _SQL_SCHEMA_TABLE_ROWS = `select att.TABLE_NAME, NUM_ROWS 
                              from ALL_TABLES att 
							  LEFT JOIN ALL_EXTERNAL_TABlES axt 
							         on att.OWNER = axt.OWNER and att.TABLE_NAME = axt.TABLE_NAME 
					    where att.OWNER = :target 
						  and axt.OWNER is NULL 
						  and att.SECONDARY = 'N' 
						  and att.DROPPED = 'NO'
                          and att.TEMPORARY = 'N'
                          and att.NESTED = 'NO'
						  and (att.IOT_TYPE is NULL or att.IOT_TYPE = 'IOT')`;
						  




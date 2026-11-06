
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

  // Lets a worker thread load this QA class.
  get WORKER_MODULE_URL()     { return import.meta.url }
    			
    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }
    static get SQL_GATHER_SCHEMA_STATS()   { return _SQL_GATHER_SCHEMA_STATS }
    static get SQL_SHARED_POOL_HEADROOM()  { return _SQL_SHARED_POOL_HEADROOM }
    // Flush the shared pool after a DDL extraction when the SGA can give it less than this.
    static get FLUSH_HEADROOM_THRESHOLD()  { return 1024 * 1024 * 1024 }

	static #DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,OracleConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[OracleConstants.DATABASE_KEY] || {},{RDBMS: OracleConstants.DATABASE_KEY}))
	   return this.#DBI_PARAMETERS
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
     const createUser = `grant connect, resource, unlimited tablespace to "${this.parameters.TO_USER}" identified by ${this.CONNECTION_PROPERTIES.password}`;
     await this.executeSQL(createUser,{});      
   }  
     
   classFactory(yadamu) {
     return new OracleQA(yadamu,this,this.connectionParameters,this.parameters)
   }

   async getDDLOperations() {
     const ddl = await super.getDDLOperations()
     // QA only: DBMS_METADATA's KU$ cursors are very large (one TABLE_T cursor was 52 MB on 26ai), and a schema extraction needs
     // 200-400 MB of shared pool for them. Where the SGA can grow the shared pool (EE and the older versions, which can take memory
     // from the buffer cache) they age out normally. Where it can't (26ai Free: about 1.5 GB of SGA with a 1 GB shared pool and a
     // fixed buffer cache), repeated extractions fragment the pool and later statements fail with ORA-04031, so flush it there.
     // Flushing an instance that doesn't need it is expensive: every statement is hard parsed again (+2 hours in dbRoundtrip).
     // YADAMU itself never flushes: it affects every session on the instance. Logged at INFO when it fails, so the run's warning count
     // doesn't change.
     try {
       const results = await this.executeSQL(OracleQA.SQL_SHARED_POOL_HEADROOM,{})
       const headroom = results.rows[0][0]
       if ((headroom === null) || (headroom < OracleQA.FLUSH_HEADROOM_THRESHOLD)) {
         await this.executeSQL(`alter system flush shared_pool`,{})
       }
     } catch (e) {
       this.LOGGER.info([this.DATABASE_VENDOR,'QA','FLUSH SHARED POOL'],`Unable to flush the shared pool: ${e.message}`)
     }
     return ddl
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
			  const cause = this.createDatabaseError(e,stack,sqlStatement)
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

// How much more memory the SGA could give the shared pool: the buffer cache above its configured minimum, plus unallocated SGA.
// CDB-wide, and readable by SYSTEM in a PDB (v$sgastat in a PDB shows only that PDB's share).
const _SQL_SHARED_POOL_HEADROOM = `select (select current_size - user_specified_size from v$sga_dynamic_components where component = 'DEFAULT buffer cache')
                                        + (select current_size from v$sga_dynamic_free_memory) from dual`;

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
						  




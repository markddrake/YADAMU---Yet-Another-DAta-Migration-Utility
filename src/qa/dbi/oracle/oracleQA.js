"use strict" 

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
        
      try {
        const dropUser = `drop user "${this.parameters.TO_USER}" cascade`;
        await this.executeSQL(dropUser,{});      
      } catch (e) {
        if ((e.cause) && (e.cause.errorNum && (e.cause.errorNum === 1918))) {
        }
        else {
          throw e;
        }
      }
      const createUser = `grant connect, resource, unlimited tablespace to "${this.parameters.TO_USER}" identified by ${this.vendorProperties.password}`;
      await this.executeSQL(createUser,{});      
    }  

    async getRowCounts(target) {
        
      let args = {target:`"${target.schema}"`}
      await this.executeSQL(OracleQA.SQL_GATHER_SCHEMA_STATS,args)
      
      args = {target:target.schema}
      const results = await this.executeSQL(OracleQA.SQL_SCHEMA_TABLE_ROWS,args)
      
      return results.rows.map((row,idx) => {          
        return [target.schema,row[0],parseInt(row[1])]
      })
      
    }

	async compareSchemas(source,target,rules) {
		
	  let compareRules = this.yadamu.getCompareRules(rules)	  
	  
	  compareRules.objectsRule   = rules.OBJECTS_COMPARISON_RULE || 'SKIP'
      compareRules.excludeMViews = ((rules.OPERATION  !== 'DBROUNDTRIP') && (rules.MODE === 'DATA_ONLY'))
   	  
	  compareRules = this.JSON_PARSING_SUPPORTED ? JSON.stringify(compareRules) : this.yadamu.makeXML(compareRules)

	  const args = {
		P_SOURCE_SCHEMA        : source.schema,
		P_TARGET_SCHEMA        : target.schema,
		P_RULES                : compareRules
	  }
	        
      const report = {
        successful : []
       ,failed     : []
      }

	  await this.executeSQL(OracleQA.SQL_COMPARE_SCHEMAS,args)      

      const successful = await this.executeSQL(OracleQA.SQL_SUCCESS,{})
            
      report.successful = successful.rows.map((row,idx) => {          
        return [row[0],row[1],row[2],parseInt(row[4])]
      })
        
	  
	  const options = {fetchInfo:[{type: oracledb.STRING, maxSize: 128},{type: oracledb.STRING, maxSize: 128},{type: oracledb.STRING, maxSize: 128},{type: oracledb.STRING, maxSize: 12},{type: oracledb.NUMBER},{type: oracledb.NUMBER},{type: oracledb.NUMBER},{type: oracledb.NUMBER},{type: oracledb.STRING, maxSize: 16*1024*1024}]}
      // const failed = await this.executeSQL(OracleQA.SQL_FAILED,{},options)      
      const failed = await this.executeSQL(OracleQA.SQL_FAILED,{})      
      report.failed = await Promise.all(failed.rows.map(async (row,idx) => {
		const result = [row[0],row[1],row[2],parseInt(row[4]),parseInt(row[5]),parseInt(row[6]),parseInt(row[7]),((row[8] === null) || (typeof row[8] === 'string')) ? row[8] : row[8].getData()]
		return await Promise.all(result)
      }))
	  
      return report
    }
     
    classFactory(yadamu) {
      return new OracleQA(yadamu,this,this.connectionParameters,this.parameters)
    }
	
    async scheduleTermination(pid,workerId) {
	  const tags = this.getTerminationTags(workerId,`${pid.sid},${pid.serial}`)
	  this.yadamuLogger.qa(tags,`Termination Scheduled.`);
      setTimeout(this.yadamu.KILL_DELAY,pid,{ref: false}).then(async (pid) => {
        if ((this.pool instanceof this.oracledb.Pool) && (this.pool.status === this.oracledb.POOL_STATUS_OPEN)) {
	      this.yadamuLogger.log(tags,`Killing connection.`);
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
			  this.yadamuLogger.log(tags,`Worker finished prior to termination.`)
 		    }
			else {
			  const cause = new OracleError(this.DRIVER_ID,e,stack,sqlStatement)
			  this.yadamuLogger.handleException(tags,cause)
			}
            if (!e.message.startsWith('DPI-1010')) {
              try {
     		    await conn.close()
	          } catch (closeError) {
                closeError.cause = e;
 			    this.yadamuLogger.handleException(tags,cause)
              }
		    }    
	      }
		}
		else {
		  this.yadamuLogger.log(tags,`Unable to Kill Connection: Connection Pool no longer available.`);
		}
      })
    }
}
	

export { OracleQA as default }

const _SQL_COMPARE_SCHEMAS = `begin YADAMU_TEST.COMPARE_SCHEMAS(:P_SOURCE_SCHEMA, :P_TARGET_SCHEMA, :P_RULES); end;`;

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
						  




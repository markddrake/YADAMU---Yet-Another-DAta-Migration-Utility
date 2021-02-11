"use strict" 

const oracledb = require('oracledb');

const OracleDBI = require('../../../YADAMU/oracle/node/oracleDBI.js');
const OracleError = require('../../../YADAMU/oracle/node/oracleException.js')
const OracleConstants = require('../../../YADAMU/oracle/node/oracleConstants.js');

const YadamuTest = require('../../common/node/yadamuTest.js');

class OracleQA extends OracleDBI {
    			
    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }
    static get SQL_GATHER_SCHEMA_STATS()   { return _SQL_GATHER_SCHEMA_STATS }

	static #_YADAMU_DBI_PARAMETERS
	
	static get YADAMU_DBI_PARAMETERS()  { 
	   this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,OracleConstants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[OracleConstants.DATABASE_KEY] || {},{RDBMS: OracleConstants.DATABASE_KEY}))
	   return this.#_YADAMU_DBI_PARAMETERS
    }
   
    get YADAMU_DBI_PARAMETERS() {
      return OracleQA.YADAMU_DBI_PARAMETERS
    }	
	
    constructor(yadamu) {
       super(yadamu)
    }

    async scheduleTermination(pid,workerId) {
      this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,`${pid.sid},${pid.serial}`],`Termination Scheduled.`);
      this.assassin = setTimeout(
        async (pid) => {
	      if ((this.pool instanceof this.oracledb.Pool) && (this.pool.status === this.oracledb.POOL_STATUS_OPEN)) {
		    this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,`${pid.sid},${pid.serial}`],`Killing connection.`);
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
			    this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,`${pid.sid},${pid.serial}`],`Worker finished prior to termination.`)
 			  }
			  else {
				const cause = new OracleError(e,stack,sqlStatement)
			    this.yadamuLogger.handleException(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,`${pid.sid},${pid.serial}`],cause)
			  }
              if (!e.message.startsWith('DPI-1010')) {
                try {
     		      await conn.close()
	            } catch (closeError) {
                  closeError.cause = e;
 			      this.yadamuLogger.handleException(['KILL','CLOSE',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,`${pid.sid},${pid.serial}`],cause)
                }
		      }    
			}
		  }
		  else {
		    this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,`${pid.sid},${pid.serial}`],`Unable to Kill Connection: Connection Pool no longer available.`);
		  }
		},
		this.killConfiguration.delay,
	    pid
      )
	  this.assassin.unref()
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
      const createUser = `grant connect, resource, unlimited tablespace to "${this.parameters.TO_USER}" identified by ${this.connectionProperties.password}`;
      await this.executeSQL(createUser,{});      
    }  


	async initialize() {
	  await super.initialize();
      if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	  }
	  if (this.terminateConnection()) {
        const pid = await this.getConnectionID();
	    this.scheduleTermination(pid,this.getWorkerNumber());
	  }
	}
	
	async compareSchemas(source,target,rules) {
				
      const args = {
		P_SOURCE_SCHEMA        : source.schema,
		P_TARGET_SCHEMA        : target.schema,
		P_DOUBLE_PRECISION     : rules.DOUBLE_PRECISION || null,
		P_TIMESTAMP_PRECISION  : rules.TIMESTAMP_PRECISION || null,
		P_ORDERED_JSON         : rules.ORDERED_JSON.toString().toUpperCase(),
		P_XML_RULE             : rules.XML_COMPARISSON_RULES || null,
		P_OBJECTS_RULE         : this.MODE === 'DATA_ONLY' ? 'OMIT_OBJECTS' : null,
		P_EXCLUDE_MVIEWS       : Boolean(rules.MODE === 'DATA_ONLY').toString().toUpperCase()
	  }
	        
      const report = {
        successful : []
       ,failed     : []
      }

	  await this.executeSQL(OracleQA.SQL_COMPARE_SCHEMAS,args)      

      const successful = await this.executeSQL(OracleQA.SQL_SUCCESS,{})
            
      report.successful = successful.rows.map((row,idx) => {          
        return [row[0],row[1],row[2],row[4]]
      })
        
	  
	  const options = {fetchInfo:[{type: oracledb.STRING, maxSize: 128},{type: oracledb.STRING, maxSize: 128},{type: oracledb.STRING, maxSize: 128},{type: oracledb.STRING, maxSize: 12},{type: oracledb.NUMBER},{type: oracledb.NUMBER},{type: oracledb.NUMBER},{type: oracledb.NUMBER},{type: oracledb.STRING, maxSize: 16*1024*1024}]}
      // const failed = await this.executeSQL(OracleQA.SQL_FAILED,{},options)      
      const failed = await this.executeSQL(OracleQA.SQL_FAILED,{})      
      report.failed = await Promise.all(failed.rows.map(async (row,idx) => {
		const result = [row[0],row[1],row[2],row[4],row[5],row[6],row[7],row[8] === null ? row[8] : this.clobToString(row[8])]
		return await Promise.all(result)
      }))
	  
      return report
    }
      
    async getRowCounts(target) {
        
      let args = {target:`"${target.schema}"`}
      await this.executeSQL(OracleQA.SQL_GATHER_SCHEMA_STATS,args)
      
      args = {target:target.schema}
      const results = await this.executeSQL(OracleQA.SQL_SCHEMA_TABLE_ROWS,args)
      
      return results.rows.map((row,idx) => {          
        return [target.schema,row[0],row[1]]
      })
      
    }

    async workerDBI(idx)  {
	  const workerDBI = await super.workerDBI(idx);
      // Manager needs to schedule termination of worker.
	  if (this.terminateConnection(idx)) {
        const pid = await workerDBI.getConnectionID();
	    this.scheduleTermination(pid,idx);
	  }
	  return workerDBI
    }

    classFactory(yadamu) {
      return new OracleQA(yadamu)
    }

}
	

module.exports = OracleQA

const _SQL_COMPARE_SCHEMAS = `begin YADAMU_TEST.COMPARE_SCHEMAS(:P_SOURCE_SCHEMA, :P_TARGET_SCHEMA, :P_DOUBLE_PRECISION, :P_TIMESTAMP_PRECISION, :P_ORDERED_JSON, :P_XML_RULE, :P_OBJECTS_RULE, :P_EXCLUDE_MVIEWS); end;`;

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
						  




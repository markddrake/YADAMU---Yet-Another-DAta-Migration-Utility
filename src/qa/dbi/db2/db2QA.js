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

	static #_DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,DB2Constants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[DB2Constants.DATABASE_KEY] || {},{RDBMS: DB2Constants.DATABASE_KEY}))
	   return this.#_DBI_PARAMETERS
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

	async getRowCounts(target) {

	  let results = await this.executeSQL(`select TABNAME from SYSCAT.TABLES where TABSCHEMA = '${this.CURRENT_SCHEMA}'`)
	  results = results.map((result) => { return Object.values(result)})
	  if (results.length > 0 ) {
	    const sqlCountRows = results.map((row) => { return `select cast('${this.CURRENT_SCHEMA}' as VARCHAR(128)), cast('${row[0]}' as VARCHAR(128)), count(*) from "${this.CURRENT_SCHEMA}"."${row[0]}"`}).join('\nunion all \n')
	    results = await this.executeSQL(sqlCountRows)
	    results = results.map((result) => { return Object.values(result)})
	  }
	  return results
    }    


	buildColumnLists(schemaInfo,rules) {
		
      const timestampLength = 20 + rules.TIMESTAMP_PRECISION;
	  
	  return schemaInfo.map((tableInfo) => {
		  const dataTypes = JSON.parse(tableInfo.DATA_TYPE_ARRAY)
		  const sizeConstraints = JSON.parse(tableInfo.SIZE_CONSTRAINT_ARRAY)
		  return {
			TABLE_NAME   : tableInfo.TABLE_NAME
		  , COLUMN_LIST  : JSON.parse(tableInfo.COLUMN_NAME_ARRAY).map((columnName,idx) => { 
		     switch (dataTypes[idx]) {
			   case this.DATA_TYPES.XML_TYPE:
			     return `HASH(xmlserialize("${columnName}" as BLOB(2G) EXCLUDING XMLDECLARATION),2) "${columnName}"`
			   case this.DATA_TYPES.JSON_TYPE:
			   case this.DATA_TYPES.BLOB_TYPE:
			     return `HASH("${columnName}",2) "${columnName}"`
			   case this.DATA_TYPES.CLOB_TYPE:
			   case this.DATA_TYPES.NCLOB_TYPE:
			     return rules.EMPTY_STRING_IS_NULL ? `HASH(case when length("${columnName}") = 0 then NULL else "${columnName}" end,2) "${columnName}"` : `HASH("${columnName}",2)` 
			   case this.DATA_TYPES.VARCHAR_TYPE:
			   case this.DATA_TYPES.NVARCHAR_TYPE:
			      return rules.EMPTY_STRING_IS_NULL ? `case when "${columnName}" = '' then NULL else "${columnName}" end "${columnName}"` : `"${columnName}"`
			   case this.DATA_TYPES.TIMESTAMP_TYPE:
			      return rules.TIMESTAMP_PRECISION < sizeConstraints[idx][0] ? `substr(to_char("${columnName}",'YYYY-MM-DD HH24:MI:SS.FF12'),1,${timestampLength})` : `"${columnName}"`
			   default:			   
			     return `"${columnName}"`
			  }
		   }).join(',')
		  }
	  })
    }  

    async compareSchemas(source,target,rules) {

      const report = {
        successful : []
       ,failed     : []
      }
	  
	  this.parameters.FROM_USER = source.schema;
      const schemaInfo = (await this.getSchemaMetadata()).filter((tableInfo) => {
         return ((this.TABLE_FILTER.length === 0) || (this.TABLE_FILTER.includes(tableInfo.TABLE_NAME)))
	   })
	
      let columnLists = this.buildColumnLists(schemaInfo,rules)
      
	  const compareResults = [] 
      for (const table of columnLists) {
        const sqlStatement =
`select 
  '${table.TABLE_NAME}' "TABLE_NAME",
  (select count(*) from "${source.schema}"."${table.TABLE_NAME}") SOURCE_ROWS,
  (select count(*) from "${target.schema}"."${table.TABLE_NAME}") TARGET_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from "${source.schema}"."${table.TABLE_NAME}" except select ${table.COLUMN_LIST} from "${target.schema}"."${table.TABLE_NAME}") T1) EXTRA_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from "${target.schema}"."${table.TABLE_NAME}" except select ${table.COLUMN_LIST} from "${source.schema}"."${table.TABLE_NAME}") T2) MISSING_ROWS
  from sysibm.sysdummy1`;
  
        // console.log(sqlStatement)
  
        let results = await this.executeSQL(sqlStatement);
		results = Object.values(results[0])
        compareResults.push(results)
	  }
	 
	 compareResults.forEach((results,idx) => {
        const compareResult =  results
        if ((parseInt(compareResult[1]) === parseInt(compareResult[2])) && (parseInt(compareResult[3]) === 0) && (parseInt(compareResult[4])  === 0)) {
          report.successful.push(new Array(source.schema,target.schema,...compareResult))
        }
        else {
          report.failed.push(new Array(source.schema,target.schema,...compareResult,''))
        }
      })
     
      return report
    }  
   
    classFactory(yadamu) {
      return new DB2QA(yadamu,this,this.connectionParameters,this.parameters)
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
        this.yadamu.LOGGER.handleException(tags,new DB2Error(this.DRIVER_ID,e,stack,operation));
      })
	}

}

export { DB2QA as default }

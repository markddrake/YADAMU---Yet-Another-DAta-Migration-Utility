"use strict" 

import TeradataDBI       from '../../../../node/dbi/teradata/worker/teradataDBI.js';
import {TeradataError}   from '../../../../node/dbi/teradata/teradataException.js'
import TeradataConstants from '../../../../node/dbi/teradata/teradataConstants.js';

import YadamuQALibrary   from '../../../lib/yadamuQALibrary.js'
import Yadamu            from '../../../core/yadamu.js';

class TeradataQA extends YadamuQALibrary.qaMixin(TeradataDBI) {
	
	static #_DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,TeradataConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[TeradataConstants.DATABASE_KEY] || {},{RDBMS: TeradataConstants.DATABASE_KEY}))
	   return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return TeradataQA.DBI_PARAMETERS
    }	
		
	constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters)
    }
	
    async recreateSchema() {		
	  const sqlStatement = `{call YADAMU.SP_RECREATE_DATABASE(?,?,?)}`
	  const results = await this.executeSQL(sqlStatement,[this.parameters.TO_USER,1*1024*1024*1024,null])
    }   
	
    async getRowCounts(target) {
		
	  let results = await this.executeSQL(`select TRIM(TABLENAME) from DBC.TablesVX where DATABASENAME = '${this.CURRENT_SCHEMA}'`)
	  const sqlCountRows = results.map((row) => { return `select cast('${this.CURRENT_SCHEMA}' as VARCHAR(128)), cast('${row[0]}' as VARCHAR(128)), count(*) from "${this.CURRENT_SCHEMA}"."${row[0]}"`}).join('\nunion all \n')
	  results = await this.executeSQL(sqlCountRows)
	  return results
	}
    
    async compareSchemas(source,target,rules)  {

      const report = {
        successful : []
       ,failed     : []
      }

      return report
    }
            

	buildColumnLists(schemaInfo,rules) {

	  return schemaInfo.map((tableInfo) => {
		  return {
			TABLE_NAME   : tableInfo.TABLE_NAME
		  , COLUMN_LIST  : `"${JSON.parse(tableInfo.COLUMN_NAME_ARRAY).join('","')}"`
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

	  let sqlStatement
	  const compareResults = [] 
      for (const table of columnLists) {
        sqlStatement =
`select 
  '${table.TABLE_NAME}' "TABLE_NAME",
  (select count(*) from "${source.schema}"."${table.TABLE_NAME}") SOURCE_ROWS,
  (select count(*) from "${target.schema}"."${table.TABLE_NAME}") TARGET_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from "${source.schema}"."${table.TABLE_NAME}" except select ${table.COLUMN_LIST} from "${target.schema}"."${table.TABLE_NAME}") T1) EXTRA_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from "${target.schema}"."${table.TABLE_NAME}" except select ${table.COLUMN_LIST} from "${source.schema}"."${table.TABLE_NAME}") T2) MISSING_ROWS`;
        try {
          const results = await this.executeSQL(sqlStatement);
          compareResults.push(results)
		} catch (e) {
		  const results = await this.compareResultSets(table.TABLE_NAME,`select ${table.COLUMN_LIST} from "${source.schema}"."${table.TABLE_NAME}"`,`select ${table.COLUMN_LIST} from "${target.schema}"."${table.TABLE_NAME}"`)
          compareResults.push(results)
		} 
	  }
	  
	  compareResults.forEach((results,idx) => {
        const compareResult =  results[0]
        if ((parseInt(compareResult[1]) === parseInt(compareResult[2])) && (parseInt(compareResult[3]) === 0) && (parseInt(compareResult[4])  === 0)) {
          report.successful.push(new Array(source.schema,target.schema,compareResult[0],compareResult[1]))
        }
        else {
          report.failed.push(new Array(source.schema,target.schema,...compareResult,''))
        }
      })
     
      return report
    }  
   
    classFactory(yadamu) {
	  return new TeradataQA(yadamu,this,this.connectionParameters,this.parameters)
    } 
      
}

export { TeradataQA as default }
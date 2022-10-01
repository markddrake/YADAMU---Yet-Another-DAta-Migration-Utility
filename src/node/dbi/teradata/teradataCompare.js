
import YadamuCompare   from '../base/yadamuCompare.js'

class TeradataCompare extends YadamuCompare {    static get SQL_SCHEMA_TABLE_NAMES()    { return _SQL_SCHEMA_TABLE_NAMES }

    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}
	    async getRowCounts(target) {
		
	  let results = await this.dbi.executeSQL(`select TRIM(TABLENAME) from DBC.TABLESV where DATABASENAME = '${target}'`)
	  const sqlCountRows = results.map((row) => { return `select cast('${target}' as VARCHAR(128)), cast('${row[0]}' as VARCHAR(128)), count(*) from "${target}"."${row[0]}"`}).join('\nunion all \n')
	  results = await this.dbi.executeSQL(sqlCountRows)
	  return results
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
	  
	  this.dbi.parameters.FROM_USER = source;
      const schemaInfo = (await this.dbi.getSchemaMetadata()).filter((tableInfo) => {
         return ((this.dbi.TABLE_FILTER.length === 0) || (this.dbi.TABLE_FILTER.includes(tableInfo.TABLE_NAME)))
	   })
	
	  let columnLists = this.buildColumnLists(schemaInfo,rules)
	  
	  const compareResults = [] 
      for (const table of columnLists) {
        const sqlStatement =
`select 
  '${table.TABLE_NAME}' "TABLE_NAME",
  (select count(*) from "${source}"."${table.TABLE_NAME}") SOURCE_ROWS,
  (select count(*) from "${target}"."${table.TABLE_NAME}") TARGET_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from "${source}"."${table.TABLE_NAME}" except select ${table.COLUMN_LIST} from "${target}"."${table.TABLE_NAME}") T1) EXTRA_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from "${target}"."${table.TABLE_NAME}" except select ${table.COLUMN_LIST} from "${source}"."${table.TABLE_NAME}") T2) MISSING_ROWS`;

        let results
        try {
          results = await this.dbi.executeSQL(sqlStatement);
		  results = Object.values(results[0])
          compareResults.push(results)
		} catch (e) {
		  // this.yadamuLogger.handleException([this.dbi.DATABASE_VENDOR,'QA','COMPARE_SCHEMA'],e)  
		  compareResults.push([table.TABLE_NAME,-1,-1,-1,-1,e.message])
		}
	  }
	  
	 compareResults.forEach((results,idx) => {
        const compareResult =  results
        if ((parseInt(compareResult[1]) === parseInt(compareResult[2])) && (parseInt(compareResult[3]) === 0) && (parseInt(compareResult[4])  === 0)) {
          report.successful.push(new Array(source,target,compareResult[0],compareResult[2]))
        }
        else {
          report.failed.push(new Array(source,target,...compareResult,''))
        }
      })

      return report
    }  
	
}
export { TeradataCompare as default }


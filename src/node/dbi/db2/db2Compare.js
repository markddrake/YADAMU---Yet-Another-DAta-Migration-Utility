
import YadamuCompare   from '../base/yadamuCompare.js'

class DB2Compare extends YadamuCompare {

    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

	async getRowCounts(target) {

	  let results = await this.dbi.executeSQL(`select TABNAME from SYSCAT.TABLES where TABSCHEMA = '${target}'`)
	  results = results.map((result) => { return Object.values(result)})
	  if (results.length > 0 ) {
	    const sqlCountRows = results.map((row) => { return `select cast('${target}' as VARCHAR(128)), cast('${row[0]}' as VARCHAR(128)), count(*) from "${target}"."${row[0]}"`}).join('\nunion all \n')
	    results = await this.dbi.executeSQL(sqlCountRows)
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
			   case this.dbi.DATA_TYPES.XML_TYPE:
			     return `HASH(xmlserialize("${columnName}" as BLOB(2G) EXCLUDING XMLDECLARATION),2) "${columnName}"`
			   case this.dbi.DATA_TYPES.JSON_TYPE:
			   case this.dbi.DATA_TYPES.BLOB_TYPE:
			     return `HASH("${columnName}",2) "${columnName}"`
			   case this.dbi.DATA_TYPES.CLOB_TYPE:
			   case this.dbi.DATA_TYPES.NCLOB_TYPE:
			     return rules.EMPTY_STRING_IS_NULL ? `HASH(case when length("${columnName}") = 0 then NULL else "${columnName}" end,2) "${columnName}"` : `HASH("${columnName}",2)` 
			   case this.dbi.DATA_TYPES.VARCHAR_TYPE:
			   case this.dbi.DATA_TYPES.NVARCHAR_TYPE:
			      return rules.EMPTY_STRING_IS_NULL ? `case when "${columnName}" = '' then NULL else "${columnName}" end "${columnName}"` : `"${columnName}"`
			   case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
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
  (select count(*) from (select ${table.COLUMN_LIST} from "${target}"."${table.TABLE_NAME}" except select ${table.COLUMN_LIST} from "${source}"."${table.TABLE_NAME}") T2) MISSING_ROWS
  from sysibm.sysdummy1`;
  
        let results
        try {
          results = await this.dbi.executeSQL(sqlStatement);
		  results = Object.values(results[0])
          compareResults.push(results)
		} catch (e) {
		  // this.yadamuLogger.handleException([this.DATABASE_VENDOR,'QA','COMPARE_SCHEMA'],e)  
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
export { DB2Compare as default }

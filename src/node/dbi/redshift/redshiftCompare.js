
import YadamuCompare   from '../base/yadamuCompare.js'

class RedshiftCompare extends YadamuCompare {    static get SQL_SCHEMA_TABLE_NAMES()    { return _SQL_SCHEMA_TABLE_NAMES }

    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

    buildColumnLists(schemaColumnInfo,rules) {

      const columnLists = {}
      let tableInfo = undefined
      let tableName = undefined
      let columns = undefined
      schemaColumnInfo.forEach((columnInfo) => {
        if (tableName !== columnInfo[1] ) {
          if (tableName) {
            columnLists[tableName] = columns.join(',')
          }
          tableName = columnInfo[1]
          columns = []
        }
        
	    switch (true) {
		   case columnInfo[3].startsWith('xml') :
			 columns.push(rules.XML_COMPARISON_RULE === 'STRIP_XML_DECLARATION' ? `YADAMU.STRIP_XML_DECLARATION("${columnInfo[2]}")` : `"${columnInfo[2]}"`)
			 break;
           case columnInfo[3].startsWith('char') :
           case columnInfo[3].startsWith('varchar') :
           case columnInfo[3].startsWith('long varchar') :
		     columns.push(rules.EMPTY_STRING_IS_NULL ? `case when "${columnInfo[2]}" = '' then NULL else "${columnInfo[2]}" end` : `"${columnInfo[2]}"`)
             break;
		  case columnInfo[3].startsWith('float') :
		     columns.push(rules.DOUBLE_PRECISION !== null ? `round("${columnInfo[2]}",${rules.DOUBLE_PRECISION})` : `"${columnInfo[2]}"`)
			 break;
		  case columnInfo[3].startsWith('geography') :
		  case columnInfo[3].startsWith('geometry') :
		     columns.push(`case when YADAMU.invalidGeoHash("${columnInfo[2]}") then ST_AsText("${columnInfo[2]}") else ${rules.SPATIAL_PRECISION < 20 ? `ST_GeoHash("${columnInfo[2]}" USING PARAMETERS NUMCHARS=${rules.SPATIAL_PRECISION})` : `ST_GeoHash("${columnInfo[2]}")`} end`)
			 break;
           default:
             columns.push(`"${columnInfo[2]}"`)
        }
      })
      if (tableName) {
        columnLists[tableName] = columns.join(',')
      }
      return columnLists
    }

	async getRowCounts(target) {
	
	  let results = await this.dbi.executeSQL(`select table_name from information_schema.tables where table_schema = '${target}'`)
	  results = results.rows.map((result) => { return Object.values(result)})
	  if (results.length > 0 ) {
	    const sqlCountRows = results.map((row) => { return `select cast('${target}' as VARCHAR(128)), cast('${row[0]}' as VARCHAR(128)), count(*) from "${target}"."${row[0]}"`}).join('\nunion all \n')
	    results = await this.dbi.executeSQL(sqlCountRows)
		results.rows.forEach((row) => {
		  row[2] = parseInt(row[2])
		})
	    results = results.rows.map((result) => { return Object.values(result)})
	  }
	  return results
    }  
	
    async compareSchemas(source,target,rules) {

      const report = {
        successful : []
       ,failed     : []
      }
	  
      const results = await this.dbi.executeSQL(this.dbi.StatementLibrary.SQL_SCHEMA_INFORMATION(source));
      
      let columnLists = this.buildColumnLists(results.rows,rules)

      let compareOperations = {}
	  if (this.dbi.TABLE_FILTER.length === 0) {
		compareOperations = columnLists 
      }	   
	  else {
	   this.dbi.TABLE_FILTER.forEach((tableName) => {
           compareOperations[tableName] = columnLists[tableName]
        })  
      }
	  
      const compareResults = await Promise.all(Object.keys(compareOperations).map(async (TABLE_NAME) => {
        const sqlStatement =
`select 
  '${TABLE_NAME}' "TABLE_NAME",
  (select count(*) from "${source}"."${TABLE_NAME}") SOURCE_ROWS,
  (select count(*) from "${target}"."${TABLE_NAME}") TARGET_ROWS,
  (select count(*) from (select ${compareOperations[TABLE_NAME]} from "${source}"."${TABLE_NAME}" except select ${compareOperations[TABLE_NAME]} from "${target}"."${TABLE_NAME}") T1) EXTRA_ROWS,
  (select count(*) from (select ${compareOperations[TABLE_NAME]} from "${target}"."${TABLE_NAME}" except select ${compareOperations[TABLE_NAME]} from "${source}"."${TABLE_NAME}") T2) MISSING_ROWS`;
        return this.dbi.executeSQL(sqlStatement);
     }))
	 
     compareResults.forEach((results,idx) => {
        const compareResult =  results.rows[0]
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

export { RedshiftCompare as default }


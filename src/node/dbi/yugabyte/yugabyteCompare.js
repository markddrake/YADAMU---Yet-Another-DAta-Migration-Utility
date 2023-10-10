
import YadamuCompare   from '../base/yadamuCompare.js'

class YugabyteCompare extends YadamuCompare {    

    static get SQL_GENERATE_COMPARE()      { return _SQL_GENERATE_COMPARE }
    
    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

	async getRowCounts(target) {
		
	  let results = await this.dbi.executeSQL(`select table_name from information_schema.tables where table_schema = '${target}'`)
	  results = results.rows.map((result) => { return Object.values(result)})
	  if (results.length > 0 ) {
	    const sqlCountRows = results.map((row) => { return `select cast('${target}' as VARCHAR(128)), cast('${row[0]}' as VARCHAR(128)), count(*) from "${target}"."${row[0]}"`}).join('\nunion all \n')
	    results = await this.dbi.executeSQL(sqlCountRows)
	    results = results.rows.map((result) => { return Object.values(result)})
	  }
	  return results
    }    

    async compareSchemas(source,target,rules) {

      const report = {
        successful : []
       ,failed     : []
      }

      const compareRules = this.formatCompareRules(rules)	  

      // Fails :  Query error: Restart read required at: { read: { physical: 1657219918659873 logical: 1 } local_limit: { physical: 1657219918659873 logical: 1 } global_limit: <min> in_txn_limit: <max> serial_no: 0 }	  
      
	  let results = await this.dbi.executeSQL(YugabyteCompare.SQL_GENERATE_COMPARE,[source,compareRules])      
    	  
      const tableList = (results.rows).filter((tableInfo) => {
         return ((this.dbi.TABLE_FILTER.length === 0) || (this.dbi.TABLE_FILTER.includes(tableInfo[0])))
	   })

      const timeout_period = 30 * 60 * 1000;
      results = await this.dbi.executeSQL(`set statement_timeout=${timeout_period}`);
	  
	  const compareResults = [] 
      for (const table of tableList) {
        const sqlStatement =
`select 
  '${table[0]}' "TABLE_NAME",
  (select count(*) from "${source}"."${table[0]}") SOURCE_ROWS,
  (select count(*) from "${target}"."${table[0]}") TARGET_ROWS,
  (select count(*) from (select ${table[1]} from "${source}"."${table[0]}" except select ${table[1]} from "${target}"."${table[0]}") T1) EXTRA_ROWS,
  (select count(*) from (select ${table[1]} from "${target}"."${table[0]}" except select ${table[1]} from "${source}"."${table[0]}") T2) MISSING_ROWS`
  
        try {
          let results = await this.dbi.executeSQL(sqlStatement);
  		  results = Object.values(results.rows[0])
          compareResults.push(results)
		} catch (e) {
		  // this.LOGGER.handleException([this.dbi.DATABASE_VENDOR,'QA','COMPARE_SCHEMA'],e)  
		  compareResults.push([table[0],-1,-1,-1,-1,e.message])
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

export { YugabyteCompare as default }

const _SQL_GENERATE_COMPARE   = `select * from YADAMU.GENERATE_COMPARE_COLUMNS($1,$2)`
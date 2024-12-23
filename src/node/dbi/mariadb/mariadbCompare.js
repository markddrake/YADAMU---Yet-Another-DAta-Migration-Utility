
import YadamuCompare   from '../base/yadamuCompare.js'

class MariadbCompare extends YadamuCompare {    static get SQL_SCHEMA_TABLE_NAMES()    { return _SQL_SCHEMA_TABLE_NAMES }

static #SQL_SCHEMA_TABLE_ROWS = `select TABLE_NAME, TABLE_ROWS from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = ?`;

  static #SQL_ACCURATE_ROW_COUNT = `select group_concat(concat('select ''',TABLE_NAME,''' "TABLE_NAME", count(*) TABLE_ROWS from "',TABLE_SCHEMA,'"."',TABLE_NAME,'"') SEPARATOR ' union all ' ) "SQL_STATEMENT" from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = ?`;  

  static #SQL_COMPARE_SCHEMAS =  `CALL COMPARE_SCHEMAS(?,?,?);`;

  static #SQL_SUCCESS =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'SUCCESSFUL' as "RESULTS", TARGET_ROW_COUNT
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
   and MISSING_ROWS = 0
   and EXTRA_ROWS = 0
   and NOTES is NULL
order by TABLE_NAME`;

  static #SQL_FAILED =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, NOTES
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
    or MISSING_ROWS <> 0
    or EXTRA_ROWS <> 0
    or NOTES is NOT NULL
 order by TABLE_NAME`;


  static get SQL_SCHEMA_TABLE_ROWS()     { return this.#SQL_SCHEMA_TABLE_ROWS }
  static get SQL_ACCURATE_ROW_COUNT()    { return this.#SQL_ACCURATE_ROW_COUNT }
  static get SQL_COMPARE_SCHEMAS()       { return this.#SQL_COMPARE_SCHEMAS }
  static get SQL_SUCCESS()               { return this.#SQL_SUCCESS }
  static get SQL_FAILED()                { return this.#SQL_FAILED }
		
    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

    async getRowCounts(target) {

      let results = await this.dbi.executeSQL(MariadbCompare.SQL_ACCURATE_ROW_COUNT,[target]);
	  const sqlStatement = results[0][0]
	  
	  results = sqlStatement ? await this.dbi.executeSQL(sqlStatement) : []
	  	  
	  results.forEach((row,idx) => {          
        row[1] = parseInt(row[1])
	    row.unshift(target)
      })
      return results
    }
    
    async compareSchemas(source,target,rules) {

      const report = {
        successful : []
       ,failed     : []
      }

      const compareRules =  JSON.stringify(this.formatCompareRules(rules))

      let results = await this.dbi.executeSQL(MariadbCompare.SQL_COMPARE_SCHEMAS,[source,target,compareRules])

      const successful = await this.dbi.executeSQL(MariadbCompare.SQL_SUCCESS,{})
      report.successful = successful
     
      const failed = await this.dbi.executeSQL(MariadbCompare.SQL_FAILED,{})
      report.failed = failed

      return report
    }
			
}

export { MariadbCompare as default }


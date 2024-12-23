
import YadamuCompare   from '../base/yadamuCompare.js'

class MySQLCompare extends YadamuCompare {    

  
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
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'FAILED' as "RESULTS", SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, NOTES
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

      let results = await this.dbi.executeSQL(MySQLCompare.SQL_ACCURATE_ROW_COUNT,[target]);
	  const sqlStatement = results[0].SQL_STATEMENT
	  
	  results = sqlStatement ? await this.dbi.executeSQL(sqlStatement) : []
      	  
      return results.map((row,idx) => {          
        return [target,row.TABLE_NAME,parseInt(row.TABLE_ROWS)]
      })
	  
	  

    }
    
    async compareSchemas(source,target,rules) {     

      const report = {
        successful : []
       ,failed     : []
      }
      const compareRules =  this.formatCompareRules(rules)
	  
      let results = await this.dbi.executeSQL(MySQLCompare.SQL_COMPARE_SCHEMAS,[source,target,JSON.stringify(compareRules)]);

      const successful = await this.dbi.executeSQL(MySQLCompare.SQL_SUCCESS,{})
      report.successful = successful.map((row,idx) => {          
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,parseInt(row.TARGET_ROW_COUNT)]
      })

      const failed = await this.dbi.executeSQL(MySQLCompare.SQL_FAILED,{})
      report.failed = failed.map((row,idx) => {
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,parseInt(row.SOURCE_ROW_COUNT),parseInt(row.TARGET_ROW_COUNT),parseInt(row.MISSING_ROWS),parseInt(row.EXTRA_ROWS),(row.NOTES !== undefined ? row.NOTES : '')]
      })

      return report
    }
   

			
}

export { MySQLCompare as default }



import YadamuCompare   from '../base/yadamuCompare.js'

class MariadbCompare extends YadamuCompare {    static get SQL_SCHEMA_TABLE_NAMES()    { return _SQL_SCHEMA_TABLE_NAMES }

    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }
	
    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

    async getRowCounts(target) {
      
	  const ACURATE_ROW_COUNT = results.map((row) => {return `select '${target}', '${row.TABLE_NAME}', count(*) from "${target}"."{row.TABLE}"`}).join('\nunion all\n');
      const results = await this.dbi.executeSQL(MariadbCompare.ACURATE_ROW_COUNT);
	  
      return results.map((row,idx) => {          
        return [target,row.TABLE_NAME,parseInt(row.TABLE_ROWS)]
      })
	  
    }
    
    async getRowCounts(target) {

      const results = await this.dbi.executeSQL(MariadbCompare.SQL_SCHEMA_TABLE_ROWS,[target]);
	  results.forEach((row,idx) => {          
        row[2] = parseInt(row[2])
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

const _SQL_SUCCESS =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
   and MISSING_ROWS = 0
   and EXTRA_ROWS = 0
   and NOTES is NULL
order by TABLE_NAME`;

const _SQL_FAILED = 
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, NOTES
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
    or MISSING_ROWS <> 0
    or EXTRA_ROWS <> 0
    or NOTES is NOT NULL
 order by TABLE_NAME`;

const _SQL_SCHEMA_TABLE_ROWS = `select TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = ?`;

const _SQL_COMPARE_SCHEMAS =  `CALL COMPARE_SCHEMAS(?,?,?);`;

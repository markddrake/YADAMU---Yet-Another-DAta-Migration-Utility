
import YadamuCompare   from '../base/yadamuCompare.js'

class MySQLCompare extends YadamuCompare {    static get SQL_SCHEMA_TABLE_NAMES()    { return _SQL_SCHEMA_TABLE_NAMES }

    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }
	
    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

    async getRowCounts(target) {

      let results = await this.dbi.executeSQL(MySQLCompare.SQL_SCHEMA_TABLE_ROWS,[target]);
      
	  // const ACURATE_ROW_COUNT = results.map((row) => {return `select '${target}', '${row.TABLE_NAME}', count(*) from "${target}"."{row.TABLE}"`}).join('\nunion all\n');
      // results = await this.dbi.executeSQL(MySQLCompare.ACURATE_ROW_COUNT);
	  
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

const _SQL_SUCCESS =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'SUCCESSFUL' as "RESULTS", TARGET_ROW_COUNT
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
   and MISSING_ROWS = 0
   and EXTRA_ROWS = 0
   and NOTES is NULL
order by TABLE_NAME`;

const _SQL_FAILED = 
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'FAILED' as "RESULTS", SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, NOTES
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
    or MISSING_ROWS <> 0
    or EXTRA_ROWS <> 0
    or NOTES is NOT NULL
 order by TABLE_NAME`;

const _SQL_SCHEMA_TABLE_ROWS = `select TABLE_NAME, TABLE_ROWS from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = ?`;

const _SQL_COMPARE_SCHEMAS =  `CALL COMPARE_SCHEMAS(?,?,?);`;

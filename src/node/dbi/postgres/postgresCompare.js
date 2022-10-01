
import YadamuCompare   from '../base/yadamuCompare.js'

class PostgresCompare extends YadamuCompare {    static get SQL_SCHEMA_TABLE_NAMES()    { return _SQL_SCHEMA_TABLE_NAMES }

    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }
	
    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

	async getRowCounts(target) {
	
     
      const results = await this.dbi.executeSQL(PostgresCompare.SQL_SCHEMA_TABLE_ROWS,[target]);
      return results.rows
    }    

    async compareSchemas(source,target,rules) {

      const report = {
        successful : []
       ,failed     : []
      }

      const compareRules = this.formatCompareRules(rules)	  

      await this.dbi.executeSQL(PostgresCompare.SQL_COMPARE_SCHEMAS,[source,target,compareRules])      
      
      const successful = await this.dbi.executeSQL(PostgresCompare.SQL_SUCCESS)            
      report.successful = successful.rows
      
      const failed = await this.dbi.executeSQL(PostgresCompare.SQL_FAILED)
      report.failed = failed.rows
      return report
    }
			
}

export { PostgresCompare as default }

const _SQL_SUCCESS =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT
   from SCHEMA_COMPARE_RESULTS 
  where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
    and MISSING_ROWS = 0
    and EXTRA_ROWS = 0
    and SQLERRM is NULL
 order by TABLE_NAME`;

const _SQL_FAILED = 
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS,  SQLERRM
   from SCHEMA_COMPARE_RESULTS 
  where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
     or MISSING_ROWS <> 0
      or EXTRA_ROWS <> 0
    or SQLERRM is NOT NULL
  order by TABLE_NAME`;
  
const _SQL_SCHEMA_TABLE_NAMES = `select relname from pg_stat_user_tables where schemaname = $1`;

// const _SQL_SCHEMA_TABLE_ROWS  = `select schemaname, relname, n_live_tup from pg_stat_user_tables where schemaname = $1`;

const _SQL_SCHEMA_TABLE_ROWS = 
`with ROW_COUNTS as (
select table_schema, table_name,  query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
  from information_schema.tables
 where table_schema = $1 
)
select table_schema, table_name, (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
  from ROW_COUNTS`

const _SQL_COMPARE_SCHEMAS    = `call YADAMU.COMPARE_SCHEMA($1,$2,$3)`
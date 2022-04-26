
import {
  setTimeout 
}                               from "timers/promises"
						       
import RedshiftDBI              from '../../../node/dbi//redshift/redshiftDBI.js';
import RedshiftError            from '../../../node/dbi//redshift/redshiftException.js'
import RedshiftConstants        from '../../../node/dbi//redshift/redshiftConstants.js';
						       
import Yadamu                   from '../../core/yadamu.js';
import YadamuQALibrary          from '../../lib/yadamuQALibrary.js'

import RedshiftStatementLibrary from './pg802StatementLibrary.js'

class RedshiftQA extends YadamuQALibrary.qaMixin(RedshiftDBI) {
	
    // static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }

    static #_DBI_PARAMETERS
    
    static get DBI_PARAMETERS()  { 
       this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,RedshiftConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[RedshiftConstants.DATABASE_KEY] || {},{RDBMS: RedshiftConstants.DATABASE_KEY}))
       return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return RedshiftQA.DBI_PARAMETERS
    }   
    
    SQL_SCHEMA_TABLE_ROWS(schema) { return `with num_rows as (
    select schema_name,   
           anchor_table_name as table_name,
           sum(total_row_count) as rows
      from v_monitor.storage_containers sc
      join v_catalog.projections p
           on sc.projection_id = p.projection_id
           and p.is_super_projection = true
    where schema_name = '${schema}'
     group by schema_name,
              table_name,
              sc.projection_id
),
tables_with_rows as (
  select schema_name, table_name, max(rows) as rows
    from num_rows
   group by schema_name,
         table_name
)
select t.table_schema, t.table_name, case when rows is null then 0 else rows end
  from tables_with_rows twr
       right outer join v_catalog.tables t
                on t.table_schema = twr.schema_name
               and t.table_name = twr.table_name 
  where t.table_schema = '${schema}'

`
    }
    
    get REDSHIFT_SIMULATION_MODE()      { return true }

    get SUPPORTED_STAGING_PLATFORMS()   { return this.REDSHIFT_SIMULATION_MODE ?  ["loader"] : super.SUPPORTED_STAGING_PLATFORMS }
    
    constructor(yadamu,manager,connectionSettings,parameters) {
      super(yadamu,manager,connectionSettings,parameters);
      this.StatementLibrary = this.REDSHIFT_SIMULATION_MODE ? RedshiftStatementLibrary : this.StatementLibrary
    }
       
    async recreateSchema() {
      try {
        const dropSchema = this.REDSHIFT_SIMULATION_MODE ? `select execute($$drop schema "${this.parameters.TO_USER}" cascade$$) where exists (select 1 from INFORMATION_SCHEMA.SCHEMATA where schema_name = '${this.parameters.TO_USER}')` : `drop schema if exists "${this.parameters.TO_USER}" cascade`;
        this.SQL_TRACE.traceSQL(dropSchema)
        await this.executeSQL(dropSchema);      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          this.yadamu.LOGGER.handleException([this.DATABASE_VENDOR,'RECREATE DATABASE',this.parameters.TO_USER],e);
		  throw e
        }
      }
      // await this.createSchema(this.parameters.TO_USER);    
    }      

    async getRowCounts(target) {
      const results = await this.executeSQL(this.SQL_SCHEMA_TABLE_ROWS(target.schema));
      return results.rows
    }    


    buildColumnLists(schemaColumnInfo,rules) {

      // console.log(rules)
	  
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

    async compareSchemas(source,target,rules) {

      const report = {
        successful : []
       ,failed     : []
      }
	  
      const results = await this.executeSQL(this.StatementLibrary.SQL_SCHEMA_INFORMATION(source.schema));
      
      let columnLists = this.buildColumnLists(results.rows,rules)

      let compareOperations = {}
	  if (this.TABLE_FILTER.length === 0) {
		compareOperations = columnLists 
      }	   
	  else {
	   this.TABLE_FILTER.forEach((tableName) => {
           compareOperations[tableName] = columnLists[tableName]
        })  
      }
	  
	  // console.log(compareOperations)
	  
      const compareResults = await Promise.all(Object.keys(compareOperations).map(async (TABLE_NAME) => {
        const sqlStatement =
`select 
  '${TABLE_NAME}' "TABLE_NAME",
  (select count(*) from "${source.schema}"."${TABLE_NAME}") SOURCE_ROWS,
  (select count(*) from "${target.schema}"."${TABLE_NAME}") TARGET_ROWS,
  (select count(*) from (select ${compareOperations[TABLE_NAME]} from "${source.schema}"."${TABLE_NAME}" except select ${compareOperations[TABLE_NAME]} from "${target.schema}"."${TABLE_NAME}") T1) EXTRA_ROWS,
  (select count(*) from (select ${compareOperations[TABLE_NAME]} from "${target.schema}"."${TABLE_NAME}" except select ${compareOperations[TABLE_NAME]} from "${source.schema}"."${TABLE_NAME}") T2) MISSING_ROWS`;
        return this.executeSQL(sqlStatement);
     }))
	 
	 
     
     compareResults.forEach((results,idx) => {
        const compareResult =  results.rows[0]
        if ((parseInt(compareResult[1]) === parseInt(compareResult[2])) && (parseInt(compareResult[3]) === 0) && (parseInt(compareResult[4])  === 0)) {
          report.successful.push(new Array(source.schema,target.schema,...compareResult))
        }
        else {
          report.failed.push(new Array(source.schema,target.schema,...compareResult,''))
        }
      })
     
      return report
    }
        
	async scheduleTermination(pid,workerId) {
	  let stack
	  const operation = `select pg_terminate_backend(${pid})`
	  const tags = this.getTerminationTags(workerId,pid)
	  this.yadamuLogger.qa(tags,`Termination Scheduled.`);
	  setTimeout(this.yadamu.KILL_DELAY,pid,{ref : false}).then(async (pid) => {
        if (this.pool !== undefined && this.pool.end) {
	      stack = new Error().stack
		  this.yadamuLogger.log(tags,`Killing connection.`);
	      const conn = await this.getConnectionFromPool();
		  const res = await conn.query(operation);
		  await conn.release()
		}
		else {
		  this.yadamuLogger.log(tags,`Unable to Kill Connection: Connection Pool no longer available.`);
		}
      }).catch((e) => {
        this.yadamu.LOGGER.handleException(tags,new PostgresError(this.DRIVER_ID,e,stack,operation));
      })
	}
	
    classFactory(yadamu) {
      return new RedshiftQA(yadamu,this,this.connectionParameters,this.parameters)
    }
}

export { RedshiftQA as default }

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

const _SQL_SCHEMA_TABLE_ROWS = 
`with num_rows as (
    select schema_name,
           anchor_table_name as table_name,
           sum(total_row_count) as rows
    from v_monitor.storage_containers sc
    join v_catalog.projections p
         on sc.projection_id = p.projection_id
         and p.is_super_projection = true
    where schema_name = ?
    group by schema_name,
             table_name,
             sc.projection_id
)
select schema_name,
       table_name,
       max(rows) as rows
from num_rows
group by schema_name,
         table_name;`

const _SQL_COMPARE_SCHEMAS = `call COMPARE_SCHEMA($1,$2,$3)`
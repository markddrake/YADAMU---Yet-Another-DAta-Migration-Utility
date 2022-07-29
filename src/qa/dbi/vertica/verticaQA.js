
import {
  setTimeout 
}                       from "timers/promises"

import VerticaDBI       from '../../../node/dbi/vertica/verticaDBI.js';
import {VerticaError}   from '../../../node/dbi/vertica/verticaException.js'
import VerticaConstants from '../../../node/dbi/vertica/verticaConstants.js';

import Yadamu           from '../../core/yadamu.js';
import YadamuQALibrary  from '../../lib/yadamuQALibrary.js'


class VerticaQA extends YadamuQALibrary.qaMixin(VerticaDBI) {
    
    // static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }

    static #_DBI_PARAMETERS
    
    static get DBI_PARAMETERS()  { 
       this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,VerticaConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[VerticaConstants.DATABASE_KEY] || {},{RDBMS: VerticaConstants.DATABASE_KEY}))
       return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return VerticaQA.DBI_PARAMETERS
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
    
    constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters);
    }

    async recreateSchema() {
      try {
        const dropSchema = `drop schema if exists "${this.parameters.TO_USER}" cascade`;
        await this.executeSQL(dropSchema);      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }        
        else {
          this.yadamu.LOGGER.handleException([this.DATABASE_VENDOR,'RECREATE DATABASE',this.parameters.TO_USER],e);
        }
      }
      await this.createSchema(this.parameters.TO_USER);    
    }      

    async getRowCounts(target) {
      const results = await this.executeSQL(this.SQL_SCHEMA_TABLE_ROWS(target.schema));
      return results.rows
    }    

    buildColumnLists(schemaMetadata,rules) {
		
	  const compareInfo = {}
	  schemaMetadata.forEach((tableMetadata) => {
        compareInfo[tableMetadata.TABLE_NAME] = tableMetadata.DATA_TYPE_ARRAY.map((dataType,idx) => {
		  const columnName = tableMetadata.COLUMN_NAME_ARRAY[idx]
		  switch (dataType) {
			case (this.DATA_TYPES.XML_TYPE):
			  return rules.XML_COMPARISON_RULE === 'STRIP_XML_DECLARATION' ? `YADAMU.STRIP_XML_DECLARATION("${columnName}")` : `"${columnName}"`
		    case this.DATA_TYPES.JSON_TYPE:
              return `case when "${columnName}" is NULL then NULL when SUBSTR("${columnName}",1,1) = '{' then MAPTOSTRING(MAPJSONEXTRACTOR("${columnName}")) when SUBSTR("${columnName}",1,1) = '[' then MAPTOSTRING(mapDelimitedExtractor(substr("${columnName}",2,length("${columnName}")-2) using parameters delimiter=',')) else "${columnName}" end`
            case this.DATA_TYPES.CHAR_TYPE :
            case this.DATA_TYPES.VARCHAR_TYPE :
            case this.DATA_TYPES.CLOB_TYPE :
		      return rules.EMPTY_STRING_IS_NULL ? `case when "${columnName}" = '' then NULL else "${columnName}" end` : `"${columnName}"`
		    case this.DATA_TYPES.DOUBLE_TYPE :
		      const columnClause = rules.DOUBLE_PRECISION !== null ? `round("${columnName}",${rules.DOUBLE_PRECISION})` : `"${columnName}"`
			  return rules.INFINITY_IS_NULL ? `case when "${columnName}" = 'Inf' then NULL when "${columnName}" = '-Inf' then NULL when "${columnName}" <> "${columnName}" then NULL else ${columnClause} end` : columnClause
		    case this.DATA_TYPES.NUMERIC_TYPE :
		    case this.DATA_TYPES.DECIMAL_TYPE:
              return ((rules.NUMERIC_SCALE !== null) && (rules.NUMERIC_SCALE < tableMetadata.SIZE_CONSTRAINT_ARRAY[idx][0])) ? `round("${columnName}",${rules.NUMERIC_SCALE})` : `"${columnName}"`
  		    case this.DATA_TYPES.GEOGRAPHY_TYPE :
            case this.DATA_TYPES.GEOMETRY_TYPE :
		      return `case when YADAMU.invalidGeoHash("${columnName}") then ST_AsText("${columnName}") else ${rules.SPATIAL_PRECISION < 20 ? `ST_GeoHash("${columnName}" USING PARAMETERS NUMCHARS=${rules.SPATIAL_PRECISION})` : `ST_GeoHash("${columnName}")`} end`
            default:
              return `"${columnName}"`
          }
	    }).join(',')
      })
	  return compareInfo
    }

    async compareSchemas(source,target,rules) {
		
      const report = {
        successful : []
       ,failed     : []
      }
	  
	  const results = await this.executeSQL(this.StatementLibrary.SQL_SCHEMA_INFORMATION(source.schema))
	  let compareInfo = this.buildSchemaInfo(results.rows)
	 	  
      const compareOperations = this.buildColumnLists(compareInfo,rules) 
	  
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
        
    classFactory(yadamu) {
      return new VerticaQA(yadamu,this,this.connectionParameters,this.parameters)
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
        this.yadamu.LOGGER.handleException(tags,new VerticaError(this.DRIVER_ID,e,stack,operation));
      })
    }
  
}

export { VerticaQA as default }

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
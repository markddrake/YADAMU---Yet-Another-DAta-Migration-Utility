"use strict" 

const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js')

const VerticaDBI = require('../../../YADAMU/vertica/node/verticaDBI.js');
const VerticaError = require('../../../YADAMU/vertica/node/verticaException.js')
const VerticaConstants = require('../../../YADAMU/vertica/node/verticaConstants.js');

const YadamuTest = require('../../common/node/yadamuTest.js');


class VerticaQA extends VerticaDBI {
    
    // static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }

    static #_YADAMU_DBI_PARAMETERS
    
    static get YADAMU_DBI_PARAMETERS()  { 
       this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,VerticaConstants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[VerticaConstants.DATABASE_KEY] || {},{RDBMS: VerticaConstants.DATABASE_KEY}))
       return this.#_YADAMU_DBI_PARAMETERS
    }
   
    get YADAMU_DBI_PARAMETERS() {
      return VerticaQA.YADAMU_DBI_PARAMETERS
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
    
    constructor(yadamu) {
       super(yadamu);
    }
    
    setMetadata(metadata) {
      super.setMetadata(metadata)
    }
     
    async initialize() {
      await super.initialize();
      if (this.options.recreateSchema === true) {
        await this.recreateSchema();
      }
      if (this.terminateConnection()) {
        const pid = await this.getConnectionID();
        this.scheduleTermination(pid,this.getWorkerNumber());
      }
    }
    
    async recreateSchema() {
      try {
        const dropSchema = `drop schema if exists "${this.parameters.TO_USER}" cascade`;
        this.status.sqlTrace.write(`${dropSchema};\n--\n`)
        await this.executeSQL(dropSchema);      
      } catch (e) {
        console.log(e)
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        
        else {
          throw e;
        }
      }
      await this.createSchema(this.parameters.TO_USER);    
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
			 columns.push(rules.XML_COMPARISSON_RULE === 'STRIP_XML_DECLARATION' ? `YADAMU.STRIP_XML_DECLARATION("${columnInfo[2]}")` : `"${columnInfo[2]}"`)
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
        
    async workerDBI(idx)  {
      const workerDBI = await super.workerDBI(idx);
      // Manager needs to schedule termination of worker.
      if (this.terminateConnection(idx)) {
        const pid = await workerDBI.getConnectionID();
        this.scheduleTermination(pid,idx);
      }
      return workerDBI
    }

    async scheduleTermination(pid,workerId) {
      this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Termination Scheduled.`);
      const timer = setTimeout(
        async (pid) => {
          try {
            if (this.pool !== undefined && this.pool.end) {
              this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Killing connection.`);
              const conn = await this.getConnectionFromPool();
              const res = await conn.query(`select pg_terminate_backend(${pid})`);
              await conn.release()
            }
            else {
              this.yadamuLogger.qa(['KILL',this.ON_ERROR,this.DATABASE_VENDOR,this.killConfiguration.process,workerId,this.killConfiguration.delay,pid],`Unable to Kill Connection: Connection Pool no longer available.`);
            }
           } catch (e) {
             console.log(e);
           }
        },
        this.killConfiguration.delay,
        pid
      )
      timer.unref()
    }
    
}

module.exports = VerticaQA

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
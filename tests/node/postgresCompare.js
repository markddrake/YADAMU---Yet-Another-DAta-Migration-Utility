"use strict" 

const PostgresDBI = require('../../postgres/node/postgresDBI.js');

const sqlSuccess =
`select SOURCE_SCHEMA "SOURCE_SCHEMA", TARGET_SCHEMA "TARGET_SCHEMA", TABLE_NAME "TABLE_NAME", 'SUCCESSFUL' "RESULTS", TARGET_ROW_COUNT "TARGET_ROW_COUNT"
   from SCHEMA_COMPARE_RESULTS 
  where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
    and MISSING_ROWS = 0
    and EXTRA_ROWS = 0
    and SQLERRM is NULL
 order by TABLE_NAME`;

const sqlFailed = 
`select SOURCE_SCHEMA "SOURCE_SCHEMA", TARGET_SCHEMA "TARGET_SCHEMA", TABLE_NAME "TABLE_NAME", 'FAILED' "RESULTS", SOURCE_ROW_COUNT "SOURCE_ROW_COUNT", TARGET_ROW_COUNT "TARGET_ROW_COUNT", MISSING_ROWS "MISSING_ROWS", EXTRA_ROWS "EXTRA_ROWS",  SQLERRM "SQLERRM"
   from SCHEMA_COMPARE_RESULTS 
  where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
     or MISSING_ROWS <> 0
      or EXTRA_ROWS <> 0
    or SQLERRM is NOT NULL
  order by TABLE_NAME`;

const sqlSchemaTableRows = `select relname "TABLE_NAME", n_live_tup "ROW_COUNT" from pg_stat_user_tables where schemaname = $1`;

const sqlCompareSchema = `call COMPARE_SCHEMA($1,$2,$3,$4)`

class PostgresCompare extends PostgresDBI {
    
    constructor(yadamu) {
       super(yadamu);
    }
    
    async recreateSchema(schema,password) {
      try {
        const dropSchema = `drop schema if exists "${schema.schema}" cascade`;
        if (this.status.sqlTrace) {
          this.status.sqlTrace.write(`${dropSchema};\n--\n`)
        }
        await this.pgClient.query(dropSchema);      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
      await this.createSchema(schema.schema);    
    }      

    async importResults(target,timings) {
        
      const results = await this.pgClient.query(sqlSchemaTableRows,[target.schema]);

      return results.rows.map(function(row,idx) {          
        const tableName = (this.parameters.TABLE_MATCHING === 'INSENSITIVE') ? row.TABLE_NAME.toLowerCase() : row.TABLE_NAME;
        const tableTimings = (timings[0][tableName] === undefined) ? { rowCount : -1 } : timings[0][tableName]
        return [target.schema,row.TABLE_NAME,row.ROW_COUNT,tableTimings.rowCount]
      },this)
      
    }
    
    async report(source,target,timingsArray) {

      const report = {
        successful : []
       ,failed     : []
      }

      const timings = timingsArray[timingsArray.length - 1];

      if (this.parameters.TABLE_MATCHING === 'INSENSITIVE') {
        Object.keys(timings).forEach(function(tableName) {
          if (tableName !== tableName.toLowerCase()) {
            timings[tableName.toLowerCase()] = Object.assign({}, timings[tableName])
            delete timings[tableName]
          }
        },this)
      }
      
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(`${sqlCompareSchema};\n--\n`)
      }
      
      await this.pgClient.query(sqlCompareSchema,[source.schema,target.schema,this.parameters.EMPTY_STRING_IS_NULL === true,this.parameters.STRIP_XML_DECLARATION === true])      
      
      const successful = await this.pgClient.query(sqlSuccess)
            
      report.successful = successful.rows.map(function(row,idx) {          
        const tableName = (this.parameters.TABLE_MATCHING === 'INSENSITIVE') ? row.TABLE_NAME.toLowerCase() : row.TABLE_NAME;
        const tableTimings = (timings[tableName] === undefined) ? { elapsedTime : 'N/A', throughput : "-1ms" } : timings[tableName]
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.TARGET_ROW_COUNT,tableTimings.elapsedTime,tableTimings.throughput]
      },this)
      
      const failed = await this.pgClient.query(sqlFailed)

      report.failed = failed.rows.map(function(row,idx) {
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.SOURCE_ROW_COUNT,row.TARGET_ROW_COUNT,row.MISSING_ROWS,row.EXTRA_ROWS,(row.SQLERRM !== null ? row.SQLERRM : '')]
      },this)

      return report
    }
}

module.exports = PostgresCompare
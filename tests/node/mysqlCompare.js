"use strict" 

const MySQLDBI = require('../../mysql/node/mysqlDBI.js');

const sqlSuccess =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'SUCCESSFUL' "RESULTS", TARGET_ROW_COUNT
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
   and MISSING_ROWS = 0
   and EXTRA_ROWS = 0
   and SQLERRM is NULL
order by TABLE_NAME`;

const sqlFailed = 
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'FAILED' "RESULTS", SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, SQLERRM "NOTES"
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
    or MISSING_ROWS <> 0
    or EXTRA_ROWS <> 0
    or SQLERRM is NOT NULL
 order by TABLE_NAME`;

const sqlSchemaTableRows = `select TABLE_NAME, TABLE_ROWS from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = ?`;

class MySQLCompare extends MySQLDBI {
       
    constructor(yadamu) {
       super(yadamu)
    }

    async recreateSchema(schema,password) {
        
      try {
        const dropSchema = `drop schema if exists "${schema.schema}"`;
        await this.executeSQL(dropSchema,{});      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
      const createSchema = `create schema "${schema.schema}"`;
      await this.executeSQL(createSchema,{});      
    }    

    async importResults(target,timingsArray) {

      const timings = timingsArray[timingsArray.length - 1];
      const results = await this.executeSQL(sqlSchemaTableRows,[target.schema]);
      
      Object.keys(timings).forEach(function(tableName) {
        if (tableName !== tableName.toLowerCase()) {
          timings[tableName.toLowerCase()] = Object.assign({}, timings[tableName])
          delete timings[tableName]
        }
      },this)

      return results.map(function(row,idx) {          
        const tableName = (this.parameters.TABLE_MATCHING === 'INSENSITIVE') ? row.TABLE_NAME.toLowerCase() : row.TABLE_NAME;
        const tableTimings = (timings[tableName] === undefined) ? { rowCount : -1 } : timings[tableName]
        return [target.schema,row.TABLE_NAME,row.TABLE_ROWS,tableTimings.rowCount]
      },this)

    }
    
    async report(source,target,timingsArray) {     

      const report = {
        successful : []
       ,failed     : []
      }

      const timings = timingsArray[timingsArray.length - 1];
                     
      Object.keys(timings).forEach(function(tableName) {
        if (tableName !== tableName.toLowerCase()) {
          timings[tableName.toLowerCase()] = Object.assign({}, timings[tableName])
          delete timings[tableName]
        }
      },this)
      
      const sqlStatement = `CALL COMPARE_SCHEMAS(?,?,?, ?);`;					   
      let results = await this.executeSQL(sqlStatement,[source.schema,target.schema,this.parameters.EMPTY_STRING_IS_NULL === true,this.parameters.hasOwnProperty('SPATIAL_PRECISION') ? this.parameters.SPATIAL_PRECISION : 18]);

      const successful = await this.executeSQL(sqlSuccess,{})
          
      report.successful = successful.map(function(row,idx) {          
        const tableName = (this.parameters.TABLE_MATCHING === 'INSENSITIVE') ? row.TABLE_NAME.toLowerCase() : row.TABLE_NAME;
        const tableTimings = (timings[tableName] === undefined) ? { elapsedTime : 'N/A', throughput : "-1ms" } : timings[tableName]
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.TARGET_ROW_COUNT,tableTimings.elapsedTime,tableTimings.throughput]
      },this)

      const failed = await this.executeSQL(sqlFailed,{})

      report.failed = failed.map(function(row,idx) {
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.SOURCE_ROW_COUNT,row.TARGET_ROW_COUNT,row.MISSING_ROWS,row.EXTRA_ROWS,(row.SQLEERM !== undefined ? row.SQLERRM : '')]
      },this)
      
      return report
    }
   
}

module.exports = MySQLCompare
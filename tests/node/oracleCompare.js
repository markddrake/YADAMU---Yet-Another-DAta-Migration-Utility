"use strict" 

const OracleDBI = require('../../oracle/node/oracleDBI.js');

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

const sqlGatherSchemaStats = `begin dbms_stats.gather_schema_stats(ownname => :target); end;`;

const sqlSchemaTableRows = `select TABLE_NAME, NUM_ROWS from ALL_TABLES where OWNER = :target`;

const sqlCompareSchemas = `begin YADAMU_IMPORT.COMPARE_SCHEMAS(:source,:target,:maxTimestampPrecision); end;`;


class OracleCompare extends OracleDBI {
    
    constructor(yadamu) {
       super(yadamu)
    }
    
    configureTest(connectionProperties,testParameters,schema,tableMappings) {
      super.configureTest(connectionProperties,testParameters,this.DEFAULT_PARAMETERS,tableMappings);
    }
    
    async recreateSchema(schema,password) {
        
      try {
        const dropUser = `drop user "${schema.schema}" cascade`;
        await this.executeSQL(dropUser,{});      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
      const createUser = `grant connect, resource, unlimited tablespace to "${schema.schema}" identified by ${password}`;
      await this.executeSQL(createUser,{});      
    }    
    
    
    async importResults(target,timings) {
        
      let args = {target:`"${target.schema}"`}
      await this.executeSQL(sqlGatherSchemaStats,args)
      
      args = {target:target.schema}
      const results = await this.executeSQL(sqlSchemaTableRows,args)
      
      return results.rows.map(function(row,idx) {          
        const tableName = (this.parameters.TABLE_MATCHING === 'INSENSITIVE') ? row[0].toLowerCase() : row[0];
        const tableTimings = (timings[0][tableName] === undefined) ? { rowCount : -1 } : timings[0][tableName]
        return [target.schema,row[0],row[1],tableTimings.rowCount]
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
      
      const args = {source:source.schema,target:target.schema,maxTimestampPrecision:this.parameters.MAX_TIMESTAMP_PRECISION}
      await this.executeSQL(sqlCompareSchemas,args)      

      const successful = await this.executeSQL(sqlSuccess,{})
            
      report.successful = successful.rows.map(function(row,idx) {          
        const tableName = (this.parameters.TABLE_MATCHING === 'INSENSITIVE') ? row[2].toLowerCase() : row[2];
        const tableTimings = (timings[tableName] === undefined) ? { elapsedTime : 'N/A', throughput : "-1ms" } : timings[tableName]
        return [row[0],row[1],row[2],row[4],tableTimings.elapsedTime,tableTimings.throughput]
      },this)
        
      const failed = await this.executeSQL(sqlFailed,{})
      
      report.failed = failed.rows.map(function(row,idx) {
        return [row[0],row[1],row[2],row[4],row[5],row[6],row[7],row[8]]
      },this)
      
      return report
    }
      
}

module.exports = OracleCompare
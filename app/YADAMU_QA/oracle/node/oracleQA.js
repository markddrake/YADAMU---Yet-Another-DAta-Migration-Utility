"use strict" 

const OracleDBI = require('../../../YADAMU/oracle/node/oracleDBI.js');

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

// LEFT Join works in 11.x databases where 'EXTERNAL' column does not exist in ALL_TABLES

const sqlSchemaTableRows = `select att.TABLE_NAME, NUM_ROWS from ALL_TABLES att LEFT JOIN ALL_EXTERNAL_TABlES axt on att.OWNER = axt.OWNER and att.TABLE_NAME = axt.TABLE_NAME where att.OWNER = :target and axt.OWNER is NULL and SECONDARY = 'N'`;

const sqlCompareSchemas = `begin YADAMU_TEST.COMPARE_SCHEMAS(:source,:target,:maxTimestampPrecision); end;`;


class OracleQA extends OracleDBI {
    
    constructor(yadamu) {
       super(yadamu)
    }

	async initialize() {
	  await super.initialize();
	  if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	  }
	}
	
 	async recreateSchema() {
        
      try {
        const dropUser = `drop user "${this.parameters.TO_USER}" cascade`;
        await this.executeSQL(dropUser,{});      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
      const createUser = `grant connect, resource, unlimited tablespace to "${this.parameters.TO_USER}" identified by ${this.connectionProperties.password}`;
      await this.executeSQL(createUser,{});      
    }  

	async compareSchemas(source,target) {

      const report = {
        successful : []
       ,failed     : []
      }
     
      const args = {source:source.schema,target:target.schema,maxTimestampPrecision:this.parameters.TIMESTAMP_PRECISION}
      await this.executeSQL(sqlCompareSchemas,args)      

      const successful = await this.executeSQL(sqlSuccess,{})
            
      report.successful = successful.rows.map(function(row,idx) {          
        return [row[0],row[1],row[2],row[4]]
      },this)
        
      const failed = await this.executeSQL(sqlFailed,{})
      
      report.failed = failed.rows.map(function(row,idx) {
        return [row[0],row[1],row[2],row[4],row[5],row[6],row[7],row[8]]
      },this)
      
      return report
    }
      
    async getRowCounts(target) {
        
      let args = {target:`"${target.schema}"`}
      await this.executeSQL(sqlGatherSchemaStats,args)
      
      args = {target:target.schema}
      const results = await this.executeSQL(sqlSchemaTableRows,args)
      
      return results.rows.map(function(row,idx) {          
        return [target.schema,row[0],row[1]]
      },this)
      
    }

}
	

module.exports = OracleQA
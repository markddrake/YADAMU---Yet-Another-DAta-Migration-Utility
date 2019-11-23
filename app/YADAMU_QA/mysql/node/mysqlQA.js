"use strict" 

const MySQLDBI = require('../../../YADAMU/mysql/node/mysqlDBI.js');

const sqlSuccess =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'SUCCESSFUL' as "RESULTS", TARGET_ROW_COUNT
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
   and MISSING_ROWS = 0
   and EXTRA_ROWS = 0
   and SQLERRM is NULL
order by TABLE_NAME`;

const sqlFailed = 
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'FAILED' as "RESULTS", SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, SQLERRM "NOTES"
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
    or MISSING_ROWS <> 0
    or EXTRA_ROWS <> 0
    or SQLERRM is NOT NULL
 order by TABLE_NAME`;

const sqlSchemaTableRows = `select TABLE_NAME, TABLE_ROWS from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = ?`;

class MySQLQA extends MySQLDBI {

    doTimeout(milliseconds) {
    
       return new Promise(function (resolve,reject) {
        setTimeout(
          function() {
		   resolve();
          },
          milliseconds
       )
     })  
    }
  
    constructor(yadamu) {
       super(yadamu)
    }
	
	async initialize() {
	  await super.initialize();
	  if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	  }
	}
	
    // ### Hack to avoid missng rows when using a new connection to read previously written rows using new connection immediately after closing current connection.....'

    async finalize() {
      super.finalize()
	  await this.doTimeout(100);
	}
	
    async recreateSchema() {
        
      try {
        const dropSchema = `drop schema if exists "${this.parameters.TO_USER}"`;
        await this.executeSQL(dropSchema,{});      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
      const createSchema = `create schema "${this.parameters.TO_USER}"`;
      await this.executeSQL(createSchema,{});      
    }    

    async compareSchemas(source,target) {     

      const report = {
        successful : []
       ,failed     : []
      }

      const sqlStatement = `CALL COMPARE_SCHEMAS(?,?,?, ?);`;					   
      let results = await this.executeSQL(sqlStatement,[source.schema,target.schema,this.parameters.EMPTY_STRING_IS_NULL === true,this.parameters.hasOwnProperty('SPATIAL_PRECISION') ? this.parameters.SPATIAL_PRECISION : 18]);

      const successful = await this.executeSQL(sqlSuccess,{})
          
      report.successful = successful.map(function(row,idx) {          
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.TARGET_ROW_COUNT]
      },this)

      const failed = await this.executeSQL(sqlFailed,{})

      report.failed = failed.map(function(row,idx) {
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.SOURCE_ROW_COUNT,row.TARGET_ROW_COUNT,row.MISSING_ROWS,row.EXTRA_ROWS,(row.SQLEERM !== undefined ? row.SQLERRM : '')]
      },this)
      
      return report
    }
   
    async getRowCounts(target) {

      const results = await this.executeSQL(sqlSchemaTableRows,[target.schema]);
      
      return results.map(function(row,idx) {          
        return [target.schema,row.TABLE_NAME,row.TABLE_ROWS]
      },this)

    }
    

}

module.exports = MySQLQA
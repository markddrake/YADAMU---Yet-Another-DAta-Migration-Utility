
import YadamuCompare   from '../base/yadamuCompare.js'

class MsSQLCompare extends YadamuCompare {

    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }

    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

    async useDatabase(databaseName) {     
      const statement = `use ${databaseName}`
      const results = await this.dbi.executeSQL(statement);
    } 
	
   async getRowCounts(target) {
     await this.useDatabase(target.database);
     const results = await this.dbi.pool.request().input('SCHEMA',this.dbi.sql.VarChar,target.owner).query(MsSQLCompare.SQL_SCHEMA_TABLE_ROWS);
     return results.recordset.map((row,idx) => {          
       return [target.owner === 'dbo' ? target.database : target.owner,row.TableName,parseInt(row.RowCount)]
     })
   }

   async compareSchemas(source,target,rules) {

      const report = {
        successful : []
       ,failed     : []
      }

      await this.useDatabase(source.database);
      
      let compareRules = this.formatCompareRules(rules)   
      compareRules = this.dbi.DATABASE_VERSION  > 12 ? JSON.stringify(compareRules) : this.makeXML(compareRules)

      
      let args = 
`--
-- declare @FORMAT_RESULTS         bit           = 0;
-- declare @SOURCE_DATABASE        varchar(128)  = '${source.database}';
-- declare @SOURCE_SCHEMA          varchar(128)  = '${source.owner}';
-- declare @TARGET_DATABASE        varchar(128)  = '${target.database}';
-- declare @TARGET_SCHEMA          varchar(128)  = '${target.owner}';
-- declare @COMMENT                varchar(128)  = '';
-- declare @RULES                  narchar(4000) = '${compareRules}';
--`;
            
      this.dbi.SQL_TRACE.trace(`${args}\nexecute sp_COMPARE_SCHEMA(@FORMAT_RESULTS,@SOURCE_DATABASE,@SOURCE_SCHEMA,@TARGET_DATABASE,@TARGET_SCHEMA,@COMMENT,@EMPTY_STRING_IS_NULL,@SPATIAL_PRECISION,@DATE_TIME_PRECISION)\ngo\n`)

      const request = this.dbi.getRequest();
      
      let results = await request
                          .input('FORMAT_RESULTS',this.dbi.sql.Bit,false)
                          .input('SOURCE_DATABASE',this.dbi.sql.VarChar,source.database)
                          .input('SOURCE_SCHEMA',this.dbi.sql.VarChar,source.owner)
                          .input('TARGET_DATABASE',this.dbi.sql.VarChar,target.database)
                          .input('TARGET_SCHEMA',this.dbi.sql.VarChar,target.owner)
                          .input('COMMENT',this.dbi.sql.VarChar,'')
                          .input('RULES',this.dbi.sql.VarChar,compareRules)
                          .execute(MsSQLCompare.SQL_COMPARE_SCHEMAS,{},{resultSet: true});

      // Use length-2 and length-1 to allow Debugging info to be included in the output
      
      // console.log(results.recordsets[0])
      
      const successful = results.recordsets[results.recordsets.length-2]      
      report.successful = successful.map((row,idx) => {          
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.TARGET_ROW_COUNT,]
      })
        
      const failed = results.recordsets[results.recordsets.length-1]
      report.failed = failed.map((row,idx) => {
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.SOURCE_ROW_COUNT,row.TARGET_ROW_COUNT,row.MISSING_ROWS,row.EXTRA_ROWS,(row.SQLERRM !== null ? row.SQLERRM : '')]
      })

      return report
    }

}
export { MsSQLCompare as default }

const _SQL_SCHEMA_TABLE_ROWS = `SELECT so.name AS [TableName], SUM(ptn.Rows) AS [RowCount] 
   FROM sys.objects AS so 
  INNER JOIN sys.partitions AS ptn	 
     ON so.object_id = ptn.object_id 
  WHERE so.type = 'U' 
    AND so.schema_id = SCHEMA_ID(@SCHEMA) 
    AND so.is_ms_shipped = 0x0
    AND index_id < 2
 GROUP BY so.schema_id, so.name`;
 
const _SQL_COMPARE_SCHEMAS = `sp_COMPARE_SCHEMA`

						  





import YadamuCompare   from '../base/yadamuCompare.js'

class SnowflakeCompare extends YadamuCompare {    static get SQL_SCHEMA_TABLE_NAMES()    { return _SQL_SCHEMA_TABLE_NAMES }

	static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }

    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

    async getRowCounts(target) {
	  
	  target = this.dbi.getSchema(target)

      const useDatabase = `USE DATABASE "${target.database}";`;	  
      let results =  await this.dbi.executeSQL(useDatabase,[]);      
         
      results = await this.dbi.executeSQL(SnowflakeCompare.SQL_SCHEMA_TABLE_ROWS,[target.schema]); 
      return results.map((row,idx) => {          
        return [target.schema,row.TABLE_NAME,parseInt(row.ROW_COUNT)]
      })
    }
    
    async compareSchemas(source,target,rules) {

      source = this.dbi.getSchema(source)
	  target = this.dbi.getSchema(target)
    
      const compareRules = JSON.stringify(this.formatCompareRules(rules))  
 	
      const useDatabase = `use database "${source.database}";`;
      let results =  await this.dbi.executeSQL(useDatabase,[]);      
         
      const report = {
        successful : []
       ,failed     : []
      }
      results = await this.dbi.executeSQL(SnowflakeCompare.SQL_COMPARE_SCHEMAS,[source.database,source.schema,target.schema,compareRules]);

      let compare = JSON.parse(results[0].COMPARE_SCHEMAS)
      compare.forEach((result) => {
        if ((result[3] === result[4]) && (result[5] === 0) && (result[6] === 0)) {
          report.successful.push([result[0],result[1],result[2],result[4]]);
        } 
        else {
          report.failed.push(result)
        }
      })
     
      return report
    }

}
export { SnowflakeCompare as default }

const _SQL_COMPARE_SCHEMAS = `call YADAMU_SYSTEM.PUBLIC.COMPARE_SCHEMAS(:1,:2,:3,:4);`
const _SQL_SCHEMA_TABLE_ROWS = `select TABLE_NAME, ROW_COUNT from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = 'BASE TABLE' and TABLE_SCHEMA = ?`;


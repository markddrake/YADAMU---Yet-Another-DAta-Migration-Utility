
import YadamuCompare   from '../base/yadamuCompare.js'

class SnowflakeCompare extends YadamuCompare { 

    static #SQL_COMPARE_SCHEMAS   = `call YADAMU_SYSTEM.PUBLIC.COMPARE_SCHEMAS(:1,:2,:3,:4);`
    static #SQL_SCHEMA_TABLE_ROWS = `select TABLE_NAME, ROW_COUNT from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = 'BASE TABLE' and TABLE_SCHEMA = ?`;

	static get SQL_SCHEMA_TABLE_ROWS()     { return this.#SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return this.#SQL_COMPARE_SCHEMAS }

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
	  // console.log(compare)
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


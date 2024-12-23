
import oracledb from 'oracledb';
oracledb.fetchAsString = [ oracledb.DATE, oracledb.NUMBER ]

import YadamuCompare   from '../base/yadamuCompare.js'

class OracleCompare extends YadamuCompare {
	
    static #SQL_COMPARE_SCHEMAS = `begin YADAMU_COMPARE.COMPARE_SCHEMAS(:P_SOURCE_SCHEMA, :P_TARGET_SCHEMA, :P_RULES); end;`;

    static #SQL_SUCCESS =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'SUCCESSFUL', TARGET_ROW_COUNT
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
   and MISSING_ROWS = 0
   and EXTRA_ROWS = 0
   and SQLERRM is NULL
order by TABLE_NAME`;

    static #SQL_FAILED = 
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, 'FAILED', SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, SQLERRM
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
    or MISSING_ROWS <> 0
    or EXTRA_ROWS <> 0
    or SQLERRM is NOT NULL
 order by TABLE_NAME`;

    static #SQL_GATHER_SCHEMA_STATS = `begin dbms_stats.gather_schema_stats(ownname => :target); end;`;

// LEFT Join works in 11.x databases where 'EXTERNAL' column does not exist in ALL_TABLES

    static #SQL_SCHEMA_TABLE_ROWS = 
`select att.TABLE_NAME, coalesce(NUM_ROWS,0) NUM_ROWS
   from ALL_ALL_TABLES att 
   LEFT JOIN ALL_EXTERNAL_TABlES axt 
	 on att.OWNER = axt.OWNER and att.TABLE_NAME = axt.TABLE_NAME 
 where att.OWNER = :target 
   and axt.OWNER is NULL 
   and att.SECONDARY = 'N' 
  and att.DROPPED = 'NO'
   and att.TEMPORARY = 'N'
   and att.NESTED = 'NO'
   and (att.IOT_TYPE is NULL or att.IOT_TYPE = 'IOT')`;
						  
    static get SQL_SCHEMA_TABLE_ROWS()     { return this.#SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return this.#SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return this.#SQL_SUCCESS }
    static get SQL_FAILED()                { return this.#SQL_FAILED }
    static get SQL_GATHER_SCHEMA_STATS()   { return this.#SQL_GATHER_SCHEMA_STATS }

    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}
    			
    async getRowCounts(target) {
        
      let args = {target:`"${target}"`}
      await this.dbi.executeSQL(OracleCompare.SQL_GATHER_SCHEMA_STATS,args)
      
      args = {target:target}
      const results = await this.dbi.executeSQL(OracleCompare.SQL_SCHEMA_TABLE_ROWS,args)
      return results.rows.map((row,idx) => {          
        return [target,row[0],parseInt(row[1])]
      })
      
    }

	async compareSchemas(source,target,rules) {
	
	  let compareRules = this.formatCompareRules(rules)	  
	  
	  compareRules.objectsRule   = rules.OBJECTS_COMPARISON_RULE || 'SKIP'
	  
	  // Exclude Materialzied Views from the Comparrison when the copy operation did not detect materialized views present.
	  
	  compareRules.excludeMViews = !this.configuration.includeMaterializedViews
	  compareRules = this.dbi.JSON_PARSING_SUPPORTED ? JSON.stringify(compareRules) : this.makeXML(compareRules)
      
	  const args = {
		P_SOURCE_SCHEMA        : source,
		P_TARGET_SCHEMA        : target,
		P_RULES                : compareRules
	  }
	        
      const report = {
        successful : []
       ,failed     : []
      }

      let retryCount = 0 
	  while (retryCount < 5) {
	    try {
	      await this.dbi.executeSQL(OracleCompare.SQL_COMPARE_SCHEMAS,args)      
		  break
		} catch (e) {
		  // ComapareSchema throws missing table on the Global TemporaryTable following an ORA-3113 and getting a new connection from the Pool. 
		  // Problem apperas to be reslved by closing and re-opening the connection Pool.
		  if (e.missingTable()) {
			this.LOGGER.handleWarning([`COMPARE`,`${this.dbi.DATABASE_VENDOR}`],e)
			retryCount++
			await this.dbi.closeConnection()
			await this.dbi.closePool(0)
			await this.dbi.createConnectionPool();
			this.dbi.connection = await this.dbi.getConnectionFromPool();
		    continue
		  }
		  throw e
		}
	  }
	  
      const successful = await this.dbi.executeSQL(OracleCompare.SQL_SUCCESS,{})
            
      report.successful = successful.rows.map((row,idx) => {          
        return [row[0],row[1],row[2],parseInt(row[4])]
      })
        
	  
	  const options = {fetchInfo:[{type: oracledb.STRING, maxSize: 128},{type: oracledb.STRING, maxSize: 128},{type: oracledb.STRING, maxSize: 128},{type: oracledb.STRING, maxSize: 12},{type: oracledb.NUMBER},{type: oracledb.NUMBER},{type: oracledb.NUMBER},{type: oracledb.NUMBER},{type: oracledb.STRING, maxSize: 16*1024*1024}]}
      // const failed = await this.dbi.executeSQL(OracleCompare.SQL_FAILED,{},options)      
      const failed = await this.dbi.executeSQL(OracleCompare.SQL_FAILED,{})      
      report.failed = await Promise.all(failed.rows.map(async (row,idx) => {
		const result = [row[0],row[1],row[2],parseInt(row[4]),parseInt(row[5]),parseInt(row[6]),parseInt(row[7]),((row[8] === null) || (typeof row[8] === 'string')) ? row[8] : row[8].getData()]
		return await Promise.all(result)
      }))
	  
      return report
    }
     
}
export { OracleCompare as default }


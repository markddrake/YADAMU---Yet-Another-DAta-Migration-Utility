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

const sqlCompareSchemas = `begin YADAMU_IMPORT.COMPARE_SCHEMAS(:source,:target); end;`;


class OracleCompare extends OracleDBI {
    
    constructor(yadamu) {
       super(yadamu)
       this.logger = undefined;
    }
    
    configureTest(logger,connectionProperties,testParameters,schema) {
      this.logger = logger;
      super.configureTest(connectionProperties,testParameters,this.DEFAULT_PARAMETERS);
    }
    
    async recreateSchema(schema,password) {
        
      try {
        const dropUser = `drop user "${schema}" cascade`;
        await this.executeSQL(dropUser,{});      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
      const createUser = `grant connect, resource, unlimited tablespace to "${schema}" identified by ${password}`;
      await this.executeSQL(createUser,{});      
    }    
    
    
    async importResults(target,timings) {
        
      const colSizes = [32, 48, 14, 14]
      
      let seperatorSize = (colSizes.length * 3) - 1;
      colSizes.forEach(function(size) {
        seperatorSize += size;
      },this);

      let args = {target:`"${target}"`}
      await this.executeSQL(sqlGatherSchemaStats,args)
      
      args = {target:target}
      const results = await this.executeSQL(sqlSchemaTableRows,args)
      
      results.rows.forEach(function(row,idx) {          
        const tableName = (this.parameters.TABLE_MATCHING === 'INSENSITIVE') ? row[0].toLowerCase() : row[0];
        const tableTimings = (timings[0][tableName] === undefined) ? { rowCount : -1 } : timings[0][tableName]
        if (idx === 0) {
          this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
          this.logger.write(`|`
                          + ` ${'TARGET SCHEMA'.padStart(colSizes[0])} |` 
                          + ` ${'TABLE_NAME'.padStart(colSizes[1])} |`
                          + ` ${'ROWS'.padStart(colSizes[2])} |`
                          + ` ${'ROWS IMPORTED'.padStart(colSizes[3])} |`
                          + '\n');
          this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
          this.logger.write(`|`
                          + ` ${target.padStart(colSizes[0])} |`
                          + ` ${row[0].padStart(colSizes[1])} |`
                          + ` ${row[1].toString().padStart(colSizes[2])} |` 
                          + ` ${tableTimings.rowCount.toString().padStart(colSizes[3])} |` 
                          + '\n');
        }
        else {
          this.logger.write(`|`
                          + ` ${''.padStart(colSizes[0])} |`
                          + ` ${row[0].padStart(colSizes[1])} |`
                          + ` ${row[1].toString().padStart(colSizes[2])} |` 
                          + ` ${tableTimings.rowCount.toString().padStart(colSizes[3])} |` 
                          + '\n');
          
        }
      },this)

      if (results.rows.length > 0) {
        this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
      }
    }

    async report(source,target,timingsArray) {

      const colSizes = [12, 32, 32, 48, 14, 14, 14, 14, 72]
      
      const timings = timingsArray[timingsArray.length - 1];

      if (this.parameters.TABLE_MATCHING === 'INSENSITIVE') {
        Object.keys(timings).forEach(function(tableName) {
          if (tableName !== tableName.toLowerCase()) {
            timings[tableName.toLowerCase()] = Object.assign({}, timings[tableName])
            delete timings[tableName]
          }
        },this)
      }
      
      const args = {source:source,target:target}
      await this.executeSQL(sqlCompareSchemas,args)      
      const successful = await this.executeSQL(sqlSuccess,{})
      const failed = await this.executeSQL(sqlFailed,{})
            
      let seperatorSize = (colSizes.slice(0,7).length *3) - 1
      colSizes.slice(0,7).forEach(function(size) {
        seperatorSize += size;
      },this);     
       
      successful.rows.forEach(function(row,idx) {          
        const tableName = (this.parameters.TABLE_MATCHING === 'INSENSITIVE') ? row[2].toLowerCase() : row[2];
        const tableTimings = (timings[tableName] === undefined) ? { elapsedTime : 'N/A', throughput : "-1ms" } : timings[tableName]
        if (idx === 0) {
          this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
          this.logger.write(`|`
                          + ` ${'RESULT'.padEnd(colSizes[0])} |`
                          + ` ${'SOURCE SCHEMA'.padStart(colSizes[1])} |`
                          + ` ${'TARGET SCHEMA'.padStart(colSizes[2])} |` 
                          + ` ${'TABLE_NAME'.padStart(colSizes[3])} |`
                          + ` ${'ROWS'.padStart(colSizes[4])} |`
                          + ` ${'ELAPSED TIME'.padStart(colSizes[5])} |`
                          + ` ${'THROUGHPUT'.padStart(colSizes[6])} |`
                          + '\n');
          this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
          this.logger.write(`|`
                          + ` ${'SUCCESSFUL'.padEnd(colSizes[0])} |`
                          + ` ${row[0].padStart(colSizes[1])} |`
                          + ` ${row[1].padStart(colSizes[2])} |`)
        }
        else {
          this.logger.write(`|`
                          + ` ${''.padEnd(colSizes[0])} |`
                          + ` ${''.padStart(colSizes[1])} |`
                          + ` ${''.padStart(colSizes[2])} |` )
                          
        }

        this.logger.write(` ${row[2].padStart(colSizes[3])} |` 
                        + ` ${row[4].toString().padStart(colSizes[4])} |` 
                        + ` ${tableTimings.elapsedTime.padStart(colSizes[5])} |` 
                        + ` ${tableTimings.throughput.padStart(colSizes[6])} |` 
                        + '\n');
      },this)
        
      if (successful.rows.length > 0) {
        this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
      }
      
      seperatorSize = (colSizes.length * 3) - 1;
      colSizes.forEach(function(size) {
        seperatorSize += size;
      },this);
      
      failed.rows.forEach(function(row,idx) {
        if (idx === 0) {
          this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
          this.logger.write(`|`
                          + ` ${'RESULT'.padEnd(colSizes[0])} |`
                          + ` ${'SOURCE SCHEMA'.padStart(colSizes[1])} |`
                          + ` ${'TARGET SCHEMA'.padStart(colSizes[2])} |` 
                          + ` ${'TABLE_NAME'.padStart(colSizes[3])} |`
                          + ` ${'SOURCE ROWS'.padStart(colSizes[4])} |`
                          + ` ${'TARGET ROWS'.padStart(colSizes[5])} |`
                          + ` ${'MISSING ROWS'.padStart(colSizes[6])} |`
                          + ` ${'EXTRA ROWS'.padStart(colSizes[7])} |`
                          + ` ${'NOTES'.padEnd(colSizes[8])} |`
                          + '\n');
          this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
          this.logger.write(`|`
                          + ` ${'FAILED'.padEnd(colSizes[0])} |`
                          + ` ${row[0].padStart(colSizes[1])} |`
                          + ` ${row[1].padStart(colSizes[2])} |`) 
        }
        else {
          this.logger.write(`|`
                          + ` ${''.padEnd(colSizes[0])} |`
                          + ` ${''.padStart(colSizes[1])} |`
                          + ` ${''.padStart(colSizes[2])} |`)
        }

        this.logger.write(` ${row[2].padStart(colSizes[3])} |` 
                        + ` ${row[4].toString().padStart(colSizes[4])} |` 
                        + ` ${row[5].toString().padStart(colSizes[5])} |` 
                        + ` ${row[6].toString().padStart(colSizes[6])} |` 
                        + ` ${row[7].toString().padStart(colSizes[7])} |` 
                        + ` ${(row[8] !== null ? row[8] :  '').padEnd(colSizes[8])} |` 
                        + '\n');

                        if (idx+1 === failed.rows.length) {
          this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
        }
      },this)
      
      if (failed.rows.lngth > 0) {
        this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
      }
    }
      
}

module.exports = OracleCompare
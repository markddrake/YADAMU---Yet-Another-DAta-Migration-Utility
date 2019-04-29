"use strict" 

const Yadamu = require('../../common/yadamu.js').Yadamu;
const MsSQLDBI = require('../../mssql/node/mssqlDBI.js');

const colSizes = [12, 32, 32, 48, 14, 14, 14, 14, 72]

class MsSQLCompare extends MsSQLDBI {
    
    constructor(yadamu) {
       super(yadamu)
       this.logger = undefined
    }
    
    configureTest(logger,connectionProperties,testParameters,schema) {
       this.logger = logger;
       if (schema !== undefined) {
         connectionProperties.database = schema.schema;
       }
       super.configureTest(connectionProperties,testParameters,this.DEFAULT_PARAMETERS);
    }
    
    async recreateSchema(schema,password) {
       
      let results;       
      const dropDatabase = `drop database if exists "${schema.schema}"`;
      if (this.status.sqlTrace) {
         this.status.sqlTrace.write(`${dropDatabase}\ngo\n`)
      }
      results =  await this.pool.request().batch(dropDatabase);      
      
      const createDatabase = `create database "${schema.schema}"`;
      if (this.status.sqlTrace) {
         this.status.sqlTrace.write(`${createDatabase}\ngo\n`)
      }
      results = await this.pool.request().batch(createDatabase);      
       
   }

    async report(source,target,timingsArray) {

      const timings = timingsArray[timingsArray.length - 1];
       
      if (this.parameters.TABLE_MATCHING === 'INSENSITIVE') {
        Object.keys(timings).forEach(function(tableName) {
          if (tableName !== tableName.toUpperCase()) {
            timings[tableName.toUpperCase()] = Object.assign({}, timings[tableName])
            delete timings[tableName]
          }
        },this)
      }
      
      this.useDatabase(this.pool,source.schema,this.status);

      let args = 
`--@FORMAT_RESULTS = false
--@SOURCE_DATABASE = '${source.schema}'
--@SOURCE_SCHEMA   = '${source.owner}'
--@TARGET_DATABASE = '${target.schema}'
--@TARGET_SCHEMA   = '${target.owner}'
--@COMMENT         = ''
--`;
                 
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(`${args}\nexecute sp_COMPARE_SCHEMA(@FORMAT_RESULTS,@SOURCE_DATABASE,@SOURCE_SCHEMA,@TARGET_DATABASE,@TARGET_SCHEMA,@COMMENT)\ngo\n`)
      }

      const request = this.pool.request();
      let results = await request
                          .input('FORMAT_RESULTS',this.sql.Bit,false)
                          .input('SOURCE_DATABASE',this.sql.VarChar,source.schema)
                          .input('SOURCE_SCHEMA',this.sql.VarChar,source.owner)
                          .input('TARGET_DATABASE',this.sql.VarChar,target.schema)
                          .input('TARGET_SCHEMA',this.sql.VarChar,target.owner)
                          .input('COMMENT',this.sql.VarChar,'')
                          .execute('sp_COMPARE_SCHEMA',{},{resultSet: true});

      const successful = results.recordsets[0]
      const failed = results.recordsets[1]

      let seperatorSize = (colSizes.slice(0,7).length *3) - 1
      colSizes.slice(0,7).forEach(function(size) {
        seperatorSize += size;
      },this);
      
      successful.forEach(function(row,idx) {   
        const tableName = (this.parameters.TABLE_MATCHING === 'INSENSITIVE') ? row.TABLE_NAME.toUpperCase() : row.TABLE_NAME;
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
                          + ` ${row.SOURCE_SCHEMA.padStart(colSizes[1])} |`
                          + ` ${row.TARGET_SCHEMA.padStart(colSizes[2])} |`);
        }
        else {
          this.logger.write(`|`
                          + ` ${''.padEnd(colSizes[0])} |`
                          + ` ${''.padStart(colSizes[1])} |`
                          + ` ${''.padStart(colSizes[2])} |`)
        }
                      
        this.logger.write(` ${row.TABLE_NAME.padStart(colSizes[3])} |` 
                        + ` ${row.TARGET_ROW_COUNT.toString().padStart(colSizes[4])} |` 
                        + ` ${tableTimings.elapsedTime.padStart(colSizes[5])} |` 
                        + ` ${tableTimings.throughput.padStart(colSizes[6])} |` 
                        + '\n');

                        if (idx+1 === successful.length) {
          this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
        }
      },this)
        
      seperatorSize = (colSizes.length * 3) - 1;
      colSizes.forEach(function(size) {
        seperatorSize += size;
      },this);
       
      failed.forEach(function(row,idx) {
        if (idx === 0) {
          this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
          this.logger.write(`|`
                          + ` ${'RESULT'.padEnd(colSizes[0])} |`
                          + ` ${'SOURCE SCHEMA'.padStart(colSizes[1])} |`
                          + ` ${'TARGET SCHEMA'.padStart(colSizes[2])} |` 
                          + ` ${'TABLE_NAME'.padStart(colSizes[3])} |`
                          + ` ${'SOURCE ROWS'.padStart(colSizes[5])} |`
                          + ` ${'TARGET ROWS'.padStart(colSizes[6])} |`
                          + ` ${'MISSING ROWS'.padStart(colSizes[7])} |`
                          + ` ${'EXTRA ROWS'.padStart(colSizes[8])} |`
                          + ` ${'NOTES'.padEnd(colSizes[9])} |`
                          + '\n');
          this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n') 
          this.logger.write(`|`
                          + ` ${'FAILED'.padEnd(colSizes[0])} |`
                          + ` ${row.SOURCE_SCHEMA.padStart(colSizes[1])} |`
                          + ` ${row.TARGET_SCHEMA.padStart(colSizes[2])} |`)
        }
        else {
          this.logger.write(`|`
                          + ` ${''.padEnd(colSizes[0])} |`
                          + ` ${''.padStart(colSizes[1])} |`
                          + ` ${''.padStart(colSizes[2])} |`)
        }

        this.logger.write(` ${row.TABLE_NAME.padStart(colSizes[3])} |` 
                        + ` ${row.SOURCE_ROW_COUNT.toString().padStart(colSizes[5])} |` 
                        + ` ${row.TARGET_ROW_COUNT.toString().padStart(colSizes[6])} |` 
                        + ` ${row.MISSING_ROWS.toString().padStart(colSizes[7])} |` 
                        + ` ${row.EXTRA_ROWS.toString().padStart(colSizes[8])} |` 
                        + ` ${(row.SQLERRM !== null ? row.SQLERRM : '').padEnd(colSizes[9])} |` 
                        + '\n');

        if (idx+1 === failed.length) {
          this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
        }
      },this)
    }
   
}

module.exports = MsSQLCompare
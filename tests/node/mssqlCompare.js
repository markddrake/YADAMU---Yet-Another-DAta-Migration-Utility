"use strict" 

const MsSQLDBI = require('../../mssql/node/mssqlDBI.js');

const sqlSchemaTableRows = `SELECT sOBJ.name AS [TableName], SUM(sPTN.Rows) AS [RowCount] 
   FROM sys.objects AS sOBJ 
  INNER JOIN sys.partitions AS sPTN ON sOBJ.object_id = sPTN.object_id 
  WHERE sOBJ.type = 'U' 
    AND sOBJ.schema_id = SCHEMA_ID(@SCHEMA) 
    AND sOBJ.is_ms_shipped = 0x0
    AND index_id < 2
 GROUP BY sOBJ.schema_id, sOBJ.name`;

const sqlCompareSchema = `sp_COMPARE_SCHEMA`

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

    async importResults(target,timings) {
        
      const colSizes = [32, 48, 14, 14]
      
      let seperatorSize = (colSizes.length * 3) - 1;
      colSizes.forEach(function(size) {
        seperatorSize += size;
      },this);

      await this.useDatabase(this.pool,target.schema,this.status);
      const results = await this.pool.request().input('SCHEMA',this.sql.VarChar,target.owner).query(sqlSchemaTableRows);

      results.recordset.forEach(function(row,idx) {          
        const tableName = (this.parameters.TABLE_MATCHING === 'INSENSITIVE') ? row.TableName.toLowerCase() : row.TableName;
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
                          + ` ${target.schema.padStart(colSizes[0])} |`
                          + ` ${row.TableName.padStart(colSizes[1])} |`
                          + ` ${row.RowCount.toString().padStart(colSizes[2])} |` 
                          + ` ${tableTimings.rowCount.toString().padStart(colSizes[3])} |` 
                          + '\n');
        }
        else {
          this.logger.write(`|`
                          + ` ${''.padStart(colSizes[0])} |`
                          + ` ${row.TableName.padStart(colSizes[1])} |`
                          + ` ${row.RowCount.toString().padStart(colSizes[2])} |` 
                          + ` ${tableTimings.rowCount.toString().padStart(colSizes[3])} |` 
                          + '\n');          
        }
      },this)
  
      if (results.recordset.length > 0) {
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
      
      await this.useDatabase(this.pool,source.schema,this.status);

      let args = 
`--@FORMAT_RESULTS        = false
--@SOURCE_DATABASE        = '${source.schema}'
--@SOURCE_SCHEMA          = '${source.owner}'
--@TARGET_DATABASE        = '${target.schema}'
--@TARGET_SCHEMA          = '${target.owner}'
--@COMMENT                = ''
--@EMPTY_STRING_IS_NULL   = ${this.parameters.EMPTY_STRING_IS_NULL === true}
--@SPATIAL_PRECISION      = ${this.parameters.hasOwnProperty('SPATIAL_PRECISION') ? this.parameters.SPATIAL_PRECISION : null}
--`;
                 
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(`${args}\nexecute sp_COMPARE_SCHEMA(@FORMAT_RESULTS,@SOURCE_DATABASE,@SOURCE_SCHEMA,@TARGET_DATABASE,@TARGET_SCHEMA,@COMMENT,@EMPTY_STRING_IS_NULL,@SPATIAL_PRECISION)\ngo\n`)
      }

      const request = this.pool.request();
      let results = await request
                          .input('FORMAT_RESULTS',this.sql.Bit,false)
                          .input('SOURCE_DATABASE',this.sql.VarChar,source.schema)
                          .input('SOURCE_SCHEMA',this.sql.VarChar,source.owner)
                          .input('TARGET_DATABASE',this.sql.VarChar,target.schema)
                          .input('TARGET_SCHEMA',this.sql.VarChar,target.owner)
                          .input('COMMENT',this.sql.VarChar,'')
                          .input('EMPTY_STRING_IS_NULL',this.sql.Bit,(this.parameters.EMPTY_STRING_IS_NULL === true ? 1 : 0))
                          .input('SPATIAL_PRECISION',this.sql.Int,(this.parameters.hasOwnProperty('SPATIAL_PRECISION') ? this.parameters.SPATIAL_PRECISION : null))
                          .execute(sqlCompareSchema,{},{resultSet: true});

      const successful = results.recordsets[0]
      const failed = results.recordsets[1]

      let seperatorSize = (colSizes.slice(0,7).length *3) - 1
      colSizes.slice(0,7).forEach(function(size) {
        seperatorSize += size;
      },this);
      
      successful.forEach(function(row,idx) {   
        const tableName = (this.parameters.TABLE_MATCHING === 'INSENSITIVE') ? row.TABLE_NAME.toLowerCase() : row.TABLE_NAME;
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
      },this)
        
      if (successful.length > 0) {
        this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
      }
      
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
                          + ` ${'SOURCE ROWS'.padStart(colSizes[4])} |`
                          + ` ${'TARGET ROWS'.padStart(colSizes[5])} |`
                          + ` ${'MISSING ROWS'.padStart(colSizes[6])} |`
                          + ` ${'EXTRA ROWS'.padStart(colSizes[7])} |`
                          + ` ${'NOTES'.padEnd(colSizes[8])} |`
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
                        + ` ${row.SOURCE_ROW_COUNT.toString().padStart(colSizes[4])} |` 
                        + ` ${row.TARGET_ROW_COUNT.toString().padStart(colSizes[5])} |` 
                        + ` ${row.MISSING_ROWS.toString().padStart(colSizes[6])} |` 
                        + ` ${row.EXTRA_ROWS.toString().padStart(colSizes[7])} |` 
                        + ` ${(row.SQLERRM !== null ? row.SQLERRM : '').padEnd(colSizes[8])} |` 
                        + '\n');
      },this)

      if (failed.length > 0) {
        this.logger.write('+' + '-'.repeat(seperatorSize) + '+' + '\n\n') 
      }
    }
   
}

module.exports = MsSQLCompare
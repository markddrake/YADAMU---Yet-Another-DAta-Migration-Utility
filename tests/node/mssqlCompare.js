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
    }
    
    configureTest(connectionProperties,testParameters,connectInfo,tableMappings) {
       if (connectInfo !== undefined) {
		 connectionProperties.database = connectInfo.database ? connectInfo.database : connectInfo.schema
       }
       super.configureTest(connectionProperties,testParameters,tableMappings);
    }
    
    async recreateSchema(connectInfo,password) {
             
      let results;       
      const dropDatabase = `drop database if exists "${connectInfo.database}"`;
      if (this.status.sqlTrace) {
         this.status.sqlTrace.write(`${dropDatabase}\ngo\n`)
      }
      results =  await this.pool.request().batch(dropDatabase);      
      
      const createDatabase = `create database "${connectInfo.database}"`;
      if (this.status.sqlTrace) {
         this.status.sqlTrace.write(`${createDatabase}\ngo\n`)
      }
      results = await this.pool.request().batch(createDatabase);      
       
   }

    async importResults(connectInfo,timings) {
        
      await this.useDatabase(connectInfo.database);
      const results = await this.pool.request().input('SCHEMA',this.sql.VarChar,connectInfo.owner).query(sqlSchemaTableRows);
      
      return results.recordset.map(function(row,idx) {          
        const tableName = (this.parameters.TABLE_MATCHING === 'INSENSITIVE') ? row.TableName.toLowerCase() : row.TableName;
        const tableTimings = (timings[0][tableName] === undefined) ? { rowCount : -1 } : timings[0][tableName]
        return [connectInfo.database,row.TableName,row.RowCount,tableTimings.rowCount]
      },this)
    }
    
    async report(sourceInfo,targetInfo,timingsArray) {

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
      
      await this.useDatabase(sourceInfo.database);

      let args = 
`--
--@FORMAT_RESULTS         = false
--@SOURCE_DATABASE        = '${sourceInfo.database}'
--@SOURCE_SCHEMA          = '${sourceInfo.owner}'
--@TARGET_DATABASE        = '${targetInfo.database}'
--@TARGET_SCHEMA          = '${targetInfo.owner}'
--@COMMENT                = ''
--@EMPTY_STRING_IS_NULL   = ${this.parameters.EMPTY_STRING_IS_NULL === true}
--@SPATIAL_PRECISION      = ${this.parameters.hasOwnProperty('SPATIAL_PRECISION') ? this.parameters.SPATIAL_PRECISION : null}
--@DATE_TIME_PRECISION    = ${this.parameters.MAX_TIMESTAMP_PRECISION}
--`;
            
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(`${args}\nexecute sp_COMPARE_SCHEMA(@FORMAT_RESULTS,@SOURCE_DATABASE,@SOURCE_SCHEMA,@TARGET_DATABASE,@TARGET_SCHEMA,@COMMENT,@EMPTY_STRING_IS_NULL,@SPATIAL_PRECISION)\ngo\n`)
      }

      const request = this.pool.request();
      let results = await request
                          .input('FORMAT_RESULTS',this.sql.Bit,false)
                          .input('SOURCE_DATABASE',this.sql.VarChar,sourceInfo.database)
                          .input('SOURCE_SCHEMA',this.sql.VarChar,sourceInfo.owner)
                          .input('TARGET_DATABASE',this.sql.VarChar,targetInfo.database)
                          .input('TARGET_SCHEMA',this.sql.VarChar,targetInfo.owner)
                          .input('COMMENT',this.sql.VarChar,'')
                          .input('EMPTY_STRING_IS_NULL',this.sql.Bit,(this.parameters.EMPTY_STRING_IS_NULL === true ? 1 : 0))
                          .input('SPATIAL_PRECISION',this.sql.Int,(this.parameters.hasOwnProperty('SPATIAL_PRECISION') ? this.parameters.SPATIAL_PRECISION : null))
                          .input('DATE_TIME_PRECISION',this.sql.Int,this.parameters.MAX_TIMESTAMP_PRECISION)
                          .execute(sqlCompareSchema,{},{resultSet: true});

      const successful = results.recordsets[0]
      
      report.successful = successful.map(function(row,idx) {          
        const tableName = (this.parameters.TABLE_MATCHING === 'INSENSITIVE') ? row.TABLE_NAME.toLowerCase() : row.TABLE_NAME;
        const tableTimings = (timings[tableName] === undefined) ? { elapsedTime : 'N/A', throughput : "-1ms" } : timings[tableName]
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.TARGET_ROW_COUNT,tableTimings.elapsedTime,tableTimings.throughput]
      },this)
        
      const failed = results.recordsets[1]

      report.failed = failed.map(function(row,idx) {
        return [row.SOURCE_SCHEMA,row.TARGET_SCHEMA,row.TABLE_NAME,row.SOURCE_ROW_COUNT,row.TARGET_ROW_COUNT,row.MISSING_ROWS,row.EXTRA_ROWS,(row.SQLERRM !== null ? row.SQLERRM : '')]
      },this)

      return report
    }
   
}

module.exports = MsSQLCompare
"use strict";

const YadamuLibrary = require('../../common/yadamuLibrary.js');

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, spatialFormat) {    
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.spatialFormat = spatialFormat
  }
  

  async generateStatementCache(executeDDL) {    
    const sqlStatement = `SET @RESULTS = '{}'; CALL GENERATE_STATEMENTS(?,?,?,@RESULTS); SELECT @RESULTS "INSERT_INFORMATION"`;                       
    let results = await this.dbi.executeSQL(sqlStatement,[JSON.stringify({metadata : this.metadata}),this.targetSchema,this.spatialFormat]);
    results = results.pop();
    let statementCache = JSON.parse(results[0].INSERT_INFORMATION)
    if (statementCache === null) {
      statementCache = {}      
    }
    else {
      const tables = Object.keys(this.metadata); 
      const ddlStatements = tables.map((table,idx) => {
        const tableMetadata = this.metadata[table];
        const tableName = tableMetadata.tableName;
        const tableInfo = statementCache[tableName];
        tableInfo.columnNames = tableMetadata.columnNames

        const dataTypes = YadamuLibrary.decomposeDataTypes(tableInfo.targetDataTypes)

        tableInfo._BATCH_SIZE     = this.dbi.BATCH_SIZE
        tableInfo._COMMIT_COUNT   = this.dbi.COMMIT_COUNT
        tableInfo._SPATIAL_FORMAT = this.spatialFormat
        tableInfo.insertMode      = 'Batch';
  
        /*
        **
        ** Avoid use of Iterative Mode where possible due to significant performance impact.
        **
        */

        const setOperators = tableInfo.targetDataTypes.map((targetDataType,idx) => {
          if (this.dbi.DB_VERSION < '8.0.19' || false) {
            switch (targetDataType) {
              case 'geometry':
                tableInfo.insertMode = 'Iterative'; 
                switch (this.spatialFormat) {
                  case "WKB":
                  case "EWKB":
                    return ' "' + tableInfo.columnNames[idx] + '"' + " = ST_GeomFromWKB(?)";
                    break;
                  case "WKT":
                  case "EWRT":
                    return ' "' + tableInfo.columnNames[idx] + '"' + " = ST_GeomFromText(?)";
                    break;
                  case "GeoJSON":
                    return ' "' + tableInfo.columnNames[idx] + '"' + " = ST_GeomFromGeoJSON(?)";
                    break;
                  default:
                    return ' "' + tableInfo.columnNames[idx] + '"' + " = ST_GeomFromWKB(?)";
                }              
              default:
                return ' "' + tableInfo.columnNames[idx] + '" = ?'
            }
          }
          else {
            switch (targetDataType) {
              case 'geometry':
                tableInfo.insertMode = 'Rows';  
                switch (this.spatialFormat) {
                  case "WKB":
                  case "EWKB":
                    return 'ST_GeomFromWKB(?)';
                    break;
                  case "WKT":
                  case "EWRT":
                    return 'ST_GeomFromText(?)';
                    break;
                  case "GeoJSON":
                    return 'ST_GeomFromGeoJSON(?)';
                    break;
                  default:
                    return 'ST_GeomFromWKB(?)';
                }              
              default:
                return '?'
            }
          }
        }) 

        switch (tableInfo.insertMode) {
          case 'Batch':
            tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + `  values ?`;
            break;
          case 'Rows':
            tableInfo.rowConstructor = `(${setOperators.join(',')})`
            tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + `  values `;
            break;
          case 'Iterative':
            tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('(')) + ` set ` + setOperators.join(',');
            break;
        }
        return tableInfo.ddl;
      });
      if (executeDDL === true) {
        await this.dbi.executeDDL(ddlStatements);
      }
    }
    return statementCache;
  }
 
}

module.exports = StatementGenerator;
"use strict";

const YadamuLibrary = require('../../common/yadamuLibrary.js');

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, spatialFormat, batchSize, commitSize) {
    
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.spatialFormat = spatialFormat
  }
  

  async generateStatementCache () {    
  
    const sqlStatement = `select GENERATE_SQL($1,$2,$3)`
    const results = await this.dbi.executeSQL(sqlStatement,[{metadata : this.metadata}, this.targetSchema, this.spatialFormat])
    let statementCache = results.rows[0][0]
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

        const maxBatchSize        = Math.trunc(45000 / tableInfo.targetDataTypes.length);
        tableInfo._BATCH_SIZE     = this.dbi.BATCH_SIZE > maxBatchSize ? maxBatchSize : this.dbi.BATCH_SIZE
        tableInfo._COMMIT_COUNT   = this.dbi.COMMIT_COUNT
        tableInfo._SPATIAL_FORMAT = this.spatialFormat
        tableInfo.insertMode      = 'Batch';
        
        tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('select ')-1) + '\nvalues ';        
        tableInfo.insertOperators = dataTypes.map((dataType) => {
          switch (dataType.type) {
            case "geography":
            case "geometry":
              switch (this.spatialFormat) {
                case "WKB":
                  return "ST_GeomFromWKB($%)"
                  break;
                case "EWKB":
                  return "ST_GeomFromEWKB($%)"
                  break;
                case "WKT":
                  return "ST_GeomFromText($%)"
                  break;
                case "EWKT":
                  return "ST_GeomFromEWKT($%)"
                  break;
                case "GeoJSON":
                  return "ST_GeomFromGeoJSON($%)"
                  break;
                default:
                  return "ST_GeomFromWKB($%)"
            }
            break;
          default:
            return '$%';
          }            
        })
        return tableInfo.ddl
      });
    }
    return statementCache;
  }
}

module.exports = StatementGenerator;
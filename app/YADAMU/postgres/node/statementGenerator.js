"use strict";

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, spatialFormat, batchSize, commitSize, status, yadamuLogger) {
    
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.batchSize = batchSize
    this.spatialFormat = spatialFormat
    this.commitSize = commitSize;
    this.status = status,
    this.yadamuLogger = yadamuLogger
  }
  

  async generateStatementCache (executeDDL, vendor) {    
  
    const sqlStatement = `select GENERATE_SQL($1,$2,$3)`
    const results = await this.dbi.pgClient.query(sqlStatement,[{metadata : this.metadata}, this.targetSchema, this.spatialFormat])
    
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement};\n--\n`);
    }
    let statementCache = results.rows[0].generate_sql;
    if (statementCache === null) {
      statementCache = {}
    }
    else {
      const tables = Object.keys(this.metadata); 
      const ddlStatements = tables.map(function(table,idx) {
        const tableInfo = statementCache[this.metadata[table].tableName];
        tableInfo.dataTypes = this.dbi.decomposeDataTypes(tableInfo.targetDataTypes);
        const maxBatchSize = Math.trunc(45000 / tableInfo.targetDataTypes.length);
        tableInfo.batchSize = this.batchSize > maxBatchSize ? maxBatchSize : this.batchSize
        tableInfo.commitSize = this.commitSize

        tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('select ')-1) + '\nvalues ';        
        tableInfo.insertOperators = tableInfo.dataTypes.map(function(dataType) {
          switch (dataType.type) {
            case "geography":
            case "geometry":
              switch (this.spatialFormat) {
                case "WKB":
                  return "ST_GeomFromWKB(decode($%,'hex'))"
                  break;
                case "EWKB":
                  return "ST_GeomFromEWKB(decode($%,'hex'))"
                  break;
                case "WKT":
                  return "ST_GeomFromText($%)"
                  break;
                case "EWKT":
                  return "ST_GeomFromEWKT($%)"
                  break;
                default:
                  return "ST_GeomFromWKB(decode($%))"
            }
            break;
          default:
            return '$%';
          }            
        },this)
        return tableInfo.ddl
      },this);
	  	  
      if (executeDDL === true) {
        await this.dbi.executeDDL(ddlStatements);
      }
    }
    return statementCache;
  }
}

module.exports = StatementGenerator;
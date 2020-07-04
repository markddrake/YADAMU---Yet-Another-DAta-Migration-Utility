"use strict";

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, spatialFormat, batchSize, commitSize) {    
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.spatialFormat = spatialFormat
    this.batchSize = batchSize
    this.commitSize = commitSize;
  }
  

  async generateStatementCache(executeDDL, vendor) {    
   
    const sqlStatement = `SET @RESULTS = '{}'; CALL GENERATE_STATEMENTS(?,?,?,@RESULTS); SELECT @RESULTS "SQL_STATEMENTS"`;                       
    
    let results = await this.dbi.executeSQL(sqlStatement,[JSON.stringify({metadata : this.metadata}),this.targetSchema,this.spatialFormat]);
    results = results.pop();
    let statementCache = JSON.parse(results[0].SQL_STATEMENTS)
    if (statementCache === null) {
      statementCache = {}      
    }
    else {

      const tables = Object.keys(this.metadata); 
      const ddlStatements = tables.map((table,idx) => {
        const tableInfo = statementCache[this.metadata[table].tableName];
        tableInfo.columns = JSON.parse('[' + this.metadata[table].columns + ']');
        tableInfo.dataTypes = this.dbi.decomposeDataTypes(tableInfo.targetDataTypes);
        tableInfo.batchSize = this.batchSize;
        tableInfo.commitSize = this.commitSize;
        tableInfo.spatialFormat = this.spatialFormat
        tableInfo.insertMode = 'Batch';
		 
        const setOperators = tableInfo.targetDataTypes.map((targetDataType,idx) => {
           switch (targetDataType) {
             case 'geometry':
               tableInfo.insertMode = 'Iterative';
               switch (this.spatialFormat) {
                 case "WKB":
                 case "EWKB":
                   return ' "' + tableInfo.columns[idx] + '"' + " = ST_GeomFromWKB(UNHEX(?))";
                   break;
                 case "WKT":
                 case "EWRT":
                   return ' "' + tableInfo.columns[idx] + '"' + " = ST_GeomFromText(?)";
                   break;
                 case "GeoJSON":
                   return ' "' + tableInfo.columns[idx] + '"' + " = ST_GeomFromGeoJSON(?)";
                   break;
                 default:
                   return ' "' + tableInfo.columns[idx] + '"' + " = ST_GeomFromWKB(UNHEX(?))";
               }              
             /*
             **
             ** Avoid use of Iterative Mode where possible due to significant performance impact.
             **
             case 'date':
             case 'time':
             case 'datetime':
               tableInfo.insertMode = 'Iterative';
               return ' "' + tableInfo.columns[idx] + '"' + " = str_to_date(?,'%Y-%m-%dT%T.%fZ')"
            */
             default:
               return ' "' + tableInfo.columns[idx] + '" = ?'
           }
        }) 
        
        if (tableInfo.insertMode === 'Iterative') {
          tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('(')) + ` set ` + setOperators.join(',');
        }
        else {
          tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + `  values ?`;
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
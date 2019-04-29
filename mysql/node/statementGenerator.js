"use strict";

const Yadamu = require('../../common/yadamu.js');

class StatementGenerator {
  
  constructor(dbi, batchSize, commitSize) {
    
    this.dbi = dbi;
    this.batchSize = batchSize
    this.commitSize = commitSize;
  }
  

  async generateStatementCache (metadata, executeDDL) {    
  
    const sqlStatement = `SET @RESULTS = '{}'; CALL GENERATE_STATEMENTS(?,?,@RESULTS); SELECT @RESULTS "SQL_STATEMENTS"`;                       
   
    let results = await this.dbi.executeSQL(sqlStatement,[JSON.stringify({metadata : metadata}),this.dbi.parameters.TOUSER]);
    results = results.pop();
    let statementCache = JSON.parse(results[0].SQL_STATEMENTS)
    if (statementCache === null) {
      statementCache = {}      
    }
    else {
      const tables = Object.keys(metadata); 
      const ddlStatements = tables.map(function(table,idx) {
        const tableInfo = statementCache[metadata[table].tableName];
        tableInfo.batchSize = this.batchSize;
        tableInfo.commitSize = this.commitSize;
        tableInfo.insertMode = 'Bulk';
        const columnNames = JSON.parse('[' + metadata[table].columns + ']');
        
        const setOperators = tableInfo.targetDataTypes.map(function(targetDataType,idx) {
           switch (targetDataType) {
             case 'geometry':
               tableInfo.insertMode = 'Iterative';
               return ' "' + columnNames[idx] + '"' + " = ST_GeomFromText(?)";
             /*
             **
             ** Avoid use of Iterative Mode where possible due to significant performance impact.
             **
             case 'date':
             case 'time':
             case 'datetime':
               tableInfo.insertMode = 'Iterative';
               return ' "' + columnNames[idx] + '"' + " = str_to_date(?,'%Y-%m-%dT%T.%fZ')"
            */
             default:
               return ' "' + columnNames[idx] + '" = ?'
           }
        },this) 
                   
        if (tableInfo.insertMode === 'Iterative') {
          tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('(')) + ` set ` + setOperators.join(',');
        }
        else {
          tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + `  values ?`;
        } 
        return tableInfo.ddl;
      },this);
      if (executeDDL === true) {
        await this.dbi.executeDDL(ddlStatements);
      }
    }
    return statementCache;
  }
 
}

module.exports = StatementGenerator;
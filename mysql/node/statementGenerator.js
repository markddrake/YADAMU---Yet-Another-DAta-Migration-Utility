"use strict";

const Yadamu = require('../../common/yadamu.js');

class StatementGenerator {
  
  constructor(dbi ,ddlRequired, batchSize, commitSize, status, logWriter) {
    
    this.dbi = dbi;
    this.ddlRequired = ddlRequired
    this.batchSize = batchSize
    this.commitSize = commitSize;
    this.status = status;
    this.logWriter = logWriter;
  }
  

  async generateStatementCache (schema, systemInformation, metadata) {    
      
    const sqlStatement = `SET @RESULTS = '{}'; CALL GENERATE_STATEMENTS(?,?,@RESULTS); SELECT @RESULTS "SQL_STATEMENTS"`;                       
   
    let results = await this.dbi.executeSQL(sqlStatement,[JSON.stringify({systemInformation: systemInformation, metadata : metadata}),schema]);
    results = results.pop();
    const ddlStatements = [];  
    const statementCache = JSON.parse(results[0].SQL_STATEMENTS)
    const tables = Object.keys(metadata); 
    tables.forEach(async function(table,idx) {
                           const tableInfo = statementCache[table];
                           tableInfo.batchSize = this.batchSize;
                           tableInfo.commitSize = this.commitSize;
                           const columnNames = JSON.parse('[' + metadata[table].columns + ']');
                           tableInfo.useSetClause = false;
                                       
                           const setOperators = tableInfo.targetDataTypes.map(function(targetDataType,idx) {
                                                                                switch (targetDataType) {
                                                                                  case 'geometry':
                                                                                    tableInfo.useSetClause = true;
                                                                                    return ' "' + columnNames[idx] + '" = ST_GeomFromText(?)';
                                                                                  default:
                                                                                    return ' "' + columnNames[idx] + '" = ?'
                                                                                }
                            })
                                         
                            if (tableInfo.useSetClause) {
                              tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('(')) + ` set ` + setOperators.join(',');
                            }
                            else {
                              tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + `  values ?`;
                            }
                                 
                            ddlStatements[idx] = tableInfo.ddl;
    },this);
    
    await this.dbi.executeDDL(ddlStatements);
    return statementCache;
  }
 
}

module.exports = StatementGenerator;
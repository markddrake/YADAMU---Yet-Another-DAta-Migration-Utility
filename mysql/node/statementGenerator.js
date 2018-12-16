"use strict";

const MySQLCore = require('./mysqlCore.js');

class StatementGenerator {
  
  constructor(conn, status, logWriter) {
    
    // super();
    const statementGenerator = this;
    
    this.conn = conn;
    this.status = status;
    this.logWriter = logWriter;    
  }
  
  async executeDDL(ddlStatements) {
    await Promise.all(ddlStatements.map(async function(ddlStatement) {
                                          try {
                                            return await MySQLCore.query(this.conn,this.status,ddlStatement) 
                                          } catch (e) {
                                            this.logWriter.write(`${e}\n${statementCache[table].ddl}\n`)
                                          }
    },this))
  }  
  
  async executeDDL(ddlStatements) {
    await Promise.all(ddlStatements.map(async function(ddlStatement) {
                                          try {
                                            return await MySQLCore.query(this.conn,this.status,ddlStatement) 
                                          } catch (e) {
                                            this.logWriter.write(`${e}\n${statementCache[table].ddl}\n`)
                                          }
    },this))
  }
  
  async generateStatementCache(schema, systemInformation, metadata) {
      
    const sqlStatement = `SET @RESULTS = '{}'; CALL GENERATE_STATEMENTS(?,?,@RESULTS); SELECT @RESULTS "SQL_STATEMENTS"`;                       
   
    let results = await MySQLCore.query(this.conn,this.status,sqlStatement,[JSON.stringify({systemInformation: systemInformation, metadata : metadata}),schema]);
    results = results.pop();
    const ddlStatements = [];  
    const statementCache = JSON.parse(results[0].SQL_STATEMENTS)
    const tables = Object.keys(metadata); 
    tables.forEach(async function(table,idx) {
                           const tableInfo = statementCache[table];
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
    
    await this.executeDDL(ddlStatements);
    return statementCache;
  }
    
}

module.exports = StatementGenerator;
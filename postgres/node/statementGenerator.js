"use strict";

const Yadamu = require('../../common/yadamu.js');

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, batchSize, commitSize, status, yadamuLogger) {
    
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.batchSize = batchSize
    this.commitSize = commitSize;
    this.status = status,
    this.yadamuLogger = yadamuLogger
  }
  

  async generateStatementCache (executeDDL, vendor) {    
  
    const sqlStatement = `select GENERATE_SQL($1,$2)`
    const results = await this.dbi.pgClient.query(sqlStatement,[{metadata : this.metadata}, this.targetSchema])
    
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
        const maxBatchSize = Math.trunc(45000 / tableInfo.targetDataTypes.length);
        tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('select ')-1) + '\nvalues ';
        tableInfo.batchSize = this.batchSize > maxBatchSize ? maxBatchSize : this.batchSize
        tableInfo.commitSize = this.commitSize
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
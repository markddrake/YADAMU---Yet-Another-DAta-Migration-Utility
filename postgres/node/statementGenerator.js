"use strict";

class StatementGenerator {
  
  constructor(conn, status, logWriter) {
    
    // super();
    const statementGenerator = this;
    
    this.conn = conn;
    this.status = status;
    this.logWriter = logWriter;
  }
 
  async generateStatementCache(schema, systemInformation, metadata) {
      
    try {
      const sqlStatement = `select GENERATE_SQL($1,$2)`
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(`${sqlStatement};\n--\n`);
      }
      const results = await this.conn.query(sqlStatement,[{systemInformation : systemInformation, metadata : metadata},schema]);
      const statementCache = results.rows[0].generate_sql;
      const tables = Object.keys(metadata); 
      await Promise.all(tables.map(async function(table,idx) {
                                           const tableInfo = statementCache[table];
                                           tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('select ')-1) + '\nvalues ';
                                           const sqlStatement = tableInfo.ddl
                                           try {
                                             if (this.status.sqlTrace) {
                                               this.status.sqlTrace.write(`${sqlStatement};\n--\n`);
                                             }
                                             const results = await this.conn.query(sqlStatement);
                                           } catch (e) {
                                             this.logWriter.write(`${e}\n${sqlStatement}\n`)
                                           }
      },this));
    
      return statementCache;
    } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
    }
  }

}

module.exports = StatementGenerator;
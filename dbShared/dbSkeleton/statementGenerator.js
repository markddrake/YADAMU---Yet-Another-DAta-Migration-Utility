"use strict";

const Yadamu = require('../../common/yadamu.js');

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, batchSize, commitSize) {
    
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.batchSize = batchSize
    this.commitSize = commitSize;
  }
  

  async generateStatementCache (executeDDL, vendor) {    
  
    if (executeDDL) {
      this.dbi.executeDDL(ddl);
    }
  
    return statementCache;
  }
}

module.exports = StatementGenerator;
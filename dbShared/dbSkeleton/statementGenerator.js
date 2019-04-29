"use strict";

const Yadamu = require('../../common/yadamu.js');

class StatementGenerator {
  
  constructor(dbi, ddlRequired, batchSize, commitSize) {
    
    this.dbi = dbi;
    this.ddlRequired = ddlRequired
    this.batchSize = batchSize
    this.commitSize = commitSize;
  }
  

  async generateStatementCache (systemInformation, metadata, executeDDL) {    
  
    if (executeDDL) {
      this.dbi.executeDDL(ddl);
    }
  
    return statementCache;
  }
}

module.exports = StatementGenerator;
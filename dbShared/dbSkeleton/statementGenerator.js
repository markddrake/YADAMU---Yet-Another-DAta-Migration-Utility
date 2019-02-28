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
    return statementCache;
  }
}

module.exports = StatementGenerator;
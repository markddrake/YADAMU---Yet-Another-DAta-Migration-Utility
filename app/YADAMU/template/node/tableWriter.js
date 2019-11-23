"use strict"

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
  }


  async appendRow(row) {
  }

  async writeBatch() {
    this.batchCount++;
    return this.skipTable
  }
}

module.exports = TableWriter;
"use strict"

const Yadamu = require('../../common/yadamu.js');
const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
  }

  async appendRow(row) {
	this.rowCounters.cached++
  }

  async writeBatch() {
    this.batchCount++;
    this.batch.length = 0;
    this.rowCounters.written += this.rowCounters.cached;
    this.rowCounters.cached = 0;
	return this.skipTable
  }
}

module.exports = TableWriter;
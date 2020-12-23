"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const JSONWriter_FILE = require('../../file/node/jsonWriter.js');

class JSONWriter extends JSONWriter_FILE {

  /* Supress the table name from the file */

  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super(dbi,tableName,ddlComplete,0,status,yadamuLogger)
	this.startTable = '['
  }
  
  async endTable() {
	super.endTable()
  }

  
  
}

module.exports = JSONWriter;
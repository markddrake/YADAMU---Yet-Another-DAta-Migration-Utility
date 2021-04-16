"use strict"

const { performance } = require('perf_hooks');

const JSONWriter = require('./jsonWriter.js');
const StringWriter = require('../../../YADAMU/common/stringWriter.js')

class CSVWriter extends JSONWriter {

  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super(dbi,tableName,ddlComplete,status,yadamuLogger)
  }
   
  setTableInfo(tableName) {
	super.setTableInfo(tableName)
    this.csvTransformations = Array(this.tableInfo.columnNames.length);
	this.missingTransformations = this.tableInfo.columnNames.map((c,i) => { return i })
  }
  
  beginTable() { /* OVERRIDE */ }

  _setCSVTransformations(row) {

    this.missingTransformations = this.missingTransformations.flatMap((idx) => {
	  if (row[idx] !== null) {
        this.csvTransformations[idx] = this.getCSVTransformation(row[idx],(idx === row.length-1)) 
	    return []
	  }
	  return [idx]
	})
    this.setCSVTransformations = this.missingTransformations.length === 0 ? (row) => {} : this._setCSVTransformations
  }
    	
  setCSVTransformations = this._setCSVTransformations  
 
  formatRow(row) {
	 
	const sw = new StringWriter()
	this.setCSVTransformations(row);
	this.writeRowAsCSV(sw,row)
	return sw.toString();
	
  }

  async endTable() { /* OVERRIDE */ }

}

module.exports = CSVWriter;
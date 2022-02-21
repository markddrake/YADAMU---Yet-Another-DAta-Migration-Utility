"use strict"

import { performance } from 'perf_hooks';

import JSONOutputManager from '../../file/node/jsonOutputManager.js';
import StringWriter      from '../../../YADAMU/common/stringWriter.js'

import CSVLibrary        from './csvLibrary.js';

class CSVOutputManager extends JSONOutputManager {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,true,status,yadamuLogger)
	this.startTable = undefined
  }
   
  async setTableInfo(tableName) {
	await super.setTableInfo(tableName)
	this.csvTransformations = this.generateTransformations()
    this.missingTransformations = this.csvTransformations.map((c,i) => { return i })
  }
  
  _updateTransformations(row) {

    // Construct CSV Transformation Rules Incrementatlly (Row by Row). Transformations are defined basded on the first non-null column.
    // Once all columns have been processed this function beomes a No-op.

    this.missingTransformations = this.missingTransformations.flatMap((idx) => {
	  if (row[idx] !== null) {
        this.csvTransformations[idx] = CSVLibrary.getCSVTransformation(row[idx],(idx === row.length-1)) 
	    return []
	  }
	  return [idx]
	})
    this.updateTransformations = this.missingTransformations.length === 0 ? (row) => {} : this._updateTransformations
  }
    	
  updateTransformations = this._updateTransformations
 
  formatRow(row) {
	const sw = new StringWriter()
	this.updateTransformations(row);
	CSVLibrary.writeRowAsCSV(sw,row,this.csvTransformations)
	return sw.toString();
  }

  beginTable() { /* OVERRIDE */ }
      
  finalizeTable() { /* OVERRIDE */ }

}

export {CSVOutputManager as default }
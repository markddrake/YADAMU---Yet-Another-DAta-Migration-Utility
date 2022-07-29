
import { performance }   from 'perf_hooks';

import JSONOutputManager from '../file/jsonOutputManager.js';
import StringWriter      from '../../util/stringWriter.js'

import CSVLibrary        from './csvLibrary.js';

class CSVOutputManager extends JSONOutputManager {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,true,status,yadamuLogger)
	this.startTable = undefined
  }
   
  async setTableInfo(tableName) {
	await super.setTableInfo(tableName)
    
    // Cache the transformations and set the transformations array to NULL. Transformations are applied when converting the row to CSV in formatRow(). No transformations are required when the genereated rows of CSV encoded data is processed by processRow().

	this.csvTransformations = new Array(this.transformations.length).fill(null);
    this.pendingTransformations = this.transformations.map((c,i) => i)
  }
  
  _updateTransformations(row) {

    // Construct CSV Transformation Rules Incrementatlly (Row by Row). Transformations are defined basded on the first non-null column.
    // Once all columns have been processed this function beomes a No-op.
    
	this.pendingTransformations = this.pendingTransformations.flatMap((idx) => {
	  if (row[idx] !== null) {
        this.csvTransformations[idx] = CSVLibrary.getCSVTransformation(row[idx],(idx === row.length-1),this.tableInfo.targetDataTypes[idx].toLowerCase())
		return []
	  }
	  return [idx]
	})
    this.updateTransformations = this.pendingTransformations.length === 0 ? (row) => {} : this._updateTransformations
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
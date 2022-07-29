
import { performance } from 'perf_hooks';

import JSONOutputManager from './jsonOutputManager.js';

class ErrorOutputManager extends JSONOutputManager {

  constructor(dbi,tableName,firstTable,yadamuLogger) {

    super(dbi,tableName,{},firstTable,{},yadamuLogger)
	this.firstTable = true
	
  }

  async doConstruct() { 
  }

  beginTable(tableName) {
    this.startTable = this.firstTable ? `"${tableName}":[` :  `,"${tableName}":[`
	this.firstTable = false;
	this.rowSeperator = '';
	super.beginTable() 
    this.rowTransformation  = () => {}
	this.processRow =  this._processRow
	this.processOutOfSequenceMessages()
  }

  _processRow(row) {
   row = row.map((col) => {
	 return Buffer.isBuffer(col) ? col.toString('hex') : col 
   })
   this.push(this.formatRow(row));
   this.rowSeperator = ',';
  }
  
  endTable() {
	this.push(']')
  }
  
  async doTransform(messageType,obj) {
	switch (messageType) {
      case 'data':
        // processRow() becomes a No-op after calling abortTable()
        this.processRow(obj.data)
		break
	  case 'table':
        this.beginTable(obj.table)
        break
	  case 'eod':
        // Used when processing serial data sources such as files to indicate that all records have been processed by the writer
		this.endTable()
	    break
      default:
    }  
  }
  
  checkColumnCount() { /* OVERRRIDE */ }
  
  cacheRow(row) { /* OVERRRIDE */ }
  
  async doDestroy() { /* OVERRRIDE */ }
  
  
}

export { ErrorOutputManager as default }

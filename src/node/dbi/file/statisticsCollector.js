
import fs                   from 'fs';
import path                 from 'path';

import { 
  performance 
}                           from 'perf_hooks';

import { 
  Writable 
}                           from 'stream'

class StatisticsCollector extends Writable {
  
  constructor() {
	// nulLogger is used to supress row counting. ### Use of the nulLogger means supresses error reporting as well as row counting
    super({objectMode:true})
	this.tableInfo = {}
  }
  
  getStatistics() {
	return this.tableInfo
  }
  
  newTable(tableName) {
	this.tableName = tableName
    this.tableInfo[tableName] = {
      rowCount  : 0
     ,byteCount : 2
     ,hash      : null
    }    
  }
  
  processRow(row) { 
    this.tableInfo[this.tableName].rowCount++;
    this.tableInfo[this.tableName].byteCount+= JSON.stringify(row).length;    
  }
  
  async doWrite(messageType,obj) {
	  // this.yadamuLogger.trace([this.constructor.name,'doTransform()'],`${messageType}`)
	  switch (messageType) {
	    case 'data':
		  this.processRow(obj.data)
 	      break;
        case 'table':
          this.newTable(obj.table)
	      break;
        case 'systemInformation' :
        case 'metadata' :	
        case 'ddl':
        case 'eod':
	    case 'eof':
	    default:
	  }
  }
  
  
  _write (obj,encoding,callback)  {
	const messageType = Object.keys(obj)[0]
	this.doWrite(messageType,obj).then(() => { 
	  callback()
	}).catch((e) => { 
	  this.yadamuLogger.handleException(['FILE','EVENT STREAM',`_TRANSFORM(${messageType})`],e);
      callback(e)
	})
  };

}

export {StatisticsCollector as default }
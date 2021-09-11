"use strict" 

const Transform = require('stream').Transform;
const { performance } = require('perf_hooks');

class YadamuParser extends Transform {
    
  constructor(tableInfo,yadamuLogger) {
    super({objectMode: true });  
    this.tableInfo = tableInfo;
    this.yadamuLogger = yadamuLogger
	this.startTime = performance.now()
    this.rowCount = 0
	
	// Push the table name into the stream before sending the data.
	
	// Push a Partition Object or a Table Object
	
    if (tableInfo.PARTITION_COUNT) {
	  // console.log('YadamuParser()','PUSH','PARTITION',tableInfo.MAPPED_TABLE_NAME,tableInfo.partitionInfo.PARTITION_NUMBER,tableInfo.partitionPARTITION_NAME)
	  this.push({
		partition: {
	      tableName          : tableInfo.MAPPED_TABLE_NAME
	    , partitionCount     : tableInfo.PARTITION_COUNT
		, partitionNumber    : tableInfo.partitionInfo.PARTITION_NUMBER
		, partitionName      : tableInfo.partitionInfo.PARTITION_NAME
  	    }
	  })				
	}
	else {
	  // console.log('YadamuParser()','PUSH','TABLE',tableInfo.MAPPED_TABLE_NAME)
      this.push({table: tableInfo.MAPPED_TABLE_NAME})
	}
  }
    
  getRowCount() {
    return this.rowCount;
  }

  // For use in cases where the database generates a single column containing a serialized JSON reprensentation of the row.
  
  async _transform (data,encoding,callback) {
    this.rowCount++;
	if (!Array.isArray(data)) {
	  data = Object.values(data)
	}
    this.push({data:data.json})
    callback();
  }

   _final(callback) {
	// this.yadamuLogger.trace([this.constructor.name,this.tableInfo.TABLE_NAME],'_final()');
	this.endTime = performance.now();
	callback()
  } 
}

module.exports = YadamuParser
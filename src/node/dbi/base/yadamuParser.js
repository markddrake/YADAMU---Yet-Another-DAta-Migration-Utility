"use strict" 

import { Transform }    from 'stream';
import { performance }  from 'perf_hooks';
import { setTimeout }   from 'timers/promises'

class YadamuParser extends Transform {

  get COPY_METRICS()       { return this._COPY_METRICS }
  set COPY_METRICS(v)      { this._COPY_METRICS =  v }
      
  generateTransformations(queryInfo) {
	
	return queryInfo.DATA_TYPE_ARRAY.map((dataTypes,idx) => {
	  return null
	})

  }
  
  setTransformations(queryInfo) {

	this.transformations = this.generateTransformations(queryInfo)

	// Use a dummy rowTransformation function if there are no transformations required.

    this.rowTransformation = this.transformations.every((currentValue) => { return currentValue === null}) ? (row) => {} : (row) => {
    this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }
  }
  
  constructor(queryInfo,yadamuLogger) {
    super({objectMode: true });  
    this.queryInfo = queryInfo;
    this.yadamuLogger = yadamuLogger
	this.startTime = performance.now()
	this.setTransformations(queryInfo)
  }
    
  sendTableMessage() {
	
	// Push a Table Object or a Partition Object. If the this.queryInfo has a PARTITION_NUMBER property assume this is a partition level operation, rather than a table level operation
	
	// Push the table name into the stream before sending the data.

    if (this.queryInfo.hasOwnProperty('PARTITION_NUMBER')) {
      // console.log('YadamuParser()','PUSH','PARTITION',this.queryInfo.MAPPED_TABLE_NAME,this.queryInfo.PARTITION_NUMBER,this.queryInfo.PARTITION_NAME)
      const padSize = this.queryInfo.PARTITION_COUNT.toString().length
	  this.partitionInfo =  {
	    tableName          : this.queryInfo.MAPPED_TABLE_NAME
      , displayName        : `${this.queryInfo.MAPPED_TABLE_NAME}(${this.queryInfo.PARTITION_NAME || `#${this.queryInfo.PARTITION_NUMBER.toString().padStart(padSize,"0")}`})`
	  , partitionCount     : this.queryInfo.PARTITION_COUNT
	  , partitionNumber    : this.queryInfo.PARTITION_NUMBER
      , partitionName      : this.queryInfo.PARTITION_NAME || ''
  	  }
		   
	  this.push({partition: this.partitionInfo})	
	}
	else {
  	  // console.log('YadamuParser()','PUSH','TABLE',this.queryInfo.MAPPED_TABLE_NAME)
      this.push({table: this.queryInfo.MAPPED_TABLE_NAME})
	}
	  
  }
	
  async doConstruct() {
	this.sendTableMessage()
  
    // Workaround for issue with 'data' messages sometime (very, very rarely) appearing before 'table' messages
    // To-date issue has only been observed when writing to Loader based drivers
    await setTimeout(100) 
  
  }

  _construct(callback) {
	this.doConstruct().then(() => { callback() }).catch((e) => { callback(e) })
  }
  
  async doTransform(data) {
    this.rowTransformation(data)
	return data 
  }

  _transform(data,enc,callback) {

    this.COPY_METRICS.parsed++

    this.doTransform(data).then((row) => { 
	  this.push({data:row})
	  callback() 
    }).catch((e) => { 
	  callback(e) 
	})
  
  }

   _final(callback) {
	// this.yadamuLogger.trace([this.constructor.name,this.queryInfo.TABLE_NAME],'_final()');
	this.endTime = performance.now();
	callback()
  } 
  
}

export { YadamuParser as default}
"use strict"

const { performance } = require('perf_hooks');

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');
const YadamuWriter = require('../../common/yadamuWriter.js');
const {BatchInsertError} = require('../../common/yadamuError.js')

class S3Writer extends YadamuWriter {

  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
	this.buffer = Buffer.allocUnsafe(this.dbi.CHUNK_SIZE);
  }
  
  setTableInfo(tableName) {
    super.setTableInfo(tableName)
    this.insertMode = 'JSON';    
    if (this.dbi.tableMappings && this.dbi.tableMappings.hasOwnProperty(tableName)) {
	  tableName = this.dbi.tableMappings[tableName].tableName
	}
	this.offset = 0;

    this.transformations = this.tableInfo.targetDataTypes.map((targetDataType,idx) => {      
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);
	
	  if (YadamuLibrary.isBinaryDataType(dataType.type)) {
		return (row,idx) =>  {
          row[idx] = row[idx].toString('hex')
		}
      }
      
	  switch (dataType.type.toUpperCase()) {
        case "GEOMETRY":
        case "GEOGRAPHY":
        case '"MDSYS"."SDO_GEOMETRY"':
          if (this.SPATIAL_FORMAT.endsWith('WKB')) {
            return (row,idx)  => {
			  if (Buffer.isBuffer(row[idx])) {
			    row[idx] = row[idx].toString('hex')
			  }
			}
          }
		  return null;
        case "JSON":
          return (row,idx) =>  {
            if (typeof row[idx] === 'string') {
              row[idx] = JSON.parse(row[idx])
            } 
			if (Buffer.isBuffer(row[idx])) {
			  row[idx] = JSON.parse(row[idx].toString('utf8'))
			}
          }
        case "BOOLEAN":
          return (row,idx) =>  {
		    const bool = (typeof row[idx] === 'string') ? row[idx].toUpperCase() : (Buffer.isBuffer(row[idx])) ? row[idx].toString('hex') : row[idx]
			switch(bool) {
              case true:
              case "TRUE":
              case "01":
              case "1":
			  case 1:
                row[idx] = true;
				break;
              case false:
              case "FALSE":
              case "00":
              case "0":
			  case 0:
                row[idx] = false;
				break;
              default: 
            }
          }
        case "DATE":
          return (row,idx) =>  { 
            if (row[idx] instanceof Date) {
              row[idx] = row[idx].toISOString()
            }
          }
        case "TIMESTAMP":
          return (row,idx) =>  { 
            // A Ti7mestamp not explicitly marked as UTC is coerced to UTC.
			switch (true) {
              case (typeof row[idx] === 'string'):
                row[idx] = (row[idx].endsWith('Z') || row[idx].endsWith('+00:00')) ? row[idx] : row[idx] + 'Z';
				break;
              case (row[idx] instanceof Date):
                row[idx] = row[idx].toISOString()
            }
          }
		 default:
		   return null
      }
    }) 
	
    // Use a dummy rowTransformation function if there are no transformations required.

	this.rowTransformation = this.transformations.every((currentValue) => { currentValue === null}) ? (row) => {} : (row) => {
      this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }
  }
  
  startOuterArray() {
    this.startChar = '['
  }  
  
  async initialize(tableName) {
	await super.initialize(tableName)
	this.outputStream = this.dbi.getFileOutputStream(tableName);
	this.startOuterArray();
  }
	   
  cacheRow(row) {
	  
    this.rowTransformation(row)
	this.batch.push(row);	
	this.rowCounters.cached++
	return this.skipTable;
  }

  async writeBatch() {
	  
	// Write Batch in 5MB Chunks
    // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber()],`writeBatch(${this.batch.length})`)
	
	for (const row of this.batch) {
	  const chunk = Buffer.from(this.startChar + JSON.stringify(row))
      if (this.offset + chunk.length < this.dbi.CHUNK_SIZE) {
        this.offset+= chunk.copy(this.buffer,this.offset,0)
  	    this.rowCounters.written++
		this.startChar = ','
	  }
      else {
        // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber(),this.rowCounters.written],`upload(${this.offset})`)
		this.outputStream.write(this.buffer.slice(0,this.offset),undefined,() => {
		  this.rowCounters.committed = this.rowCounters.written;
		  if (this.reportCommits) {
	        this.yadamuLogger.info([`${this.tableInfo.tableName}`],`Rows uploaded: ${this.rowCounters.committed}.`);
		  }
	    })
    	this.buffer = Buffer.allocUnsafe(this.dbi.CHUNK_SIZE);	
  	    this.offset = 0
		this.offset+= chunk.copy(this.buffer,this.offset,0)
  	    this.rowCounters.written++
		this.startChar = ','
	  }
    }
	
	this.batch.length = 0;
    this.rowCounters.cached = 0;
	return this.skipTable;

  }
    
  // Define dummy transaction Management functions to prevent counters from begin reset. 
  
  async beginTransaction() {}
  async commitTransaction() {}
  async rollbackTransaction() {}
  
  async finalizeBatch() {
  	if (this.rowCounters.received === 0) {
      this.offset+= this.buffer.write('[',this.offset);
    }	  
    this.offset+= this.buffer.write(']',this.offset)
  } 
  
  async finalize() {
	await super.finalize();
	this.finalizeBatch()
    // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber(),this.rowCounters.written],`upload(${this.offset})`)
	await new Promise((resolve,reject) => {
      this.outputStream.end(this.buffer.slice(0,this.offset),undefined,() => {
		this.rowCounters.committed = this.rowCounters.written;
		resolve() })
	})
	this.offset = 0
    this.endTime = performance.now()
  }
}

module.exports = S3Writer;
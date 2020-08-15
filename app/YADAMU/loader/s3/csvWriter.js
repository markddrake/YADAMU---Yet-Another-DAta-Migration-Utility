"use strict"

const { performance } = require('perf_hooks');

const S3Writer = require('./s3Writer.js');

class CSVWriter extends S3Writer {

  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super(dbi,tableName,ddlComplete,status,yadamuLogger)
  }
   
  async startOuterArray() {
  }
  
  writeBatch() {
	  
	// Write Batch in 5MB Chunks
    // this.yadamuLogger.trace([this.constructor.name,this.tableName,this.dbi.getWorkerNumber()],`writeBatch(${this.batch.length})`)
	
	for (const row of this.batch) {
      let nextLine = JSON.stringify(row) 
	  nextLine = nextLine.substring(1,nextLine.length-1) + "\r\n"
	  const chunk = Buffer.from(nextLine);
      if (this.offset + chunk.length < this.dbi.CHUNK_SIZE) {
        this.offset+= chunk.copy(this.buffer,this.offset,0)
  	    this.rowCounters.written++
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
	  }
    }
	
	this.batch.length = 0;
    this.rowCounters.cached = 0;
	return this.skipTable;

  }
     
  finalizeBatch() {
	if (this.rowCounters.received === 0) {
      this.offset+= this.buffer.write("\r\n",this.offset);
	}
  }
 
}

module.exports = CSVWriter;
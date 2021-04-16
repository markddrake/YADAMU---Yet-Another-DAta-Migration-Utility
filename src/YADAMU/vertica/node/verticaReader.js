"use strict" 
const Readable = require('stream').Readable;
const {Query, types} = require('pg')

const array = require('postgres-array')
const parseByteA = require('postgres-bytea')

class VerticaReader extends Readable {
      
    constructor(connection,sqlStatement,tableName,yadamuLogger) {
	  super({objectMode:true}) 
	  this.connection = connection
	  this.tableName = tableName
      this.yadamuLogger = yadamuLogger
	  this.stagingArea = []
      this.highWaterMark = 1024
	  this.lowWaterMark = 512
	  this.streamComplete = false;
	  this.streamPaused = false;
	  this.streamFailed = false;
      this.pendingRead = false
	  let count = 0;
	  	  
      types.setTypeParser(116,parseByteA)
	  types.setTypeParser(117,parseByteA)
	  
	  this.query = new Query({
		text: sqlStatement, 
		rowMode: 'array'
	  });
	  const maxCached = new Promise((resolve,reject) => {
		let maxCached = 0;
	    this.query.on('row',(row) => {
	      if (this.pendingRead) { 
	        this.push(row)
		    this.pendingRead = false
		  }
		  else {
		    this.stagingArea.push(Object.values(row));
		    maxCached = this.stagingArea.length > maxCached ? this.stagingArea.length : maxCached;
		    count++
		    
			if (this.stagingArea.length === this.highWaterMark) {
			  this.connection.connection.stream.pause();
			  this.streamPaused = true;
		    }
			
		  }
		}).on('end',() => {
  	      this.streamComplete = true;
	      if (this.pendingRead) {
	        this.push(null)
		  }
		  resolve(maxCached)
		}).on('error',(err) => {
   	      this.streamFailed = true;
		  this.streamComplete = true;
		  this.emit('error',err)
		  this.destroy(err);
		})
      })
      const result = this.connection.query(this.query)
	  
	}

	_read() {
      if (this.stagingArea.length > 0) {
        this.push(this.stagingArea.shift())
	    this.pendingRead = false
		if (this.streamPaused && (this.stagingArea.length === this.lowWaterMark)) {
		  this.connection.connection.stream.resume();
		  this.streamPaused  = false;
	    }
	  }
	  else {
	    if (this.streamComplete) {
	      this.push(null)
		}
		else {
          this.pendingRead = true;		
		}
	  }
	}
	
	/*
	async _destroy(cause,callback) {
       // this.yadamuLogger.trace([this.constructor.name,this.tableName],`_destroy(${cause ? cause.message : 'Normal'})`)
	   if (!this.streamComplete) {
		 try {
		   await this.request.cancel();
		   this.streamCanceled = true;
		 } catch (e) {
	     }
	   }
	   callback(cause)
	}
	*/
	
}

module.exports = VerticaReader

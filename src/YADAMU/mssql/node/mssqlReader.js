"use strict" 
const Readable = require('stream').Readable;

class MsSQLReader extends Readable {
      
    constructor(request,sqlStatement,tableName,yadamuLogger) {
	  super({objectMode:true}) 
	  this.request = request
	  this.tableName = tableName
      this.yadamuLogger = yadamuLogger
	  this.stagingArea = []
      this.highWaterMark = 1024
	  this.lowWaterMark = 512
	  
	  this.streamPaused = false;
	  this.streamFailed = false;
	  this.streamCanceled = false;
	  this.streamComplete = false;
	  
	  this.request.stream = true

	  this.request.on('row', (row) => {
	    if (this.pendingRead) { 
	      this.push(row)
		  this.pendingRead = false
		}
		else {
		  this.stagingArea.push(row) 
		  if (this.stagingArea.length === this.highWaterMark) {
		    this.request.pause();
			this.streamPaused = true;
		  }
		}
	  }).on('error',(err, p) => {
        // this.yadamuLogger.trace([this.constructor.name,this.tableName,`sql.Request(stream).error()`],err)
		if (this.streamCanceled && (err.code && (err.code === 'ECANCEL'))) {
		  this.destroy();
		  return;
		}
        if (!this.streamFailed) {
		  this.streamFailed = true;
          // Passing the exception to destroy() should emit the error but doesn't always seem too? 
		  this.emit('error',err)
          this.destroy(err);
		}
      }).on('done',(result) => {
	    if (this.pendingRead) {
	      this.push(null)
		}
	    this.streamComplete = true;
	  })
      request.query(sqlStatement); 
	}
	
	_read() {
      if (this.stagingArea.length > 0) {
        this.push(this.stagingArea.shift())
	    this.pendingRead = false
		if (this.streamPaused && (this.stagingArea.length === this.lowWaterMark)) {
		  this.request.resume();
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
	
}

module.exports = MsSQLReader

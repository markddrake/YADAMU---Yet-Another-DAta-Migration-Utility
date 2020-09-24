"use strict" 
const Readable = require('stream').Readable;

class MsSQLReader extends Readable {
      
    constructor(request,sqlStatement) {
	  super({objectMode:true}) 
	  this.request = request
	  this.sqlStatement = sqlStatement
	  
      this.stagingArea = []
      this.highWaterMark = 1024
	  this.lowWaterMark = 512
	  
	  this.streamPaused = false;
	  this.streamFailed = false;
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
        if (!this.streamFailed) {
          // Destroy should emit the error but doesn't seem too? 
          this.destroy(err);
		  this.emit('error',err)
		}
		this.streamFailed = true;
      }).on('done',(result) => {
	    if (this.pendingRead) {
	      this.push(null)
		}
	    this.streamComplete = true;
	  })
	    
      request.query(this.sqlStatement); 
	
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
	
	_destroy(cause,callback) {
	   if (!this.streamComplete) {
		 this.request.cancel();
	   }
	   callback()
	}
	
}

module.exports = MsSQLReader

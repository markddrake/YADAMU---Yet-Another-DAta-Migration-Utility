"use strict" 
const Readable = require('stream').Readable;

class ExampleReader extends Readable {
      
	// if the database has an event interface that isnt' a Readable use this framework 
	// to create a readable stream from whatever the database provides in a Readable
	  
    constructor(conn,sqlStatement) {
	  super({objectMode:true}) 
	  this.conn = conn
	  this.sqlStatement = sqlStatement
	  
      this.stagingArea = []
      this.highWaterMark = 1024
	  this.lowWaterMark = 512
	  
	  this.streamPaused = false;
	  this.streamFailed = false;
	  this.streamComplete = false;
	  
	  this.stream = conn.query.stream(sqlStatement)

	  this.stream.on('row', (row) => {
	    if (this.pendingRead) { 
	      this.push(row)
		  this.pendingRead = false
		}
		else {
		  this.stagingArea.push(row) 
		  if (this.stagingArea.length === this.highWaterMark) {
		    this.stream.pause();
			this.streamPaused = true;
		  }
		}
	  }).on('error',(err, p) => {
		if (!this.streamFailed) {
          this.destroy(err);
		}
		this.streamFailed = true;
      }).on('done',(result) => {
	    if (this.pendingRead) {
	      this.push(null)
		}
	    this.streamComplete = true;
	  })
	 
	}
	
	_read() {
      if (this.stagingArea.length > 0) {
        this.push(this.stagingArea.shift())
	    this.pendingRead = false
		if (this.streamPaused && (this.stagingArea.length === this.lowWaterMark)) {
		  this.stream.resume();
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
}

module.exports = ExampleReader

"use strict" 
const Readable = require('stream').Readable;

class SnowflakeReader extends Readable {
      
    constructor(connection,sqlStatement) {
	  super({objectMode:true}) 
	  this.connection = connection
	  this.sqlStatement = sqlStatement
	  
      this.stagingArea = []
      this.highWaterMark = 1024
	  this.lowWaterMark = 512
	  
	  this.streamPaused = false;
	  this.streamFailed = false;
	  this.streamComplete = false;
	  
      const statement = this.connection.execute({sqlText: sqlStatement,  fetchAsString: ['Number','Date'], streamResult: true})
      this.snowflakeStream = statement.streamRows();

	  this.snowflakeStream.on('data', (row) => {
	    if (this.pendingRead) { 
	      this.push(row)
		  this.pendingRead = false
		}
		else {
		  this.stagingArea.push(row) 
		  if (this.stagingArea.length === this.highWaterMark) {
		    this.snowflakeStream.pause();
			this.streamPaused = true;
		  }
		}
	  }).on('error',(err, p) => {
		if (!this.streamFailed) {
          this.destroy(err);
		}
		this.streamFailed = true;
      }).on('end',(result) => {
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
		  this.snowflakeStream.resume();
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

module.exports = SnowflakeReader

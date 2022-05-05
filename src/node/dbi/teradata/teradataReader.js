"use strict" 

import {Readable} from 'stream';

class TeradataReader extends Readable {
      
    constructor(cursor,sqlStatement) {
	  super({objectMode:true}) 
	  this.cursor = cursor
	  this.sqlStatement = sqlStatement
	  
      this.stagingArea = []
      this.highWaterMark = 10240
	  this.lowWaterMark = 5120
	  
	  this.streamPaused = false;
	  this.streamFailed = false;
	  this.streamComplete = false;
	  
      this.cursor.execute(this.sqlStatement)
	  this.stagingArea = cursor.fetchmany(this.highWaterMark)
	    
	}
	
	_read() {
	  if (this.stagingArea.length > 0) {
        this.push(this.stagingArea.shift())
		if ((this.stagingArea.length < this.lowWaterMark) && (!this.streamComplete)) {
		  const stagingAreaLength = this.stagingArea.length
		  this.stagingArea.push(...this.cursor.fetchmany(this.lowWaterMark));
		  this.streamComplete = this.stagingArea.length === stagingAreaLength
		}
	  }
	  else {
		this.cursor.close()
		this.push(null)
	  }
	}
}

export { TeradataReader as default }
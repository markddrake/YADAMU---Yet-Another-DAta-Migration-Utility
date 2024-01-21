
import {Readable} from 'stream'

class MsSQLReader extends Readable {
      
    get LOGGER()             { return this._LOGGER }
    set LOGGER(v)            { this._LOGGER = v }
   
    get DEBUGGER()           { return this._DEBUGGER }
    set DEBUGGER(v)          { this._DEBUGGER = v }

    constructor(request,sqlStatement,tableName,yadamuLogger) {
	  super({objectMode:true}) 
	  this.request = request
	  this.tableName = tableName
      this.LOGGER = yadamuLogger
	  this.stagingArea = []
      this.highWaterMark = 1024
	  this.lowWaterMark = 512
	  
	  this.streamPaused = false;
	  this.streamFailed = false;
	  this.streamCancelled = false;
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
        // this.LOGGER.trace([this.constructor.name,this.tableName,`sql.Request(stream).error()`],err)
		if (this.streamCancelled && (err.code && (err.code === 'ECANCEL'))) {
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
       // this.LOGGER.trace([this.constructor.name,this.tableName],`_destroy(${cause ? cause.message : 'Normal'})`)
	   if (!this.streamComplete) {
		 try {
		   await this.request.cancel();
		   this.streamCancelled = true;
		 } catch (e) {
	     }
	   }
	   callback(cause)
	}
	
}

export { MsSQLReader as default }

"use strict"

const {Readable} = require('stream')

class Pushable extends Readable {

  constructor(options,endOnEnd) {
	 super(options);
	 this.data = []
	 this.endOnEnd = endOnEnd
	 this.pendingRead = false;
  }
  
  pipe(os,options) {
    options = options || {}
	options.end = this.endOnEnd;
	return super.pipe(os,options);
  }  
  
  _read() {
	 if (this.data.length === 0) {
	   this.pause()
	   this.pendingRead = true;
     }
	 else {
	   // if (this.data[0] === null) console.log('Push NULL')
	   this.push(this.data.shift())
	 }
  }
  
  pump(data) {
	this.data.push(data)
    if (this.paused) {
	  this.resume();
      if (this.pendingRead) {
	    this.push(this.data.shift())
		this.pendingRead = false;
	  }
	}
  }
}


module.exports = Pushable
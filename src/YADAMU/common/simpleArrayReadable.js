"use strict"

const {Readable} = require('stream')

class SimpleArrayReadable extends Readable {

  constructor(data,options,endOnEnd) {
	 super(options);
	 this.data = Array.isArray(data) ? data : [data]
	 this.endOnEnd = endOnEnd
  }
  
  pipe(os,options) {
	options = options || {}
	options.end = this.endOnEnd;
	return super.pipe(os,options);
  }  
  
  _read() {     
     this.push(this.data.length === 0 ? null : this.data.shift())
  }
  
}


module.exports = SimpleArrayReadable
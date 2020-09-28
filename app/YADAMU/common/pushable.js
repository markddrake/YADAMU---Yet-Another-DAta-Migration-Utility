"use strict"

const {Readable} = require('stream')

class Pushable extends Readable {

  constructor(options,endOnEnd) {
	 super(options);
	 this.data = []
	 this.endOnEnd = endOnEnd
  }
  
  pipe(os,options) {
    options = options || {}
	options.end = this.endOnEnd;
	return super.pipe(os,options);
  }  
  
  _read() {
	 if (this.data.length === 0) {
	   this.pause()
     }
	 else {
       console.log('_read()',this.data.length,this.data[0] === null ? ' END' : Object.keys(this.data[0])[0])
	   this.push(this.data.shift())
	 }
  }
  
  pump(data) {
    // console.log('pump()',this.data.length,data === null ? 'NULL' : Object.keys(data)[0])
    this.data.push(data)
    if (this.paused) {
       // console.log('_read()',this.data.length,this.data[0] === null ? ' END' : Object.keys(this.data[0])[0])
	   this.push(this.data.shift())
	   this.resume();
	   this
	}
  }
}


module.exports = Pushable
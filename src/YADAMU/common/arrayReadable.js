"use strict"

import {Readable} from 'stream'

class ArrayReadable extends Readable {

  constructor(data) {
	 super({objectMode:true});
	 this.data = data || []
  }
  
  _read() {
	 if (this.data.length === 0) {
	   this.pause()
     }
	 else {
	   this.push(this.data.shift())
	 }
  }
  
  addContent(data) {
    // console.log('pump()',this.isPaused(),this.readableFlowing,this.data.length,data === null ? 'NULL' : Object.keys(data)[0])
    this.data.push(...data)
    if (this.isPaused()) {
       // console.log('_read()',this.data.length,this.data[0] === null ? ' END' : Object.keys(this.data[0])[0])
	   this.push(this.data.shift())
	   this.resume();
	}
  }
}


export { ArrayReadable as default}
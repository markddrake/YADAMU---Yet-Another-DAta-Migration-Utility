
import {
  Readable
}                   from 'stream'

class ArrayReadable extends Readable {

  constructor(data) {
	 super({objectMode:true});
	 this.data = data || []
	 // Node 17.7.2 - isPaused() seems to unexpectedly change state from true to false
	 this.pendingRead = false;
  }
  
  _read() {
	 if (this.data.length === 0) {
	   this.pause()
       this.pendingRead = true;
     }
	 else {
	   this.push(this.data.shift())
	 }
  }
  
  addContent(data) {
	// console.log('pump()',this.isPaused(),this.pendingRead,this.readableFlowing,this.data.length,data === null ? 'NULL' : Object.keys(data)[0])
    this.data.push(...data)
    if (this.isPaused() || this.pendingRead) {
       this.push(this.data.shift())
	   this.pendingRead = false;
	   this.resume();
	}
  }
}


export { ArrayReadable as default}
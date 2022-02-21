"use strict"

import path from 'path';
import {Writable} from 'stream'

class NullWritable extends Writable {

  static get NULL_WRITABLE() {
    this._NULL_WRITABLE = this._NULL_WRITABLE || new NullWritable()
	return this._NULL_WRITABLE;
  }

  constructor(options) {
    super(options)
	this.path = `${path.sep}dev${path.sep}null`
  }
  
  _write(chunk, encoding, callback) {
	callback() 
  }
  
  _writev(chunks, callback) {
	callback()
  }
	
  _final(callback) {
	 callback()
  }

}

export { NullWritable as default}
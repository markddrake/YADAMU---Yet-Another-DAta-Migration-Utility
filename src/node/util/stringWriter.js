"use strict"

import {Writable} from 'stream';

class StringWriter extends Writable {
   
  constructor(options) {
    super(options)
    this.chunks = []
  }

  _write(chunk, encoding, callback) {
     this.chunks.push(chunk);
     callback();
  }
  
  toString() {
    return this.chunks.join('');
  }
  
  toJSON() {
	return this.chunks.map((chunk) => {
	  return chunk.toString('utf-8')
	})
  }
  
  reset() {
	this.chunks = []
  }
  
}

export { StringWriter as default}
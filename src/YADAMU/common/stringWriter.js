"use strict"

const Writable = require('stream').Writable

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
  
  reset() {
	this.chunks = []
  }
  
}

module.exports = StringWriter;
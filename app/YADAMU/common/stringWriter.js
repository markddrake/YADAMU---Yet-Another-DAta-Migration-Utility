"use strict"

const Writable = require('stream').Writable

class StringWriter extends Writable {
   
  constructor(options) {
    super(options)
    this.chunks = []
  }

  _write(chunk, encoding, done) {
     this.chunks.push(chunk);
     done();
  }
  
  toString() {
    return this.chunks.join('');
  }
  
}

module.exports = StringWriter;
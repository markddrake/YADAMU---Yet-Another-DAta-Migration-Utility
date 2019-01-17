"use strict"

const Writable = require('stream').Writable

class BufferWriter extends Writable {
    
   
  constructor(options) {
    super(options)
    this.chunks = []
  }

  _write(chunk, encoding, done) {
     this.chunks.push(chunk);
     done();
  }
  
  toHexBinary() {
    return Buffer.concat(this.chunks).toString('hex');
  }
  
}

module.exports = BufferWriter

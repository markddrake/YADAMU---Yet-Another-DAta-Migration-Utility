"use strict"
const Transform = require('stream').Transform

class HexBinToBinary extends Transform {
  
  constructor() {
    super({objectMode: false, decodeStrings : false });     
  }

  async _transform (data,encoding,done) {
    const buffer = Buffer.from(data,'hex')
    this.push(buffer);
    done();
  }
  
}

module.exports = HexBinToBinary
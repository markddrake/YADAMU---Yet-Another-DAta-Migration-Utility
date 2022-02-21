"use strict"
import {Transform} from 'stream'

class HexBinToBinary extends Transform {
  
  constructor() {
    super({objectMode: false, decodeStrings : false });     
  }

  async _transform (data,encoding,callback) {
    const buffer = Buffer.from(data,'hex')
    this.push(buffer);
    callback();
  }
  
}

export { HexBinToBinary as default}
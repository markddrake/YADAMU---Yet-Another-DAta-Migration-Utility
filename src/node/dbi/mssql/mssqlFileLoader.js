
import {
  Writable
}                 from 'stream';
import { 
  StringDecoder 
}                 from 'string_decoder';

class MsSQLFileLoader extends Writable {
     
  constructor(dbi) {
    super({})
    this.dbi = dbi;
	this._decoder = new StringDecoder('UTF-8')
  }
       
  async doWrite(chunk,encoding) {
    if (encoding === 'buffer') {
      chunk = this._decoder.write(chunk);
    }
    const data = chunk
    const results = await this.dbi.executeCachedStatement({C0: data})
    return results
  } 
  
  _write(chunk, encoding, callback) {
	this.doWrite(chunk,encoding).then((results) => { callback(null,results)}).catch((e) => { callback(e)})
  }
  
  async doFinal() {
    const data = this._decoder.end();
	let results = undefined
	if (data.length > 0) {
      results = await this.dbi.executeCachedStatement({C0: data})
    }
    return results
  }	
  
  _final(callback) {
	this.doFinal().then((results) => { callback(null,results)}).catch((e) => { callback(e)})
  }
}   
 
export { MsSQLFileLoader as default }

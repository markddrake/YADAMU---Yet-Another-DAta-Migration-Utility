"use strict"

const { StringDecoder } = require('string_decoder');
const {Transform} = require('stream');

class StringDecoderStream extends Transform {

  constructor(options) {
	super(options)
	this._decoder = new StringDecoder('UTF-8')
	// this.on('end',() => { console.log('SDS:end')}).on('finsih',() => { console.log('SDS:finsih')}).on('close',() => { console.log('SDS:close')}).on('error',() => { console.log('SDS:error')})
  }

  _transform(data,encoding,callback) {
	data = this._decoder.write(data);
    this.push(data)
	callback()
  }

  _final(callback) {
	const data = this._decoder.end()
	this.push(data)
	callback()
  }
  
}

module.exports = StringDecoderStream
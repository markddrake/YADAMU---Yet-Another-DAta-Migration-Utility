"use strict"

const PassThrough = require('stream').PassThrough

class YadamuPassThrough extends PassThrough {

  constructor(options,endOnEnd) {
    super(options)
	this.endOnEnd = endOnEnd
  }
  
  pipe(os,options) {
    options = options || {}
	options.end = this.endOnEnd;
	return super.pipe(os,options);
  }  
 
}

module.exports = YadamuPassThrough
"use strict"

import fs from 'fs';
import path from 'path';
import { performance } from 'perf_hooks';
import { Writable } from 'stream';

import YadamuLogger from '../../../YADAMU/common/yadamuLogger.js';

class ArrayWriter extends Writable {
	
  // Basically this is the opposite of Readable.from()
  
  constructor() {
	super({objectMode: true})
	this.array = [];
  }
  
  getArray() {
	return this.array
  }
  
  async doWrite(obj) {
	if (obj.data) {
	  this.array.push(obj.data)
	}
  }
  
  _write(obj,enc,callback) {
    this.doWrite(obj).then(() => { callback() }).catch((err) => { callback(e) })
  }


}

export { ArrayWriter as default }
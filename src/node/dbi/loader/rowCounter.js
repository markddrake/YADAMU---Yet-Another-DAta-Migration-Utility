"use strict"

import { Transform } from 'stream';

import JSONScanner from '../file/jsonScanner.js'

import {
  InvalidJSON
}                  from '../file/fileException.js'

/*
**
** Counts the rows in a loader data file, [[...],[...],...], without parsing them.
**
*/

class RowCounter extends Transform {

  get ROW_COUNT()   { return this._ROW_COUNT }

  get LOGGER()             { return this._LOGGER }
  set LOGGER(v)            { this._LOGGER = v }

  get DEBUGGER()           { return this._DEBUGGER }
  set DEBUGGER(v)          { this._DEBUGGER = v }

  constructor(exportFilePath,yadamuLogger) {

    super();

	this.exportFilePath = exportFilePath
    this.LOGGER = yadamuLogger;

	this.scanner = new JSONScanner(this)
	this._ROW_COUNT = 0;

  }

  // JSONScanner handler: rows are SKIPped, and each row that is an array is counted.

  decide(depth,key,firstByte) {
	return depth === 0 ? JSONScanner.DESCEND : JSONScanner.SKIP
  }

  open(depth,key,isArray) {
	if (!isArray) {
	  throw new InvalidJSON(`Expected a loader data file to contain an array of rows, found an object.`)
	}
  }

  close(depth,key,isArray) {}

  value(depth,key,value) {}

  skipped(depth,key,firstByte) {
	if (firstByte === 0x5B) {
	  this._ROW_COUNT++
	}
  }

  _transform(data,enc,callback) {
	try {
      this.scanner.write(data);
	} catch (e) {
      this.LOGGER.handleException([`JSON_PARSER`,`Invalid JSON Document`,`"${this.exportFilePath}"`],e)
	}
    callback();
  };

}

export { RowCounter as default }

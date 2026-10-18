import JSONScanner from '../file/jsonScanner.js'

import {
  InvalidJSON
}                  from '../file/fileException.js'
import _JSONParser from '../file/jsonParser.js';

/*
**
** Parses a loader data file: a single JSON array of rows, [[...],[...],...]. JSONScanner locates each row, which is parsed with JSON.parse().
**
*/

class JSONParser extends _JSONParser {

  constructor(mode, exportFilePath, pipelineState, yadamuLogger) {
    super(mode, exportFilePath, pipelineState, yadamuLogger);
  }

  decide(depth,key,firstByte) {
	return depth === 0 ? JSONScanner.DESCEND : JSONScanner.CAPTURE
  }

  open(depth,key,isArray) {
	if (!isArray) {
	  throw new InvalidJSON(`Expected a loader data file to contain an array of rows, found an object.`)
	}
	this.parseComplete = false
  }

  close(depth,key,isArray) {
	this.parseComplete = true
  }

  value(depth,key,value) {
	this.nextRow(value)
  }

  _transform(data,enc,callback) {
	try {
	  this.scanner.write(data);
      callback();
	} catch (e) {
	  this.LOGGER.handleException([`JSON_PARSER`,`Invalid JSON Document`,`"${this.exportFilePath}"`],e)
   	  this.STREAM_STATE.error = e
	  callback(e)
	}
  }

}

export {JSONParser as default }

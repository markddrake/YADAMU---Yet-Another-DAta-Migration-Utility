
import {
  performance
}                from 'perf_hooks';

import {
  Transform
}                from 'stream';

import JSONScanner from './jsonScanner.js'

import {
  IncompleteJSON,
  InvalidJSON
}                from './fileException.js'

import DBIConstants                   from '../base/dbiConstants.js'

/*
**
** Parses a YADAMU export file: {"systemInformation":{...},"metadata":{...},"ddl":[...],"data":{"TABLE":[[row],[row],...],...}}
**
** JSONScanner walks the outer structure; each top level member and each row is parsed with JSON.parse().
**
*/

class JSONParser extends Transform {

  get LOGGER()               { return this._LOGGER }
  set LOGGER(v)              { this._LOGGER = v }
  get DEBUGGER()             { return this._DEBUGGER }
  set DEBUGGER(v)            { this._DEBUGGER = v }

  get PIPELINE_STATE()       { return this._PIPELINE_STATE }
  set PIPELINE_STATE(v)      { this._PIPELINE_STATE =  v }

  get STREAM_STATE()         { return this.PIPELINE_STATE[DBIConstants.PARSER_STREAM_ID] }
  set STREAM_STATE(v)        { this.PIPELINE_STATE[DBIConstants.PARSER_STREAM_ID] = v }

  constructor(mode, exportFilePath, pipelineState, yadamuLogger) {

    super({objectMode: true });

    this.mode = mode;
	this.exportFilePath = exportFilePath
	this.PIPELINE_STATE = pipelineState
    this.STREAM_STATE = {}

    this.LOGGER = yadamuLogger;

    this.parseComplete = false;
	this.tableState = {}

    this.tableList  = new Set();
    this.dataPhase = false;

	this.scanner = new JSONScanner(this)

  }

  /*
  **
  ** JSONScanner handler. The document is an object (depth 0). Each top level member (depth 1) is parsed as a whole, except "data",
  ** an object of tables (depth 2), each an array of rows (depth 3).
  **
  */

  decide(depth,key,firstByte) {
	switch (depth) {
	  case 0:
	    return JSONScanner.DESCEND
	  case 1:
	    if (key === 'data') {
		  return JSONScanner.DESCEND
		}
		return ((this.mode === 'DATA_ONLY') && (key === 'ddl')) ? JSONScanner.SKIP : JSONScanner.CAPTURE
	  case 2:
	    return JSONScanner.DESCEND
	  default:
	    return JSONScanner.CAPTURE
	}
  }

  open(depth,key,isArray) {
	switch (depth) {
	  case 0:
	    if (isArray) {
		  throw new InvalidJSON(`Expected an export file to contain an object, found an array.`)
		}
	    this.parseComplete = false
		break
	  case 1:
		this.dataPhase = true
		break
	  case 2:
	    this.startTable(key)
	}
  }

  close(depth,key,isArray) {
	switch (depth) {
	  case 0:
	    this.endOfFile()
		break
	  case 1:
		this.dataPhase = false
		break
	  case 2:
	    this.endTable()
	}
  }

  value(depth,key,value) {
	switch (depth) {
	  case 1:
        if (key === 'metadata') {
          this.tableList = new Set(Object.keys(value));
        }
        this.nextObject(key,value)
		break
	  default:
	    this.nextRow(value)
	}
  }

  skipped(depth,key,firstByte) {
  }

  nextObject(name,value) {
    // this.LOGGER.trace([this.constructor.name,name],'nextObject()')
    this.push({[name]: value})
  }

  startTable(tableName) {
    // this.LOGGER.trace([this.constructor.name,tableName],'startTable()')
	this.currentTable = tableName
	this.tableState = {
	  startTime : performance.now()
	, parsed    : 0
	}
	this.push({table: tableName})
  }

  endTable() {
    // this.LOGGER.trace([this.constructor.name,this.currentTable],'endTable()')
    this.tableList.delete(this.currentTable);
	// Snapshot the table start and end times.
	this.tableState.endTime = performance.now()
	this.push({eod: this.tableState})
  }

  nextRow(data) {
	// this.LOGGER.trace([this.constructor.name,this.currentTable],'nextRow()')
	this.tableState.parsed++;
	this.push({data:data});
  }

  endOfFile() {
	// this.LOGGER.trace([this.constructor.name],'eof()')
	this.parseComplete = true
	this.push({eof: true})
  }

  _transform(data,enc,callback) {
	try {
	  this.scanner.write(data);
      callback();
	} catch (e) {
      this.LOGGER.handleException([`JSON_PARSER`,`JSON Parsing Error`,`"${this.exportFilePath}"`],e)
   	  this.STREAM_STATE.error = e
	  callback(e)
	}
  }

  _flush(callback) {
	try {
	  this.scanner.end()
	} catch (e) {
   	  this.STREAM_STATE.error = e
	  return callback(e)
	}
    if (this.scanner.INCOMPLETE) {
	  const parserError = new IncompleteJSON(this.exportFilePath)
   	  this.STREAM_STATE.error = parserError
	  callback(parserError)
    }
	else {
	  callback()
	}
  }

}

export { JSONParser as default }

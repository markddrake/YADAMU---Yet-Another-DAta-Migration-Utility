"use strict"

const path=require('path');

class DummyOutputStream {

  static get DUMMY_OUTPUT_STREAM() {
    this._DUMMY_OUTPUT_STREAM = this._DUMMY_OUTPUT_STREAM || new DummyOutputStream()
	return this._DUMMY_OUTPUT_STREAM;
  }

  constructor() {
	this.writableEnded = false 
	this.path = `${path.sep}dev${path.sep}null`
  }
  
  cork()              { /* DISABLED */ }
  destroy()           { /* DISABLED */ }
  end()               { /* DISABLED */ }
  seDefaultEncoding() { /* DISABLED */ }
  uncork()            { /* DISABLED */ }
  write()             { /* DISABLED */ }
 
}

module.exports = DummyOutputStream
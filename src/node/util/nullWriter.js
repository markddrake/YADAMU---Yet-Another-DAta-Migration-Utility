"use strict"

import path from 'path';

class NullWriter {

  static get NULL_WRITER() {
    this._NULL_WRITER = this._NULL_WRITER || new NullWriter()
	return this._NULL_WRITER;
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
  close()             { /* DISABLED */ }
  
}

export { NullWriter as default}
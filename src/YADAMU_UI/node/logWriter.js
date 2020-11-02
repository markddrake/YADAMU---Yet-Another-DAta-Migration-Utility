"use strict"

const Writable = require('stream').Writable

class LogWriter extends Writable {

   constructor(logWindow) {
	super();
	this.logWindow = logWindow
   }

  _write(chunk, encoding, done) {
    this.logWindow.webContents.send('write-log',chunk)
    done();
  }

}

module.exports = LogWriter
    
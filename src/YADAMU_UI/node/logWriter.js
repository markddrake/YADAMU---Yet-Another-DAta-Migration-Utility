"use strict"

import {Writable} from 'stream'

class LogWriter extends Writable {

   constructor(logWindow) {
	super();
	this.logWindow = logWindow
   }

  _write(chunk, encoding, done) {
	this.logWindow.webContents.send('write-log',chunk.toString('utf8'))
    done();
  }

}

export { LogWriter as default }
    
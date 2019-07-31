"use strict"

const fs = require('fs');
const path = require('path');
  
const Yadamu = require('../../common/yadamu.js').Yadamu;

class YadamuTest extends Yadamu {
  
  constructor() {
    super('Test')
    this.status.showInfoMsgs = true;
  }
  
  reset() {
    this.status.startTime     = new Date().getTime()
    this.status.warningRaised = false;
    this.status.errorRaised   = false;
    this.status.statusMsg     = 'successfully'
    
    if (this.parameters.SQLTRACE) {
	  this.status.sqlTrace = fs.createWriteStream(this.parameters.SQLTRACE,{flags : "a"});
    }
  }
  
  async doExport(dbi,file) {
    this.parameters.FILE = file
    const timings = await super.doExport(dbi);
    delete this.parameters.FILE
    return timings;
  }
 
  async doImport(dbi,file) {
    this.parameters.FILE = file
    const timings = await super.doImport(dbi);
    delete this.parameters.FILE
    return timings;
  }

  async doServerImport(dbi,file) {    
    dbi.parameters.FILE = file
    const timings = await super.doServerImport(dbi);
    delete this.parameters.FILE
    return timings;
  }

}  
     
module.exports = YadamuTest;

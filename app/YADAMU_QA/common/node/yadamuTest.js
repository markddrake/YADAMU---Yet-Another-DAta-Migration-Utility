"use strict"

const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');

const Yadamu = require('../../../YADAMU/common/yadamu.js');
const YadamuTestDefaults = require('./yadamuDefaults.json')

class YadamuTest extends Yadamu {
    
  get YADAMU_TEST_DEFAULTS() { return YadamuTestDefaults };	
  get YADAMU_DRIVERS() { return YadamuTestDefaults.drivers };	

  constructor(mode) {
    super(mode)
  }
  
  reset() {
    this.status.startTime     = performance.now()
    this.status.warningRaised = false;
    this.status.errorRaised   = false;
    this.status.statusMsg     = 'successfully'
    
    if (this.parameters.SQL_TRACE) {
	  this.status.sqlTrace = fs.createWriteStream(this.parameters.SQL_TRACE,{flags : "a"});
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

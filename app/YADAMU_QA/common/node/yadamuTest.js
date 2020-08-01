"use strict"

const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');

const Yadamu = require('../../../YADAMU/common/yadamu.js');
const YadamuDefaults = require('./yadamuDefaults.json')

class YadamuTest extends Yadamu {
    
    
  static get TEST_DEFAULTS()      { return YadamuDefaults };    
  static get YADAMU_DRIVERS()         { return YadamuTest.TEST_DEFAULTS.drivers }

  constructor(mode) {
    super(mode)
  }
  
  setTeradataWorker(worker) {
	this.teradateWorker = worker
  }
  
  getTeradataWorker() {
	 return this.teradateWorker
  }
  
  reset() {
    this._REJECTION_MANAGER = undefined;
    this._WARNING_MANAGER = undefined;
    this.STATUS.startTime     = performance.now()
    this.STATUS.warningRaised = false;
    this.STATUS.errorRaised   = false;
    this.STATUS.statusMsg     = 'successfully'
    
    if (this.parameters.SQL_TRACE) {
	  this.STATUS.sqlTrace = fs.createWriteStream(this.parameters.SQL_TRACE,{flags : "a"});
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

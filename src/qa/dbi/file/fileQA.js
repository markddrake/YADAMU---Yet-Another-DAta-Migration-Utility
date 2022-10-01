
import {
  setTimeout 
}                          from "timers/promises"

import fsp from 'fs/promises';
import path from 'path';

import Yadamu              from '../../core/yadamu.js';

import FileDBI             from '../../../node/dbi/file/fileDBI.js';

class FileQA extends FileDBI {
   
  static #_DBI_PARAMETERS
	
  static get DBI_PARAMETERS()  { 
	 this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,FileDBI.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[FileDBI.DATABASE_KEY] || {},{RDBMS: FileDBI.DATABASE_KEY}))
	 return this.#_DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
    return FileQA.DBI_PARAMETERS
  }	

  constructor(yadamu,role,connectionSettings,parameters) {
     super(yadamu,role,connectionSettings,parameters)
  }
  
  async createOutputStream() {
	
	if (this.options.recreateSchema === true) {
  	  await fsp.mkdir(path.dirname(this.FILE),{recursive: true})
	}
  	return await super.createOutputStream()
  }
  
  async finalize() { /* OVERRIDE */ }
  
}

export { FileQA as default }
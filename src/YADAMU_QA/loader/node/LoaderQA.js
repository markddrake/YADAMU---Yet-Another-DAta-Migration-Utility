"use strict" 
const fsp = require('fs').promises

const LoaderDBI = require('../../../YADAMU/loader/node/loaderDBI.js');

class LoaderQA extends LoaderDBI {

  async recreateSchema() {
	 await fsp.rmdir(this.IMPORT_FOLDER,{recursive: true})
  }

  constructor(yadamu) {
	super(yadamu)
  }

  async initialize() {
	await super.initialize();
	if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	}
  }
  
}
module.exports = LoaderQA
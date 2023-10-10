
import {
  setTimeout 
}                       from "timers/promises"

import fs               from 'fs';
import fsp              from 'fs/promises';
import csv              from 'csv-parser';

import LoaderDBI        from '../../../node/dbi/loader/loaderDBI.js';
import Yadamu           from '../../core/yadamu.js';

import {
  FileError, 
  FileNotFound, 
  DirectoryNotFound
}                       from '../../../node/dbi//file/fileException.js';

import YadamuQALibrary  from '../../lib/yadamuQALibrary.js'

class CloudService {
	
  constructor(dbi) {
	this.dbi = dbi
  }
  
  async getObject(objectPath) {
	return await fsp.readFile(objectPath,{encoding: 'utf8'})
  }
  
  async createReadStream(path) {
    return new Promise((resolve,reject) => {
	  const stack = new Error().stack
      const is = fs.createReadStream(path);
      is.once('open',() => {resolve(is)}).once('error',(err) => {reject(err.code === 'ENOENT' ? new FileNotFound(this.DRIVER_ID,err,stack,path) : new FileError(this.DRIVER_ID,err,stack,path) )})
    })
  }
}

class LoaderQA extends YadamuQALibrary.loaderQAMixin(LoaderDBI) {

  static #_DBI_PARAMETERS
	
  static get DBI_PARAMETERS()  { 
	this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,LoaderDBI.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[LoaderDBI.DATABASE_KEY] || {},{RDBMS: LoaderDBI.DATABASE_KEY}))
	return this.#_DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
    return LoaderQA.DBI_PARAMETERS
  }	
			
  async recreateSchema() {
	  
    this.DIRECTORY = this.TARGET_DIRECTORY
    
	let stack
	try {
	  stack = new Error().stack;
      await fsp.rm(this.IMPORT_FOLDER,{recursive: true, force: true})
	} catch(err) {
	  if (err.code !== 'ENOENT') {
	    throw new FileError(this.DRIVER_ID,err,stack,this.IMPORT_FOLDER);
	  }
	}
	try {
	  stack = new Error().stack;
      await fsp.mkdir(this.IMPORT_FOLDER,{recursive: true})
	} catch(err) {
      throw err.code === 'ENOENT' ? new DirectoryNotFound(this.DRIVER_ID,err,stack,this.IMPORT_FOLDER) : new FileError(this.DRIVER_ID,err,stack,this.IMPORT_FOLDER)
	}
  }
 
  constructor(yadamu,manager,connectionSettings,parameters) {
	super(yadamu,manager,connectionSettings,parameters)
  }

  async initializeImport() {
	if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	}
	await super.initializeImport();
  }

  async initializeExport() {

	// this.LOGGER.trace([this.constructor.name],`initializeExport()`)
	
	await this.loadControlFile()
	this.LOGGER.info(['Export',this.DATABASE_VENDOR],`Using Control File: "${this.getURI(this.CONTROL_FILE_PATH)}"`);

  }
  
  classFactory(yadamu) {
    return new LoaderQA(yadamu,this,this.connectionParameters,this.parameters)
  }
  
  getCSVParser() {
	 return csv({headers: false})
  }

  async finalize() { /* OVERRIDE */ }
  
}

export { LoaderQA as default }
"use strict" 

import {
  setTimeout 
}                       from "timers/promises"

import fs               from 'fs';
import fsp              from 'fs/promises';
import path             from 'path';
import crypto           from 'crypto';
import csv              from 'csv-parser';


import { 
  pipeline 
}                       from 'stream/promises';

// import Parser from '../../../node/dbi//clarinet/clarinet.cjs';



import NullWriter       from '../../../node/util/nullWritable.js';
import YadamuLibrary    from '../../../node/lib/yadamuLibrary.js';
import LoaderDBI        from '../../../node/dbi/loader/loaderDBI.js';
import JSONParser       from '../../../node/dbi/loader/jsonParser.js';
import Yadamu           from '../../core/yadamu.js';
import RowCounter       from '../../util/rowCounter.js';
import YadamuQALibrary  from '../../lib/yadamuQALibrary.js'
import ArrayWriter      from './arrayWriter.js';


import {
  FileError, 
  FileNotFound, 
  DirectoryNotFound
}                       from '../../../node/dbi//file/fileException.js';

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
	    throw new FileError(this.DRIVER_IDerr,stack,this.IMPORT_FOLDER);
	  }
	}
	try {
	  stack = new Error().stack;
      await fsp.mkdir(this.IMPORT_FOLDER,{recursive: true})
	} catch(err) {
      throw err.code === 'ENOENT' ? new DirectoryNotFound(err,stack,this.IMPORT_FOLDER) : new FileError(err,stack,this.IMPORT_FOLDER)
	}
  }
 

  constructor(yadamu,manager,connectionSettings,parameters) {
	super(yadamu,manager,connectionSettings,parameters)
	// Enabled Method sharing with Cloud based implementations/
	this.cloudService = new CloudService(this)
  }

  async initializeImport() {
	if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	}
	await super.initializeImport();
  }

  async getArray(filename) {
	const streams = await this.qaInputStreams(filename)
		
	const jsonParser = new JSONParser(this.yadamuLogger, this.MODE, filename)
	streams.push(jsonParser);
	
	const arrayWriter = new ArrayWriter(this)
	streams.push(arrayWriter)

    await pipeline(streams)
    return arrayWriter.getArray()
  }
    
  async calculateSortedHash(filename) {
    const array = await this.getArray(filename)
	array.sort((r1,r2) => {
	  for (const i in r1) {
        if (r1[i] < r2[i]) return -1
        if (r1[i] > r2[i]) return 1;
      }
	  return 0
    })
    return crypto.createHash('sha256').update(JSON.stringify(array)).digest('hex');
  } 
  
  async compareFiles(sourceFile,targetFile) {

	const sourceFileSize = fs.statSync(sourceFile).size
	const targetFileSize = fs.statSync(targetFile).size
	let sourceHash = ''
	let targetHash = ''
    if (sourceFileSize === targetFileSize) {
      sourceHash = await this.calculateHash(sourceFile)
	  targetHash = await this.calculateHash(targetFile)
	  if (sourceHash !== targetHash) {
		sourceHash = await this.calculateSortedHash(sourceFile);
		targetHash = await this.calculateSortedHash(targetFile)
	  }
	}
    return [sourceFileSize,targetFileSize,sourceHash,targetHash]
  }
  
  async initializeExport() {

	// this.yadamuLogger.trace([this.constructor.name],`initializeExport()`)
	
	await this.loadControlFile()
	this.yadamuLogger.info(['Export',this.DATABASE_VENDOR],`Using Control File: "${this.getURI(this.CONTROL_FILE_PATH)}"`);

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
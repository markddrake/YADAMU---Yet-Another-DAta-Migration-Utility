"use strict" 

const fs = require('fs')
const fsp = require('fs').promises
const path = require('path')
const crypto = require('crypto');
const csv = require('csv-parser');
const assert = require('assert');
const { createGzip, createGunzip, createDeflate, createInflate } = require('zlib');
const { pipeline, finished } = require('stream');

const Parser = require('../../../YADAMU/clarinet/clarinet.js');
const NullWriter = require('../../../YADAMU/common/nullWritable.js');
const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const YadamuLogger = require('../../../YADAMU/common/yadamuLogger.js');
const LoaderDBI = require('../../../YADAMU/loader/node/loaderDBI.js');
const JSONParser = require('../../../YADAMU/loader/node/jsonParser.js');
const YadamuTest = require('../../common/node/yadamuTest.js');
// const ArrayCounter = require('./arrayCounter.js');
const ArrayReader = require('./arrayReader.js');

const {FileError, FileNotFound, DirectoryNotFound} = require('../../../YADAMU/file/node/fileException.js');

class LoaderQA extends LoaderDBI {

  static #_YADAMU_DBI_PARAMETERS
	
  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,LoaderDBI.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[LoaderDBI.DATABASE_KEY] || {},{RDBMS: LoaderDBI.DATABASE_KEY}))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
    return LoaderQA.YADAMU_DBI_PARAMETERS
  }	
			
  async recreateSchema() {
    this.DIRECTORY = this.TARGET_DIRECTORY
    
	let stack
	try {
	  stack = new Error().stack;
      await fsp.rmdir(this.IMPORT_FOLDER,{recursive: true})
	} catch(err) {
	  if (err.code !== 'ENOENT') {
	    throw new FileError(err,stack,this.IMPORT_FOLDER);
	  }
	}
	try {
	  stack = new Error().stack;
      await fsp.mkdir(this.IMPORT_FOLDER,{recursive: true})
	} catch(err) {
      throw err.code === 'ENOENT' ? new DirectoryNotFound(err,stack,this.IMPORT_FOLDER) : new FileError(err,stack,this.IMPORT_FOLDER)
	}
  }

  constructor(yadamu,settings,parameters) {
	super(yadamu,settings,parameters)
  }

  async initialize() {
	await super.initialize();
	if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	}
  }

  async getArray(filename) {
	const streams = await this.qaInputStreams(filename)
		
	const jsonParser = new JSONParser(this.yadamuLogger, this.MODE, filename)
	streams.push(jsonParser);
	
	const arrayReader = new ArrayReader(this)
	streams.push(arrayReader)

	return new Promise((resolve,reject) => {
      pipeline(streams,(err) => {
		if (err) reject(err) 
		resolve(arrayReader.getArray());
	  })
	})
  }
    
  async calculateSortedHash(filename) {
    try {
    const array = await this.getArray(filename)
	array.sort((r1,r2) => {
	  for (const i in r1) {
        if (r1[i] < r2[i]) return -1
        if (r1[i] > r2[i]) return 1;
      }
	  return 0
    })
    return crypto.createHash('sha256').update(JSON.stringify(array)).digest('hex');
  } catch (e) { console.log(e) }
  } 
  
  calculateHash(file) {
	  
	return new Promise((resolve,reject) => {
	  const hash = crypto.createHash('sha256');
	  const is = fs.createReadStream(file)
	  pipeline([is,hash],(err) => {
		if (err) reject(err)
		hash.end();
		hash.setEncoding('hex');
		resolve(hash.read());
	  })
	})
  }	  
  
  async compareFiles(sourceFile,targetFile) {

	const sourceFileSize = fs.statSync(sourceFile).size
	const targetFileSize = fs.statSync(targetFile).size
	let sourceHash = ''
	let targetHash = ''
    if (sourceFileSize === targetFileSize) {
      sourceHash = this.calculateHash(sourceFile)
	  targetHash = this.calculateHash(targetFile)
	  if (sourceHash !== targetHash) {
		sourceHash = await this.calculateSortedHash(sourceFile);
		targetHash = await this.calculateSortedHash(targetFile)
	  }
	}
    return [sourceFileSize,targetFileSize,sourceHash,targetHash]
  }
  
  async compareSchemas(source,target) {

    const report = {
      successful : []
    , failed     : []
    }
	 
	this._BASE_DIRECTORY = undefined
    this.DIRECTORY = this.SOURCE_DIRECTORY
	this.setFolderPaths(path.join(this.BASE_DIRECTORY,source.schema),source.schema)
    const sourceFilePath = this.CONTROL_FILE_PATH;
    
	this._BASE_DIRECTORY = undefined
    this.DIRECTORY = this.TARGET_DIRECTORY
	this.setFolderPaths(path.join(this.BASE_DIRECTORY,target.schema),target.schema)
    const targetFilePath = this.CONTROL_FILE_PATH;
	
	try {
	  assert.notEqual(sourceFilePath,targetFilePath,`Source & Target control files are identical: "${sourceFilePath}"`);
	} catch(err) {
	  report.failed.push([source.schema,target.schema,'',0,0,0,0,err.message])
      return report	 
	}
	
    let fileContents	
	
	try {
  	  fileContents = await fsp.readFile(sourceFilePath,{encoding: 'utf8'})
	} catch(err) {
	  report.failed.push([source.schema,target.schema,'*',0,0,0,0,err.message])
      return report	 
	}

    const sourceControlFile = JSON.parse(fileContents)
	
	try {
	  fileContents = await fsp.readFile(sourceFilePath,{encoding: 'utf8'})
	} catch(err) {
	  report.failed.push([source.schema,target.schema,'*',0,0,0,0,err.message])
      return report	 
	}
	
    const targetControlFile = JSON.parse(fileContents)

	// Assume the source control file contains the correct options for both source and target
    this.controlFile = sourceControlFile
	
    const sourceFolder = path.dirname(sourceFilePath)
	const targetFolder = path.dirname(targetFilePath)
   
    let results = await Promise.all(Object.keys(targetControlFile.data).map((tableName) => {return this.compareFiles( path.resolve(sourceFolder,sourceControlFile.data[tableName].file), path.resolve(targetFolder,targetControlFile.data[tableName].file))}))
	
    Object.keys(targetControlFile.data).map((tableName,idx) => {
	  const result = results[idx]
	  if ((result[0] === result[1]) && (result[2] === result[3])) {
		report.successful.push([source.schema,target.schema,tableName,result[0]])
	  }
	  else {
		report.failed.push([source.schema,target.schema,tableName,result[0],result[1],result[2],result[3],null])
	  }
    })
	return report
  }
  
  async qaInputStreams(filename) {
    const streams = []
	const is = await this.getInputStream(filename);
	streams.push(is)
	
	if (this.ENCRYPTED_INPUT) {
	  const iv = await this.loadInitializationVector(filename)
	  streams.push(new IVReader(this.IV_LENGTH))
  	  // console.log('Decipher',filename,this.controlFile.settings.encryption,this.yadamu.ENCRYPTION_KEY,iv);
	  const decipherStream = crypto.createDecipheriv(this.controlFile.settings.encryption,this.yadamu.ENCRYPTION_KEY,iv)
	  streams.push(decipherStream);
	}

	if (this.COMPRESSED_INPUT) {
      streams.push(this.controlFile.settings.compression === 'GZIP' ? createGunzip() : createInflate())
	}
	
    return streams
  }

  async getRowCount(tableInfo) {
    
    let arrayLength = 0
    const parser = Parser.createStream()
	const nullStream = new NullWriter();
	
    parser.once('error',(err) => {
      this.yadamuLogger.handleException([`JSON_PARSER`,`Invalid JSON Document`,`"${this.exportFilePath}"`],err)
	  parser.destroy(err);
  	  parser.unpipe() 
	  // Swallow any further errors raised by the Parser
	  parser.on('error',(err) => {});
    }).on('openobject',(key) => {
      this.jDepth++;
    }).on('openarray',() => {
      this.jDepth++;
    }).on('closeobject',() => {
      // this.yadamuLogger.trace([`${this.constructor.name}.onCloseObject()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}\nCurrentObject: ${JSON.stringify(this.currentObject)}`);           
      this.jDepth--;
    }).on('closearray',() => {
	  // this.yadamuLogger.trace([`${this.constructor.name}.onclosearray()`,`${this.jDepth}`],`\nObjectStack: ${this.objectStack}.\nCurrentObject:${JSON.stringify(this.currentObject)}`);          
      this.jDepth--;
      switch (this.jDepth){
        case 1:
		  arrayLength++
      }
    });  
	
	const streams = await this.qaInputStreams(this.makeAbsolute(this.controlFile.data[tableInfo.TABLE_NAME].file))
	streams.push(parser)
	streams.push(nullStream)
	const rowCount = new Promise((resolve,reject) => {
      finished(nullStream,(err) => {
		if (err) {reject(err)} else {resolve(arrayLength)}
      })
    })
    pipeline(streams,(err) => {})
	return rowCount
  }
  
  async getRowCounts(target) {

	this.DIRECTORY = this.TARGET_DIRECTORY
	this.setFolderPaths(this.IMPORT_FOLDER,target.schema)
	
	let stack
	try {
	  const fileContents = await fsp.readFile(this.CONTROL_FILE_PATH,{encoding: 'utf8'})
      this.controlFile = JSON.parse(fileContents)
      let counts 
	  switch (this.controlFile.settings.contentType) {
		case 'JSON':
          counts = await Promise.all(Object.keys(this.controlFile.data).map((k) => {
  	        return this.getRowCount({TABLE_NAME:k})
	      }))
		 	
          return Object.keys(this.controlFile.data).map((k,i) => {
	        return [target.schema,k,counts[i]]
          })	
  	    case 'CSV':
   		  stack = new Error().stack
		  counts = await Promise.all(Object.values(this.controlFile.data).map((t) => {
		    return new Promise((resolve,reject) => {
  			  let count = 0;
			  fs.createReadStream(this.makeRelative(t.file)).pipe(this.getCSVParser()).on('data', (data) => {count++}).on('end', () => {resolve(count)});
			})
    	 }))
		 
		 return Object.keys(this.controlFile.data).map((k,i) => {
	       return [target.schema,k,counts[i]]
         })	
      }
	} catch(err) {
      throw err.code === 'ENOENT' ? new FileNotFound(err,stack,this.CONTROL_FILE_PATH) : new FileError(err,stack,this.CONTROL_FILE_PATH)
	}
  }    

  getControlFileSettings() {
    return this.controlFile.settings
  }
  
  setControlFileSettings(options) {
	this.parameters.OUTPUT_FORMAT = options.contentType
	this.yadamu.parameters.COMPRESSION = options.compression
	this.yadamu.parameters.ENCRYPTION = options.encryption
  }

  async initializeExport() {

	// this.yadamuLogger.trace([this.constructor.name],`initializeExport()`)
	
	await this.loadControlFile()
	this.yadamuLogger.info(['Export',this.DATABASE_VENDOR],`Using Control File: "${this.PROTOCOL}${this.resolve(this.CONTROL_FILE_PATH)}"`);

  }
  
  classFactory(yadamu) {
    return new LoaderQA(yadamu)
  }
  
  getCSVParser() {
	 return csv({headers: false})
  }
  
}


module.exports = LoaderQA
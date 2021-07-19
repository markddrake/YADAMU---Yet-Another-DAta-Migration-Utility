"use strict" 

const path = require('path')
const assert = require('assert');
const crypto = require('crypto');
const { pipeline, finished } = require('stream');



const Parser = require('../../../YADAMU/clarinet/clarinet.js');
const NullWriter = require('../../../YADAMU/common/nullWritable.js');

class MixinCloudQA {

  async recreateSchema() {
	this.DIRECTORY = this.TARGET_DIRECTORY
	await this.cloudService.createBucketContainer()
	await this.cloudService.deleteFolder(this.IMPORT_FOLDER)
  }
	
  async calculateSortedHash(file) {
  
    const fileContents = await this.cloudService.getObject(file)		
    const array = this.parseContents(fileContents)

    array.sort((r1,r2) => {
	  for (const i in r1) {
        if (r1[i] < r2[i]) return -1
        if (r1[i] > r2[i]) return 1;
      }
	  return 0
    })
    return crypto.createHash('sha256').update(JSON.stringify(array)).digest('hex');
  } 
  
  async calculateHash(file) {
	  
	return new Promise(async (resolve,reject) => {
	  const hash = crypto.createHash('sha256');
	  const is = await this.cloudService.createReadStream(file)
	  pipeline([is,hash],(err) => {
		if (err) reject(err)
		hash.end();
		hash.setEncoding('hex');
		resolve(hash.read());
	  })
	})
  }	  
 
  async compareFiles(sourceFile,targetFile) {
	let props = await this.cloudService.getObjectProps(sourceFile)
	const sourceFileSize = this.getContentLength(props)
	props = await this.cloudService.getObjectProps(targetFile)
	const targetFileSize = this.getContentLength(props)
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
  
  async compareSchemas(source,target,rules) {

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
	  fileContents = await this.cloudService.getObject(sourceFilePath)		      
	} catch(err) {
	  report.failed.push([source.schema,target.schema,'*',0,0,0,0,err.message])
      return report	 
	}

    const sourceControlFile = this.parseContents(fileContents)
	
	try {
	  fileContents = await this.cloudService.getObject(targetFilePath)		      
	} catch(err) {
	  report.failed.push([source.schema,target.schema,'*',0,0,0,0,err.message])
      return report	 
	}
	
    const targetControlFile = this.parseContents(fileContents)

	// Assume the source control file contains the correct options for both source and target
    this.controlFile = sourceControlFile
	
    const sourceFolder = path.dirname(sourceFilePath)
	const targetFolder = path.dirname(targetFilePath)
   
    let results = await Promise.all(Object.keys(targetControlFile.data).map((tableName) => {return this.compareFiles( this.makeCloudPath(path.join(sourceFolder,sourceControlFile.data[tableName].file)), this.makeCloudPath(path.join(targetFolder,targetControlFile.data[tableName].file)))}))
	
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
  	  // console.log('Decipher',filename,this.controlFile.yadamuOptions.encryption,this.yadamu.ENCRYPTION_KEY,iv);
	  const decipherStream = crypto.createDecipheriv(this.controlFile.yadamuOptions.encryption,this.yadamu.ENCRYPTION_KEY,iv)
	  streams.push(decipherStream);
	}

	if (this.COMPRESSED_INPUT) {
      streams.push(this.controlFile.yadamuOptions.compression === 'GZIP' ? createGunzip() : createInflate())
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

	const filename = this.makeAbsolute(this.controlFile.data[tableInfo.TABLE_NAME].file)
	const streams = await this.qaInputStreams(filename)
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
	
	const fileContents =  await this.cloudService.getObject(this.CONTROL_FILE_PATH)		
    this.controlFile = this.parseContents(fileContents)
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
  }       
  
  getControlFileSettings() {
    return this.controlFile.settings
  }
  
  setControlFileSettings(options) {
	this.parameters.OUTPUT_FORMAT = options.contentType
	this.yadamu.parameters.COMPRESSION = options.compression
	this.yadamu.parameters.ENCRYPTION = options.encryption
  }

}

module.exports = MixinCloudQA

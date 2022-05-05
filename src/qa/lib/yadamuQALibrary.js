import path            from 'path';
import assert          from 'assert';
import { 
  pipeline,
  finished
}                      from 'stream/promises';

import {  
  createHash
}                      from 'crypto'

import {
  Writable
}                      from 'stream'


import YadamuLibrary   from '../../node/lib/yadamuLibrary.js';
import NullWriter      from '../../node/util/nullWritable.js';
import RowCounter      from '../util/rowCounter.js';


class SourceTableKeys extends Writable {
	
  constructor(hashTable) {
	super({objectMode:true})
    this.hashTable = hashTable
	this.count = 0
  }

  _write(data,enc,callback) {
	this.count++
	const key = createHash('sha256').update(JSON.stringify(data)).digest('hex');
	this.hashTable.get(key)?.count === -1 ? this.hashTable.delete(key) : this.hashTable.has(key) ? this.hashTable.get(key).count++ : this.hashTable.set(key,{count : 1})
	callback()
  }	

} 

class TargetTableKeys extends Writable {
	
  constructor(hashTable) {
	super({objectMode:true})
    this.hashTable = hashTable
	this.count = 0
  }

  _write(data,enc,callback) {
	this.count++
	const key = createHash('sha256').update(JSON.stringify(data)).digest('hex');
	this.hashTable.get(key)?.count === 1 ? this.hashTable.delete(key) : this.hashTable.has(key) ? this.hashTable.get(key).count-- : this.hashTable.set(key,{count : -1})
	callback()
  }	

} 

class YadamuQALibrary {

  static qaMixin = (superclass) => class extends superclass {
    
    async initialize() {
	  await super.initialize();
      if (this.yadamu.terminateConnection(this.ROLE,this.getWorkerNumber())) {
        const pid = await this.getConnectionID();
        this.scheduleTermination(pid,this.getWorkerNumber());
      }
    }
    
    async initializeWorker(manager)  {
      await super.initializeWorker(manager);
      const idx = this.getWorkerNumber()
      // Manager needs to schedule termination of worker.
      if (this.yadamu.terminateConnection(this.ROLE,idx)) {
        const pid = await this.getConnectionID();
        this.manager.scheduleTermination(pid,idx);
      }
    }
    
    async initializeImport() {
      if (this.options.recreateSchema === true) {
        await this.recreateSchema();
      }
      await super.initializeImport();
    }   
	
    getTerminationTags(workerId,processId) {
	  return ['KILL',this.DATABASE_VENDOR,this.yadamu.killConfiguration.process,isNaN(workerId) ? 'SEQUENTIAL' : 'PARALLEL',this.ON_ERROR,workerId,this.yadamu.killConfiguration.delay,processId]
    }

    async compareResultSets(tableName, sourceSQL, targetSQL) {
	   	   
      const queryInfo = {}
      const resultTable =  new Map()
	   
	  queryInfo.SQL_STATEMENT = targetSQL
	  const source = await this.getInputStream(queryInfo)
	  const sourceKeys = new SourceTableKeys(resultTable);
	  pipeline(source,sourceKeys)
	   
	  queryInfo.SQL_STATEMENT = targetSQL
	  const target = await this.getInputStream(queryInfo)
	  const targetKeys = new TargetTableKeys(resultTable)
	  pipeline(target,targetKeys)
	 
      // console.log(source.constructor.name,sourceKeys.constructor.name,target.constructor.name,targetKeys.constructor.name)
	 
	  await Promise.all([finished(source),finished(sourceKeys),finished(target),finished(targetKeys)])
	  return [ resultTable.size === 0 ? [tableName, sourceKeys.count, targetKeys.count, 0, 0] : [tablename, sourceKeys.count, targetKeys.count, -1, -1]]
	}
  }	
  
  static loaderQAMixin = (superclass) => class extends superclass {

    async recreateSchema() {
      this.DIRECTORY = this.TARGET_DIRECTORY
      await this.cloudService.createBucketContainer()
      await this.cloudService.deleteFolder(this.IMPORT_FOLDER)
    }
      
    async calculateSortedHash(file) {
    
      const fileContents = await this.cloudService.getObject(file)        
      const array = this.parseJSON(fileContents)
   
      array.sort((r1,r2) => {
        for (const i in r1) {
          if (r1[i] < r2[i]) return -1
          if (r1[i] > r2[i]) return 1;
        }
        return 0
      })
      return createHash('sha256').update(JSON.stringify(array)).digest('hex');
    } 
   
    async calculateHash(file) {
        
      const hash = createHash('sha256');
      const is = await this.cloudService.createReadStream(file)
      await pipeline(is,hash)
      hash.end();
      hash.setEncoding('hex');
      return hash.read();
   
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
   
      const sourceControlFile = this.parseJSON(fileContents)
      
      try {
        fileContents = await this.cloudService.getObject(targetFilePath)            
      } catch(err) {
        report.failed.push([source.schema,target.schema,'*',0,0,0,0,err.message])
        return report  
      }
      
      const targetControlFile = this.parseJSON(fileContents)
   
      // Assume the source control file contains the correct options for both source and target
      this.controlFile = sourceControlFile
      
      const sourceFolder = path.dirname(sourceFilePath)
      const targetFolder = path.dirname(targetFilePath)

      let results = await Promise.all(Object.keys(targetControlFile.data).flatMap((tableName) => {
        switch (true) {
          case (sourceControlFile.data[tableName].hasOwnProperty('file') && targetControlFile.data[tableName].hasOwnProperty('file')):
            return this.compareFiles( 
              this.makeCloudPath(path.join(sourceFolder,sourceControlFile.data[tableName].file))
            , this.makeCloudPath(path.join(targetFolder,targetControlFile.data[tableName].file))
            )
            break;
          case (sourceControlFile.data[tableName].hasOwnProperty('files') && targetControlFile.data[tableName].hasOwnProperty('files')):
            return sourceControlFile.data[tableName].files.map((file,idx) => {
              return this.compareFiles( 
                this.makeCloudPath(path.join(sourceFolder,file))
              , this.makeCloudPath(path.join(targetFolder,targetControlFile.data[tableName].files[idx]))
              )
            })
            break;
          default:
            // Cannot meaningfully compare partitioned and non-partitioned data sets
            return [[-1,-1,-1,-1,'Cannot meaningfully compare partitioned and non-partitioned data']]
        }
      }))
      
      Object.keys(targetControlFile.data).map((tableName,idx) => {
        const result = results[idx]
        if ((result[0] !== -1) && (result[0] === result[1]) && (result[2] === result[3])) {
          report.successful.push([source.schema,target.schema,tableName,result[0]])
        }
        else {
          report.failed.push([source.schema,target.schema,tableName,result[0],result[1],result[2],result[3],result[4] || null])
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
      
      const fileList = this.controlFile.data[tableInfo.TABLE_NAME].files || [this.controlFile.data[tableInfo.TABLE_NAME].file]
      const rowCounts = await Promise.all(fileList.map(async (file) => {
        const exportFilePath = this.makeAbsolute(file)
        const streams = []
        try {
          streams.push(...await this.qaInputStreams(exportFilePath))
        } catch (e) {
          if (e.FileNotFound && e.FileNotFound()) {
            // this.yadamuLogger.error(['COMPARE','ROW COUNT'],`Cannot Locate File "${exportFilePath}".`)
            this.yadamuLogger.handleException(['COMPARE','ROW COUNT'],e)
            return 0
          }
          throw e
        }
        const rowCounter = new RowCounter(exportFilePath,this.yadamuLogger)
        streams.push(rowCounter)
        const nullStream = new NullWriter();
        streams.push(nullStream)  
        // this.yadamuLogger.trace([this.constructor.name,'PIPELINE',tableInfo.TABLE_NAME,],`${streams.map((s) => { return s.constructor.name }).join(' => ')}`)
        await pipeline(streams)
        return rowCounter.ROW_COUNT
      }))
      return rowCounts.reduce((a, b) => {return a + b}, 0)
      
    }
    
    async getRowCounts(target) {
   
      this.DIRECTORY = this.TARGET_DIRECTORY
      this.setFolderPaths(this.IMPORT_FOLDER,target.schema)
      
      const fileContents =  await this.cloudService.getObject(this.CONTROL_FILE_PATH)     
      this.controlFile = this.parseJSON(fileContents)
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
}

export { YadamuQALibrary as default}


import fs               from 'fs';
import fsp              from 'fs/promises';
import path             from 'path';
import assert           from 'assert';

import { 
  pipeline 
}                       from 'stream/promises';

import {  
  createHash
}                       from 'crypto'

import YadamuLibrary    from '../../lib/yadamuLibrary.js';
import NullWriter       from '../../util/nullWritable.js';

import YadamuCompare    from '../base/yadamuCompare.js'

import LoaderDBI        from './loaderDBI.js';
import JSONParser       from './jsonParser.js';
import ArrayWriter      from './arrayWriter.js';
import RowCounter       from './rowCounter.js';

class LoaderCompare extends YadamuCompare {

  constructor(dbi,configuration) {
    super(dbi,configuration)
  }

  async getArray(filename) {

	const streams = await this.dbi.compareInputStreams(filename)
		
	const jsonParser = new JSONParser(this.LOGGER, this.MODE, filename)
	streams.push(jsonParser);
	
	const arrayWriter = new ArrayWriter(this)
	streams.push(arrayWriter)

    await pipeline(streams)
    return arrayWriter.getArray()
  }

    async getRowCount(controlFile, tableInfo) {
      
      const fileList = controlFile.data[tableInfo.TABLE_NAME].files || [controlFile.data[tableInfo.TABLE_NAME].file]
      const rowCounts = await Promise.all(fileList.map(async (file) => {
        const exportFilePath = this.dbi.makeAbsolute(file)
        const streams = []
		this.dbi.controlFile = controlFile
        try {
          streams.push(...await this.dbi.compareInputStreams(exportFilePath))
        } catch (e) {
          if (e.FileNotFound && e.FileNotFound()) {
            // this.LOGGER.error(['COMPARE','ROW COUNT'],`Cannot Locate File "${exportFilePath}".`)
            this.LOGGER.handleException(['COMPARE','ROW COUNT'],e)
            return 0
          }
          throw e
        }
        const rowCounter = new RowCounter(exportFilePath,this.LOGGER)
        streams.push(rowCounter)
        const nullStream = new NullWriter();
        streams.push(nullStream)  
        // this.LOGGER.trace([this.constructor.name,'PIPELINE',tableInfo.TABLE_NAME,],`${streams.map((s) => { return s.constructor.name }).join(' => ')}`)
        await pipeline(streams)
        return rowCounter.ROW_COUNT
      }))
      return rowCounts.reduce((a, b) => {return a + b}, 0)
      
    }
    
    async getRowCounts(target) {

      this.dbi.DIRECTORY = this.dbi.TARGET_DIRECTORY
      this.dbi.setFolderPaths(this.dbi.IMPORT_FOLDER,target)
      
      const fileContents =  await this.dbi.cloudService.getObject(this.dbi.CONTROL_FILE_PATH)     
      const controlFile = this.dbi.parseJSON(fileContents)
      let counts 
      switch (controlFile.settings.contentType) {
        case 'JSON':
          counts = await Promise.all(Object.keys(controlFile.data).map((k) => {
            return this.getRowCount(controlFile,{TABLE_NAME:k})
          }))
              
          return Object.keys(controlFile.data).map((k,i) => {
            return [target,k,counts[i]]
          })  
        case 'CSV':
          stack = new Error().stack
          counts = await Promise.all(Object.values(controlFile.data).map((t) => {
            return new Promise((resolve,reject) => {
              let count = 0;
              fs.createReadStream(this.makeRelative(t.file)).pipe(this.getCSVParser()).on('data', (data) => {count++}).on('end', () => {resolve(count)});
            })
          }))
           
          return Object.keys(controlFile.data).map((k,i) => {
            return [target,k,counts[i]]
          })  
      }
    }    	
    
  async calculateHash(file) {
      
    const hash = createHash('sha256');
    const is = await this.dbi.cloudService.createReadStream(file)
    await pipeline(is,hash)
    hash.end();
    hash.setEncoding('hex');
    return hash.read();
   
  }  

  async calculateSortedHash(filename) {
    const array = await this.getArray(filename)
	array.sort((r1,r2) => {
	  for (const i in r1) {
        if (r1[i] < r2[i]) return -1 
		if (r1[i] > r2[i]) return 1
      }
	  return 0
    })
    return createHash('sha256').update(JSON.stringify(array)).digest('hex');
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
  
    async compareSchemas(source,target,rules) {
   
      const report = {
        successful : []
      , failed     : []
      }
       
      this.dbi._BASE_DIRECTORY = undefined
      this.dbi.DIRECTORY = this.dbi.SOURCE_DIRECTORY
      this.dbi.setFolderPaths(path.join(this.dbi.BASE_DIRECTORY,source),source)
      const sourceFilePath = this.dbi.CONTROL_FILE_PATH;
      
      this.dbi._BASE_DIRECTORY = undefined
      this.dbi.DIRECTORY = this.dbi.TARGET_DIRECTORY
      this.dbi.setFolderPaths(path.join(this.dbi.BASE_DIRECTORY,target),target)
      const targetFilePath = this.dbi.CONTROL_FILE_PATH;
      
      try {
        assert.notEqual(sourceFilePath,targetFilePath,`Source & Target control files are identical: "${sourceFilePath}"`);
      } catch(err) {
        report.failed.push([source,target,'',0,0,0,0,err.message])
        return report  
      }
      
      let fileContents    
	  
      try {
        fileContents = await this.dbi.cloudService.getObject(sourceFilePath)            
      } catch(err) {
        report.failed.push([source,target,'*',0,0,0,0,err.message])
        return report  
      }
   
      const sourceControlFile = this.dbi.parseJSON(fileContents)
      
      try {
        fileContents = await this.dbi.cloudService.getObject(targetFilePath)            
      } catch(err) {
        report.failed.push([source,target,'*',0,0,0,0,err.message])
        return report  
      }
      
      const targetControlFile = this.dbi.parseJSON(fileContents)
   
      // Assume the source control file contains the correct options for both source and target
      this.dbi.controlFile = sourceControlFile
      
      const sourceFolder = path.dirname(sourceFilePath)
      const targetFolder = path.dirname(targetFilePath)

      let results = await Promise.all(Object.keys(targetControlFile.data).flatMap((tableName) => {
        switch (true) {
          case (sourceControlFile.data[tableName].hasOwnProperty('file') && targetControlFile.data[tableName].hasOwnProperty('file')):
            return this.compareFiles( 
              this.dbi.makeCloudPath(path.join(sourceFolder,sourceControlFile.data[tableName].file))
            , this.dbi.makeCloudPath(path.join(targetFolder,targetControlFile.data[tableName].file))
            )
            break;
          case (sourceControlFile.data[tableName].hasOwnProperty('files') && targetControlFile.data[tableName].hasOwnProperty('files')):
            return sourceControlFile.data[tableName].files.map((file,idx) => {
              return this.compareFiles( 
                this.dbi.makeCloudPath(path.join(sourceFolder,file))
              , this.dbi.makeCloudPath(path.join(targetFolder,targetControlFile.data[tableName].files[idx]))
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
          report.successful.push([source,target,tableName,result[0]])
        }
        else {
          report.failed.push([source,target,tableName,result[0],result[1],result[2],result[3],result[4] || null])
        }
      })
      return report
    }
              
}

export { LoaderCompare as default }



import fs               from 'fs';
import fsp              from 'fs/promises';
import path             from 'path';
import assert           from 'assert';

import { 
  Writable 
}                       from 'stream';

import { 
  pipeline 
}                       from 'stream/promises';

import {  
  createHash
}                       from 'crypto'


import csv              from 'csv-parser';

import YadamuLibrary    from '../../lib/yadamuLibrary.js';
import NullWriter       from '../../util/nullWritable.js';

import YadamuCompare    from '../base/yadamuCompare.js'
import DBIConstants     from '../base/dbiConstants.js'

import {
  FileNotFound
}                       from '../file/fileException.js'

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
		
	const jsonParser = new JSONParser(this.MODE, filename, DBIConstants.PIPELINE_STATE, this.LOGGER)
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
        try {
          streams.push(...await this.dbi.compareInputStreams(exportFilePath))
        } catch (e) {
          if (this.fileNotFound(e)) {
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
	
	fileNotFound(e) {
	  return (e instanceof FileNotFound) || ((typeof e.notFound === 'function') && e.notFound())
	}
	
	async countCSVRows(inputStream) {
	  let count = 0
	  await pipeline(inputStream,csv({headers: false}),new Writable({objectMode: true, write(row,encoding,callback) { count++; callback() }}))
	  return count
	}
	
	async csvRowCount(exportFilePath) {
	  return this.countCSVRows(await this.dbi.cloudService.createReadStream(this.dbi.makeRelative(exportFilePath)))
	}
	
    async getCSVRowCount(controlFile, tableInfo) {
      const fileList = controlFile.data[tableInfo.TABLE_NAME].files || [controlFile.data[tableInfo.TABLE_NAME].file]
      const rowCounts = await Promise.all(fileList.map(async (file) => {
        const exportFilePath = this.dbi.makeAbsolute(file)
        try {
          return await this.csvRowCount(exportFilePath)
        } catch (e) {
          if (this.fileNotFound(e)) {
            this.LOGGER.handleException(['COMPARE','ROW COUNT'],e)
            return 0
          }
          throw e
        }
      }))
	  return rowCounts.reduce((a, b) => {return a + b}, 0)
    }
   	
    async getRowCounts(target) {

      this.dbi.DIRECTORY = this.dbi.TARGET_DIRECTORY
      this.dbi.setFolderPaths(this.dbi.IMPORT_FOLDER,target)
      
      const fileContents =  await this.dbi.cloudService.getContentAsString(this.dbi.CONTROL_FILE_PATH)     
      const controlFile = this.dbi.parseJSON(fileContents)
      this.dbi.controlFile = controlFile
	  
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
          counts = await Promise.all(Object.keys(controlFile.data).map((k) => {
            return  this.getCSVRowCount(controlFile,{TABLE_NAME:k})
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
        fileContents = await this.dbi.cloudService.getContentAsString(sourceFilePath)            
      } catch(err) {
        report.failed.push([source,target,'*',0,0,0,0,err.message])
        return report  
      }
   
      const sourceControlFile = this.dbi.parseJSON(fileContents)
      
      try {
        fileContents = await this.dbi.cloudService.getContentAsString(targetFilePath)            
      } catch(err) {
        report.failed.push([source,target,'*',0,0,0,0,err.message])
        return report  
      }
      
      const targetControlFile = this.dbi.parseJSON(fileContents)
   
      // Assume the source control file contains the correct options for both source and target
      this.dbi.controlFile = sourceControlFile
      
      const sourceFolder = path.dirname(sourceFilePath)
      const targetFolder = path.dirname(targetFilePath)

      const filesMatch = (result) => {
        return (result[4] === undefined) && (result[0] !== -1) && (result[0] === result[1]) && (result[2] === result[3])
      }

      // One result per table. A partitioned table matches only if every partition file matches.
      const tableNames = Object.keys(targetControlFile.data)
      const results = await Promise.all(tableNames.map(async (tableName) => {
        const sourceTable = sourceControlFile.data[tableName]
        const targetTable = targetControlFile.data[tableName]
        switch (true) {
          case (sourceTable === undefined):
            return [-1,-1,-1,-1,'Table not found in source control file']
          case (sourceTable.hasOwnProperty('file') && targetTable.hasOwnProperty('file')):
            return this.compareFiles(
              this.dbi.makeCloudPath(path.join(sourceFolder,sourceTable.file))
            , this.dbi.makeCloudPath(path.join(targetFolder,targetTable.file))
            )
          case (sourceTable.hasOwnProperty('files') && targetTable.hasOwnProperty('files')):
            if (sourceTable.files.length !== targetTable.files.length) {
              return [-1,-1,-1,-1,`Partition count mismatch: source ${sourceTable.files.length}, target ${targetTable.files.length}`]
            }
            const partitionResults = await Promise.all(sourceTable.files.map((file,idx) => {
              return this.compareFiles(
                this.dbi.makeCloudPath(path.join(sourceFolder,file))
              , this.dbi.makeCloudPath(path.join(targetFolder,targetTable.files[idx]))
              )
            }))
            const sourceSize = partitionResults.reduce((total,result) => { return total + result[0] },0)
            const targetSize = partitionResults.reduce((total,result) => { return total + result[1] },0)
            const mismatched = partitionResults.flatMap((result,idx) => { return filesMatch(result) ? [] : [idx+1] })
            if (mismatched.length === 0) {
              const tableHash = createHash('sha256').update(partitionResults.map((result) => { return result[2] }).join()).digest('hex')
              return [sourceSize,targetSize,tableHash,tableHash]
            }
            // Report the hashes of the first partition that differs
            const firstMismatch = partitionResults[mismatched[0]-1]
            return [sourceSize,targetSize,firstMismatch[2],firstMismatch[3],`Partitions ${mismatched.join(',')} of ${partitionResults.length} differ`]
          default:
            // Cannot meaningfully compare partitioned and non-partitioned data sets
            return [-1,-1,-1,-1,'Cannot meaningfully compare partitioned and non-partitioned data']
        }
      }))

      tableNames.forEach((tableName,idx) => {
        const result = results[idx]
        if (filesMatch(result)) {
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


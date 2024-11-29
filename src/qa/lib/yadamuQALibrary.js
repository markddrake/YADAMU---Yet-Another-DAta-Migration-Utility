
import path            from 'path';

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
    
    setOption(name,value) {
	  this.options[name] = value;
    }
  
    async initialize() {
	  await super.initialize();
	  if (this.yadamu.scheduleLostConnectionTest(this.ROLE,this.getWorkerNumber())) {
        const pid = await this.getConnectionID();
        this.scheduleTermination(pid,this.getWorkerNumber());
      }
    }
    
    async initializeWorker(manager)  {
      await super.initializeWorker(manager);
      const idx = this.getWorkerNumber()
      // Manager needs to schedule termination of worker.
	  if (this.yadamu.scheduleLostConnectionTest(this.ROLE,idx)) {
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
	  return ['KILL',this.DATABASE_VENDOR,this.ROLE,isNaN(workerId) ? 'SEQUENTIAL' : 'PARALLEL',this.ON_ERROR,workerId,this.yadamu.TERMINATION_CONFIGURATION.delay,processId]
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

  static fileQAMixin = (superclass) => class extends superclass {
    
    setOption(name,value) {
	  this.options[name] = value;
    }
  
  }
  
  static loaderQAMixin = (superclass) => class extends superclass {

    setOption(name,value) {
	  this.options[name] = value;
    }
  
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

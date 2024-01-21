
import { 
  performance 
}                          from 'perf_hooks';

import Yadamu              from '../../core/yadamu.js';
import YadamuLibrary       from '../../lib/yadamuLibrary.js';
import _JSONOutputManager  from '../file/jsonOutputManager.js';

class JSONOutputManager extends _JSONOutputManager{

  /* Supress the table name from the file */

  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {
    super(dbi,tableName,pipelineState,true,status,yadamuLogger)
	this.startTable = '['
  }
  
  async initializePartition(partitionInfo) {
	await super.initializePartition(partitionInfo)
  	this.tableInfo.partitionsRemaining = this.tableInfo.partitionsRemaining || partitionInfo.partitionCount
    this.PIPELINE_STATE.displayName = partitionInfo.displayName
    this.PIPELINE_STATE.partitionCount = partitionInfo.partitionCount
    this.PIPELINE_STATE.partitionedTableName = this.tableName
  }
  
  /*
  async doDestroy() {
	await super.doDestroy();
    console.log(this.PARTITIONED_TABLE,this.tableInfo,this.PIPELINE_STATE)
	if (this.PARTITITONED_TABLE) {
	}
  }
  */
  
}

export {JSONOutputManager as default }
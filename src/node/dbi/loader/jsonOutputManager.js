"use strict"

import { performance } from 'perf_hooks';

import Yadamu from '../../core/yadamu.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import _JSONOutputManager from '../file/jsonOutputManager.js';

class JSONOutputManager extends _JSONOutputManager{

  /* Supress the table name from the file */

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,true,status,yadamuLogger)
	this.startTable = '['
  }
  
  async initializePartition(partitionInfo) {
	await super.initializePartition(partitionInfo)
  	this.tableInfo.partitionsRemaining = this.tableInfo.partitionsRemaining || partitionInfo.partitionCount
  }
  
  /*
  async doDestroy() {
	await super.doDestroy();
    console.log(this.PARTITIONED_TABLE,this.tableInfo,this.COPY_METRICS)
	if (this.PARTITITONED_TABLE) {
	}
  }
  */
  
}

export {JSONOutputManager as default }
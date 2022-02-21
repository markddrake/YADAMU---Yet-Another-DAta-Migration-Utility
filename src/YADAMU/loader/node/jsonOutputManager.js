"use strict"

import { performance } from 'perf_hooks';

import Yadamu from '../../common/yadamu.js';
import YadamuLibrary from '../../common/yadamuLibrary.js';
import _JSONOutputManager from '../../file/node/jsonOutputManager.js';

class JSONOutputManager extends _JSONOutputManager{

  /* Supress the table name from the file */

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,true,status,yadamuLogger)
	this.startTable = '['
  }
  
}

export {JSONOutputManager as default }
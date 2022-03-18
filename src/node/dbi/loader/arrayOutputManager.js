"use strict"

import { performance } from 'perf_hooks';

import JSONOutputManager from './jsonOutputManager.js';

class ArrayOutputManager extends JSONOutputManager {

  
  // Write each row as a JSON array without a surrounding Array and without a comma seperating the rows. 
  // Each array is on a seperate line
  
  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }
   
  formatRow(row) {
    return `${JSON.stringify(row)}\r\n`
  }

  beginTable() { /* OVERRIDE */ }
      
  finalizeTable() { /* OVERRIDE */ }
  
}

export {ArrayOutputManager as default }
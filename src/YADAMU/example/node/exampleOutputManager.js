"use strict"

import { performance } from 'perf_hooks';

import Yadamu from '../../common/yadamu.js';
import YadamuLibrary from '../../common/yadamuLibrary.js';
import YadamuOutputManager from '../../common/yadamuOutputManager.js';
import {BatchInsertError} from '../../common/yadamuException.js'

class ExampleWriter extends YadamuOutputManager {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }

  generateTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
 
    return  this.tableInfo.targetDataTypes.map((targetDataType,idx) => {        
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);
      /*    
      if (YadamuLibrary.isBinaryType(dataType.type)){
        // For Interfaces that what Binary content rendered as hexBinary string 
        return (col,idx) => {
          return (Buffer.isBuffer(col)) return col.toString('hex') : col
          }
        } 
      }
      */
      switch (dataType.type.toLowerCase()) {
        case "json":
          return (col,idx) => {
            return typeof col === 'object' ? JSON.stringify(col) : col
          }
          break;
        case 'bit':
        case 'boolean':
          return (col,idx) => {
            return YadamuLibrary.toBoolean(col)
          }
          break;
        default :
          return null
      }
    })  
  }
  
  async setTableInfo(tableInfo) {
    await super.setTableInfo(tableInfo)
  }
  
  /*
  **
  ** The default implimentation is shown below. It applies any transformation functions that have were defiend in setTableInfo andt
  ** pushes the row into an array or rows waiting to fed to a batch insert mechanism
  **
  ** If your override this function you must ensure that this.COPY_METRICS.cached is incremented once for each call to cache row.
  ** 
  ** Also if your solution does not cache one row in this.batch for each row processed you will probably need to override the following 
  ** functions in addtion to cache row.
  **
  **  batchComplete() : returns true when it it time to perform a bulk insert.
  **   
  **  handleBatchException(): creates an exception containing a summary of the records being inserted if an error occurs during a batch insert.
  **  
    
  this.rowTransformation(row)
  this.batch.push(row);
    
  this.COPY_METRICS.cached++
  return this.skipTable;
   
  **
  */

  cacheRow(row) {
    super.cacheRow(row)
  }
      
}

export { ExampleWriter;
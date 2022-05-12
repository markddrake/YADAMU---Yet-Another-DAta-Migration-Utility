
import { 
  performance 
}                               from 'perf_hooks';
						
import YadamuLibrary            from '../../lib/yadamuLibrary.js'
import YadamuSpatialLibrary     from '../../lib/yadamuSpatialLibrary.js'

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuOutputManager      from '../base/yadamuOutputManager.js'

class ExampleOutputManager extends YadamuOutputManager {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }

  generateTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
 
rmations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
 
    return  this.tableInfo.targetDataTypes.map((targetDataType,idx) => {        
      const dataTypeDefinition = YadamuDataTypes.decomposeDataType(targetDataType);
	  switch (dataTypeDefinition.type.toUpperCase()) {
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

export { ExampleOutputManager as default }
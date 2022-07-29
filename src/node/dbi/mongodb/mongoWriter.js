
import { performance } from 'perf_hooks';

import WKX from 'wkx';

import mongodb from 'mongodb'
const { ObjectID, Decimal128, Long} = mongodb

import Yadamu from '../../core/yadamu.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import YadamuWriter from '../base/yadamuWriter.js';
import {BatchInsertError} from '../../core/yadamuException.js'

class MongoWriter extends YadamuWriter {

  /*
  **
  ** MongoDB Support allows data from a relational database to be imported using one of the following
  **
  ** If the relational table consists of a single column of Type JSON 
  **
  **   DOCUMENT:
  **  
  **     If column contains an object then the JSON object will become the mongo document.
  **
  **     If the column contains an array the array will be wrapped in an object containing a single key "array"
  **
  ** If the relational table has multiple columns then three modes are avaialbe 
  ** 
  **    OBJECT: The row is inserted an object. The document contains one key for each column in the table
  **
  **    ARRAY:  The row is inserted as object. The array containing the values from the table will be wrapped in an object containing a single key "row". 
  **            A document containing the table's metadata will be inserted into the YadamuMetadata collection.
  **
  **    BSON:   A BOSN object is contructed based on the relational metadata. 
  **  
  */

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }
  
  setTableInfo(tableName) {
	super.setTableInfo(tableName)
        
	// Set up the batchRow() function used by cacheRow...

	switch (this.tableInfo.insertMode) {
      case 'DOCUMENT' :
	    this.batchRow = (row) => {
          if (Array.isArray(row[0])) {
            this.batch.push({array : row[0]})
          }
          else {
            this.batch.push(row[0]);
          }
		}
        break;
      case 'BSON':
	    this.batchRow = (row) => {
          const bsonDocument = {}
          this.tableInfo.columnNames.forEach((key,idx) => {
             bsonDocument[key] = row[idx]
          });
          this.batch.push(bsonDocument);
		}
        break;
      case 'OBJECT' :
	    this.batchRow = (row) => {
          const jsonDocument = {}
          this.tableInfo.columnNames.forEach((key,idx) => {
             jsonDocument[key] = row[idx]
          });
          this.batch.push(jsonDocument);
		}
        break;
      case 'ARRAY' :
 	    this.batchRow = (row) => {
          this.batch.push({ row : row });
		}
        break;
      default:
 	    this.batchRow = (row) => {
          this.batch.push({ row : row });
		}
    }

  }
  
  async initialize(obj) {      
    await super.initialize(obj);
    this.collection = await this.dbi.collection(this.tableInfo.tableName)
    if (this.tableInfo.insertMode === 'ARRAY') {
      const result = await this.dbi.insertOne('yadamuMetadata',this.tableInfo);
    }
  }

  cacheRow(row) {
      
    // Apply the row transformation and add row to the current batch.
	
	this.rowTransformation(row)
	this.batchRow(row)
    this.COPY_METRICS.cached++
    return this.skipTable

  }
  
  reportBatchError(batch,operation,cause) {
   	super.reportBatchError(operation,cause,batch[0],batch[batch.length-1])
  }
   
  getMetrics() {
	const tableStats = super.getMetrics()
	tableStats.insertMode = this.tableInfo.insertMode
    return tableStats;  
  }
  
  async _writeBatch(batch,rowCount) {
	      
     // ### Todo: ERROR HANDLING and Iterative Mode.
	 
    try {
	  const results = await this.dbi.insertMany(this.tableInfo.tableName,batch);
      this.endTime = performance.now();
      this.adjustRowCounts(rowCount);
      this.releaseBatch(batch)
	  return this.skipTable
    } catch (cause) {
	  this.reportBatchError(batch.map((r) => { return Object.values(r)}),`INSERT MANY`,cause)
      this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
    } 
    
         
	for (const row in batch) {
      try {
        const results = await this.dbi.insertOne(this.tableInfo.tableName,batch[row]);
        this.adjustRowCounts(1);
      } catch(cause) {
        this.handleIterativeError(`INSERT ONE`,cause,row,Object.values(batch[row]));
        if (this.skipTable) {
          break;
        }
      }
    }

    this.endTime = performance.now();
    this.releaseBatch(batch)
    return this.skipTable  

  }
}

export { MongoWriter as default }
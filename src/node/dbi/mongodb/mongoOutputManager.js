
import { 
  performance 
}                               from 'perf_hooks';
						
import WKX from 'wkx';

import mongodb                  from 'mongodb'
const { 
  ObjectID, 
  Decimal128, 
  Long, 
  Int32, 
  Double
} = mongodb

import YadamuLibrary            from '../../lib/yadamuLibrary.js'

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuOutputManager      from '../base/yadamuOutputManager.js'

class MongoOutputManager extends YadamuOutputManager {

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
  
  generateTransformations(dataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
  
	const transformations = dataTypes.map((dataType,idx) => {      
	
	   switch(dataType.toLowerCase()){
        case this.dbi.DATA_TYPES.MONGO_OBJECTID_TYPE:
	      return (col,idx) => {
			return ObjectID(col)
	      }
          break;
		case this.dbi.DATA_TYPES.INTEGER_TYPE:
          return (col,idx) => {
            return new Int32(col)
	      }		
    	case this.dbi.DATA_TYPES.DOUBLE_TYPE:
          return (col,idx) => {
            return new Double(col)
	      }		
		case this.dbi.DATA_TYPES.MONGO_DECIMAL128_TYPE:
		  return (col,idx) => {
			 return Decimal128.fromString( typeof col === 'string' ? col : col.toString()) 
	      }
          break;
		case this.dbi.DATA_TYPES.MONGO_BIGINT_TYPE:
		  return (col,idx) => {
			 return Long.fromString(col)
	      }
          break;
        case this.dbi.DATA_TYPES.SPATIAL_TYPE:
          switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
            case "WKB":
            case "EWKB":
              return (col,idx) => {
				// Handle case where incoming format is already GeoJSON
			    return Buffer.isBuffer(col) ? WKX.Geometry.parse(col).toGeoJSON() : col
			  }
 			  return null
            case "WKT":
            case "EWKT":
              return (col,idx) => {
        	    return WKX.Geometry.parse(col).toGeoJSON()
              }
            default:
          }
		  return null
        case this.dbi.DATA_TYPES.BOOLEAN_TYPE:
          return (col,idx) => {
            return YadamuLibrary.toBoolean(col)
	      }
        case this.dbi.DATA_TYPES.MONGO_OBJECT_TYPE:
        case this.dbi.DATA_TYPES.MONGO_ARRAY_TYPE:
          return (col,idx) => {
            return typeof col === 'string' && (col.length > 0) ? JSON.parse(col) : col
	      }
		case this.dbi.DATA_TYPES.BINARY_TYPE:
		  if ((this.tableInfo.columnNames[idx] === '_id') && (this.tableInfo.sizeConstraints[idx][0] === 12)) {
  	        return (col,idx) => {
              return ObjectID(col)
	        }
		  }
		  return null
		case this.dbi.DATA_TYPES.DATE_TYPE:
		  if (this.dbi.MONGO_NATIVEJS_DATE) {
	        return (col,idx) => {
              return new Date(col)
	        }		
          }			
		  return null
    	default:
		  /*
		  if (this.dbi.DATA_TYPES.isNumericType(dataType)) {
			return (col,idx) => {
			  if (typeof col === 'string') {
			    transformations[idx] = (col,idx) => {
				  return Number(col)
				}
			    return Number(col)
			  }
			  else {
                transformations[idx] = null
				return col
			  }
			}
          }	
          */		  
          // First time through test if data is string and first character is '[' or ']'
		  // TODO ### Trim and test last character is matching ']' or '}'
		  if (this.dbi.MONGO_PARSE_STRINGS) {
		    return (col,idx) => {
			  if (typeof col === 'string' && ((col.indexOf('[') === 0) || (col.indexOf('{') === 0))) {
			    try {
			      const res = JSON.parse(col)
				  // If the parse succeeds remove the test for the remaining records.
				  transformations[idx] = (col,idx) => {
				    try {
  			          return JSON.parse(col)
				    } catch (e) {
					  // If the parse fails disable the parse on the remaining records.
		              transformations[idx] = null
					  return col
			        }
				  }	
                  return res				
			    } catch (e) {
				  // If the parse fails remove the parse 
		          transformations[idx] = null
			    }
			  }
  		      return col
			}
		  }
		  return null
	  }
	})
	
	return transformations
	
  }

  async setTableInfo(tableName) {
	await super.setTableInfo(tableName)
	    
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
  
}

export { MongoOutputManager as default }
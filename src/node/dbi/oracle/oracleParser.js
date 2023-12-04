
import { pipeline } from 'stream/promises';

import oracledb from 'oracledb';

import YadamuParser from '../base/yadamuParser.js'
import StringWriter from '../../util/stringWriter.js';
import BufferWriter from '../../util/bufferWriter.js';
import YadamuSpatialLibrary from '../../lib/yadamuSpatialLibrary.js'

import {OracleError} from './oracleException.js';

class OracleParser extends YadamuParser {
  
  constructor(dbi,queryInfo,yadamuLogger,parseDelay) {
    super(dbi,queryInfo,yadamuLogger,parseDelay)
	// console.log(queryInfo)
  }

  setColumnMetadata(resultSetMetadata) { 
  
    if (this.COPY_METRICS.failed && resultSetMetadata === undefined) {
	  // Oracle appears to raise the metadatata event appears to raised and supply undefined metadata if an error (such as A DDL error) terminates the pipeline
	  return
	}
    
	this.deferredTransformations = new Array(resultSetMetadata.length).fill(null)
    
    this.transformations = resultSetMetadata.map((column,idx) => {
	  const dataType = this.queryInfo.DATA_TYPE_ARRAY[idx].toUpperCase()
	  switch (column.fetchType) {
		case oracledb.DB_TYPE_NCLOB:
		case oracledb.DB_TYPE_CLOB:
		  switch (dataType) {
		    case this.dbi.DATA_TYPES.SPATIAL_TYPE:
			  if ((this.dbi.latestError instanceof OracleError) && this.dbi.latestError.knownBug(33561708)) {
		        this.deferredTransformations[idx] = (row,idx)  => {
                  row[idx] = YadamuSpatialLibrary.geoJSONtoWKB(JSON.parse(row[idx]))
		        }
			  }
			default:
          }
       	  return (row,idx)  => {
			row[idx] = row[idx].getData()
		  }
	    case oracledb.DB_TYPE_BLOB:	
	      if (this.queryInfo.jsonColumns.includes(idx)) {
            // Convert JSON store as BLOB to string
		    this.deferredTransformations[idx] = (row,idx)  => {
              row[idx] = row[idx].toString('utf8')
		    }
          }
          return (row,idx)  => {
            // row[idx] = this.blobToBuffer(row[idx])
			row[idx] = row[idx].getData()
		  }
        default:
 		  switch (dataType) {
		    case 'BINARY_FLOAT':
			case 'BINARY_DOUBLE':
		      return (row,idx)  => {
			    switch (row[idx]) {
  			      case 'Inf':
					row[idx] = 'Infinity'
				    break;
				  case '-Inf':
					row[idx] = '-Infinity'
				    break;
				  case 'Nan':
					row[idx] = 'NaN'
				    break;
				  default:
                }				  
		      }
			default:
		  }
  		  return null;
      }
    })
	
	// Use a dummy rowTransformation function if there are no transformations required.
	
    this.rowTransformation = this.transformations.every((currentValue) => { return currentValue === null}) ? (row) => {} : (row) => {
      this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }
	
    this.deferredTransformation = this.deferredTransformations.every((currentValue) => { return currentValue === null}) ? (row) => {} : (row) => {
      this.deferredTransformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }
    
  }
  	  
  async doTransform(data) {  
    // if (this.COPY_METRICS.parsed == 1) console.log(data)
    data = await super.doTransform(data)
	const row = await Promise.all(data)
    this.deferredTransformation(row)
	// if (this.COPY_METRICS.parsed == 1) console.log(row)
	return row
  }
}

export {OracleParser as default }

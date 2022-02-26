"use strict" 

import { pipeline } from 'stream/promises';

import oracledb from 'oracledb';

import YadamuParser from '../base/yadamuParser.js'
import StringWriter from '../../util/stringWriter.js';
import BufferWriter from '../../util/bufferWriter.js';

import {OracleError} from './oracleException.js';

class OracleParser extends YadamuParser {
  
  constructor(queryInfo,yadamuLogger,parseDelay) {
    super(queryInfo,yadamuLogger,parseDelay); 
	// console.log(queryInfo)
  }

  setColumnMetadata(resultSetMetadata) { 
    
	this.jsonTransformations = new Array(resultSetMetadata.length).fill(null)
    
    this.transformations = resultSetMetadata.map((column,idx) => {
	  switch (column.fetchType) {
		case oracledb.DB_TYPE_NCLOB:
		case oracledb.DB_TYPE_CLOB:
		  return (row,idx)  => {
            // row[idx] = this.clobToString(row[idx])
			row[idx] = row[idx].getData()
		  }           
	    case oracledb.DB_TYPE_BLOB:	
          if (this.queryInfo.jsonColumns.includes(idx)) {
            // Convert JSON store as BLOB to string
		    this.jsonTransformations[idx] = (row,idx)  => {
              row[idx] = row[idx].toString('utf8')
		    }
          }
          return (row,idx)  => {
            // row[idx] = this.blobToBuffer(row[idx])
			row[idx] = row[idx].getData()
		  }
        default:
 		  switch (this.queryInfo.DATA_TYPE_ARRAY[idx].toUpperCase()) {
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
	
    this.jsonTransformation = this.jsonTransformations.every((currentValue) => { return currentValue === null}) ? (row) => {} : (row) => {
      this.jsonTransformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          transformation(row,idx)
        }
      }) 
    }
    
  }
  	  
  async doTransform(data) {  
    data = await super.doTransform(data)
	const row = await Promise.all(data)
	this.jsonTransformation(row)
    return row
  }
}

export {OracleParser as default }

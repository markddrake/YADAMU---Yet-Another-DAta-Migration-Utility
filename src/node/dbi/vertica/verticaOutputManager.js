"use strict"

import crypto from 'crypto';
import { performance } from 'perf_hooks';
import fsp from 'fs/promises';
import path from 'path'

import Yadamu from '../../core/yadamu.js';
import YadamuLibrary from '../../lib/yadamuLibrary.js';
import YadamuSpatialLibrary from '../../lib/yadamuSpatialLibrary.js';
import YadamuOutputManager from '../base/yadamuOutputManager.js';
import StringWriter from '../../util/stringWriter.js';
import {FileError, FileNotFound, DirectoryNotFound} from '../file/fileException.js';
import {WhitespaceIssue, EmptyStringDetected, ContentTooLarge, StagingAreaMisMatch} from './verticaException.js'
import { VerticaError, VertiaCopyOperationFailure } from './verticaException.js'

class VerticaOutputManager extends YadamuOutputManager {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }

  createBatch() {
	return {
	  copy          : []
	, insert        : []
    }
  }  
    
  resetBatch(batch) {
    batch.copy.length = 0;
	batch.insert.length = 0;
  }
  
  toSQLInterval(interval) {
    const jsInterval = YadamuLibrary.parse8601Interval(interval)
	switch (jsInterval.type) {
	  case 'YM':
  	    return `${jsInterval.years || 0}-${jsInterval.months || 0}`
	  case 'DMS':
  	    return `${jsInterval.days || 0}:${jsInterval.hours || 0}:${jsInterval.minutes || 0}:${jsInterval.seconds || 0}`
	  default:
	    return interval;
	}
  }	
  
  toSQLIntervalYM(interval) {
    const jsInterval = YadamuLibrary.parse8601Interval(interval)
    return `${jsInterval.years || 0}-${jsInterval.months || 0}`
  }	
  
  toSQLIntervalDMS(interval) {
    const jsInterval = YadamuLibrary.parse8601Interval(interval)
    return `${jsInterval.days || 0}:${jsInterval.hours || 0}:${jsInterval.minutes || 0}:${jsInterval.seconds || 0}`
  }	

  generateTransformations(targetDataTypes,stringColumns) {

    // Set up Transformation functions to be applied to the incoming rows
 	  	
   return targetDataTypes.map((targetDataType,idx) => {      
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);
7
	  if ((YadamuLibrary.isBinaryType(dataType.type)) || ((dataType.type ===  'long') && (dataType.typeQualifier === 'varbinary'))) {
		return (col,idx) =>  {
		 return col.toString('hex')
		}
      }
	  
	  switch (dataType.type.toUpperCase()) {
        case "GEOMETRY":
        case "GEOGRAPHY":
        case "POINT":
        case "LSEG":
        case "BOX":
        case "PATH":
        case "POLYGON":
        case "CIRCLE":
        case "LINESTRING":
        case "MULTIPOINT":
        case "MULTILINESTRING":
        case "MULTIPOLYGON":
		case "GEOMCOLLECTION":
		case "GEOMETRYCOLLECTION":
        case '"MDSYS"."SDO_GEOMETRY"':
          if (this.SPATIAL_FORMAT.endsWith('WKB')) {
		    return (col,idx) =>  {
		     return col.toString('hex')
		    }
          }
          if (this.SPATIAL_FORMAT.endsWith('GeoJSON')) {
            return (col,idx)  => {
			  return YadamuSpatialLibrary.geoJSONtoWKT(col)
			}
          }
          /*
          if (this.SPATIAL_FORMAT.endsWith('WKB')) {
            return (col,idx)  => {
			  if (Buffer.isBuffer(col)) {
			    return Buffer.
			  }
        	  return YadamuSpatialLibrary.hexBinaryToWkT(col)
			}
          }
		  */
		  return null;
        case "JSON":
          return (col,idx) =>  {
            if (typeof col === 'string') {
              return JSON.parse(col)
            } 
			if (Buffer.isBuffer(col)) {
			  return JSON.parse(col.toString('utf8'))
			}
  	        return col
          }
        case "BOOLEAN":
          return (col,idx) =>  {
		    const bool = (typeof col === 'string') ? col.toUpperCase() : (Buffer.isBuffer(col)) ? col.toString('hex') : col
			switch(bool) {
              case true:
              case "TRUE":
              case "01":
              case "1":
			  case 1:
                return true;
				break;
              case false:
              case "FALSE":
              case "00":
              case "0":
			  case 0:
                return false;
				break;
              default: 
            }
			return col
          }
        case "DATE":
          return (col,idx) =>  { 
            if (col instanceof Date) {
              return col.toISOString()
            }
			return col
          }
        case "DATETIME":
        case "TIMESTAMP":
		  // Timestamps are truncated to a maximum of 6 digits
          // Timestamps not explicitly marked as UTC are coerced to UTC.
		  // Timestamps using a '+00:00' are converted are converted to 
		  return (col,idx) =>  { 
		    let ts
			switch (true) {
              case (col instanceof Date):
                return col.toISOString()
              case col.endsWith('+00:00'):
			    ts = col.slice(0,-6) 
				return `${ts.slice(0,26)}Z`
              case col.endsWith('Z'):
			    ts = col.slice(0,-1) 
		    	return `${ts.slice(0,26)}Z`
			  default:
			    return `${col.slice(0,26)}Z`
            }
          }
        case "INTERVAL":
	      switch (dataType.typeQualifier.toUpperCase()) {
            case "DAY TO SECOND":
		       return (col,idx) => {
			     return this.toSQLIntervalDMS(col)
		       }
            case "YEAR TO MONTH":
		      return (col,idx) => {
			    return this.toSQLIntervalYM(col)
		      }
			default:
    		  return (col,idx) => {
			    return this.toSQLInterval(col)
		      }
	      }
        case "TIME" :
		  return (col,idx) => {
            if (typeof col === 'string') {
              let components = col.split('T')
              col = components.length === 1 ? components[0] : components[1]
              return col.split('Z')[0]
            }
            else {
              return col.getUTCHours() + ':' + col.getUTCMinutes() + ':' + col.getUTCSeconds() + '.' + col.getUTCMilliseconds();  
            }
		  }
          break;
        case "TIMETZ" :
		  return (col,idx) => {
            if (typeof col === 'string') {
              let components = col.split('T')
              return components.length === 1 ? components[0] : components[1]
            }
            else {
              return col.getUTCHours() + ':' + col.getUTCMinutes() + ':' + col.getUTCSeconds() + '.' + col.getUTCMilliseconds()
            }
		  }
		case 'CHAR':
		case 'VARCHAR':
	      if (!this.dbi.COPY_TRIM_WHITEPSPACE) {
		    // Track Indexes of columns Needed Whitespace preservation
		    stringColumns.push(idx);
	      }
          return null		  
          break;     	
		case 'LONG':
	      if ((dataType.typeQualifier.toUpperCase() === 'VARCHAR') && !this.dbi.COPY_TRIM_WHITEPSPACE) {
		    // Track Indexes of columns Needed Whitespace preservation
		    stringColumns.push(idx);
	      }
          return null		  
          break;  
		default:
	      return null
      }
    }) 

  }
    
  setTransformations(targetDataTypes) {
	  
	const stringColumns = []
	 	  
	this.transformations = this.generateTransformations(targetDataTypes,stringColumns) 
	
	// Use a dummy rowTransformation function if there are no transformations required. 
	// During rowTransformation track columns that contain an empty string
	
    return (this.transformations.every((currentValue) => { return currentValue === null}) && stringColumns.length === 0)
	? (row) => {} 
	: (row) => {
      this.transformations.forEach((transformation,idx) => {
        if (transformation !== null && (row[idx] !== null)) {
          row[idx] = transformation(row[idx],idx)
        }
      })
	  const emptyStringList = []
	  stringColumns.forEach((idx) => {
		if ((row[idx] !== null) && (typeof row[idx] === 'string')) {
	      // COPY seems to cause EMPTY Strings to become NULL
	      if (row[idx].length === 0) {
			emptyStringList.push(idx)
		    // throw new WhitespaceIssue(this.tableInfo.columnNames[idx])
	      }
        }
	  })
      if (emptyStringList.length > 0) {
		throw new EmptyStringDetected(emptyStringList)
	  }
	}  
	
  }
    
  cacheRow(row) {
	 
    // if (this.COPY_METRICS.cached === 1) console.log('verticaWriter',row)
		
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
	
	// Group rows containing Empty Strings according to the indexes of the columns that contain empty strings. There will be one key in the batch for each unique combination of columns
	
	try {
	  this.rowTransformation(row)
	  this.batch.copy.push(row);	
      this.COPY_METRICS.cached++
	  return this.skipTable
	} catch (cause) {
	  if (cause instanceof EmptyStringDetected) {
		const emptyStringKey= cause.emptyStringList.join('-')
		this.batch[emptyStringKey] = this.batch[emptyStringKey] || []
		this.batch[emptyStringKey].push(row)
	    this.COPY_METRICS.cached++
	    return this.skipTable
	  }
	  if (cause instanceof WhitespaceIssue) {
	    this.batch.insert.push(row);
        this.COPY_METRICS.cached++
	    return this.skipTable
	  }
	  throw cause;
	}
  }

}

export { VerticaOutputManager as default }
 "use strict"

import crypto                                from 'crypto';
import { 
  performance 
}          
                                             from 'perf_hooks';
import fsp                                   from 'fs/promises';
import path                                  from 'path'

import Yadamu                                from '../../core/yadamu.js';
import YadamuLibrary                         from '../../lib/yadamuLibrary.js';
import YadamuSpatialLibrary                  from '../../lib/yadamuSpatialLibrary.js';
import YadamuOutputManager                   from '../base/yadamuOutputManager.js';
import StringWriter                          from '../../util/stringWriter.js';
import {
  FileError, 
  FileNotFound, 
  DirectoryNotFound
}                                            from '../file/fileException.js';
import {
  WhitespaceIssue, 
  EmptyStringDetected, 
  ContentTooLarge, 
  StagingAreaMisMatch,
  VerticaError, 
  VertiaCopyOperationFailure
}                                            from './verticaException.js'
import DataTypes                             from './verticaDataTypes.js'

class VerticaOutputManager extends YadamuOutputManager {

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }

  toSQLInterval(interval) {
    const jsInterval = YadamuLibrary.parse8601Interval(interval)
	switch (jsInterval.type) {
	  case 'YM':
  	    return `${jsInterval.years || 0} years ${jsInterval.months || 0} months`
	  case 'DMS':
  	    return `${jsInterval.days || 0} day ${jsInterval.hours || 0} hour ${jsInterval.minutes || 0} mins ${jsInterval.seconds || 0} sec`
	  default:
	    return interval;
	}
  }	
  
  toSQLIntervalYM(interval) {
    const jsInterval = YadamuLibrary.parse8601Interval(interval)
    return `${jsInterval.years || 0} years ${jsInterval.months || 0} months`
  }	
  
  toSQLIntervalDMS(interval) {
    const jsInterval = YadamuLibrary.parse8601Interval(interval)
    return `${jsInterval.days || 0} day ${jsInterval.hours || 0} hour ${jsInterval.minutes || 0} mins ${jsInterval.seconds || 0} sec`
  }	

  generateTransformations(dataTypes,stringColumns) {

    // Set up Transformation functions to be applied to the incoming rows
	
   return dataTypes.map((dataType,idx) => {      

      if (this.tableInfo.jsonColumns[idx]) {
        return (col,idx) =>  {
          if (typeof col === 'string') {
            return JSON.parse(col)
          } 
	      if (Buffer.isBuffer(col)) {
		    return JSON.parse(col.toString('utf8'))
		  }
  	      return col
   	    }
	  }
      
	  const dataTypeDefinition = YadamuLibrary.decomposeDataType(dataType);
	  
	  switch (dataTypeDefinition.type.toLowerCase()) {
		case DataTypes.BINARY_TYPE:
		case DataTypes.VARBINARY_TYPE:
		  return (col,idx) =>  {
		    return col.toString('hex')
		  }
		case DataTypes.CHAR_TYPE:
		case DataTypes.VARCHAR_TYPE:
		case DataTypes.CLOB_TYPE:
	      if (this.dbi.COPY_TRIM_WHITEPSPACE) {
            return null		  
		  }
          // Track Indexes of columns Needed Whitespace preservation
	      stringColumns.push(idx);
		  return null
		case DataTypes.GEOMETRY_TYPE:
        case DataTypes.GEOGRAPHY_TYPE:
          if (this.SPATIAL_FORMAT.endsWith('WKB')) {
		    return (col,idx) =>  {
		     return col.toString('hex')
		    }
          }
          if (this.SPATIAL_FORMAT.endsWith('GeoJSON')) {
            return (col,idx)  => {
		      console.log(col)
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
        case DataTypes.BOOLEAN_TYPE:
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
        case DataTypes.DATE_TYPE:
          return (col,idx) =>  { 
            if (col instanceof Date) {
              return col.toISOString()
            }
			return col
          }
        case DataTypes.TIME_TYPE:
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
        case DataTypes.TIME_TZ_TYPE :
		  return (col,idx) => {
            if (typeof col === 'string') {
              let components = col.split('T')
              return components.length === 1 ? components[0] : components[1]
            }
            else {
              return col.getUTCHours() + ':' + col.getUTCMinutes() + ':' + col.getUTCSeconds() + '.' + col.getUTCMilliseconds()
            }
		  }
        case DataTypes.DATETIME_TYPE:
        case DataTypes.TIMESTAMP_TYPE:
        case DataTypes.TIMESTAMP_TZ_TYPE:
		  // Timestamps are truncated to a maximum of 6 digits
          // Timestamps not explicitly marked as UTC are coerced to UTC.
		  // Timestamps using a '+00:00' are converted are converted to 'Z'
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
        case DataTypes.INTERVAL_TYPE:
  		  return (col,idx) => {
			return this.toSQLInterval(col)
		  }
        case DataTypes.INTERVAL_DAY_TO_SECOND_TYPE:
  		case "interval day to second":
		  return (col,idx) => {
		    return this.toSQLIntervalDMS(col)
		  }
        case DataTypes.INTERVAL_YEAR_TO_MONTH_TYPE:
          return (col,idx) => {
		    return this.toSQLIntervalYM(col)
		  }
		default:
	      return null
      }
    }) 

  }
    
  setTransformations(dataTypes) {
	  
	const stringColumns = []
	 	  
	this.transformations = this.generateTransformations(dataTypes,stringColumns) 
	
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
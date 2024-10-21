
import oracledb             from 'oracledb';

import {
  YadamuError
}                           from '../../core/yadamuException.js'

import YadamuParser         from '../base/yadamuParser.js'
import StringWriter         from '../../util/stringWriter.js';
import BufferWriter         from '../../util/bufferWriter.js';
import YadamuLibrary        from '../../lib/yadamuLibrary.js'
import YadamuSpatialLibrary from '../../lib/yadamuSpatialLibrary.js'

import {OracleError}        from './oracleException.js';

class OracleParser extends YadamuParser {
  
  #lobColumns = false
  
  set LOB_COLUMNS(v) { this.#lobColumns = v }
  get LOB_COLUMNS()  { return this.#lobColumns }
  
  constructor(dbi,queryInfo,pipelineState,yadamuLogger) {
    super(dbi,queryInfo,pipelineState,yadamuLogger)
	dbi.PARSER = this
  }

  fetchLobContent = (row,idx) => {
	try {
      row[idx] = row[idx].getData()
	} catch (e) {
      const cause = this.dbi.createDatabaseError(this.dbi.DRIVER_ID,e,stack,'LOB.getData()')
	  if (cause.lostConnection()) {
 		this.LOGGER.qaWarning([this.dbi.DATABASE_VENDOR,'PARSER',this.queryInfo.TABLE_NAME,'CLOB'],'LOB Content unavailable following Lost Connection')
		row[idx] = null
	  }
	  throw cause
    }
  }

  setColumnMetadata(resultSetMetadata) { 
  
    if (this.PIPELINE_STATE.failed && resultSetMetadata === undefined) {
	  // Oracle appears to raise the metadatata event appears to raised and supply undefined metadata if an error (such as A DDL error) terminates the pipeline
	  return
	}
    
	this.lobTransformations = new Array(resultSetMetadata.length) 

   	this.transformations = resultSetMetadata.map((column,idx) => {
      let stack
	  const dataType = this.queryInfo.DATA_TYPE_ARRAY[idx].toUpperCase()
	  switch (column.fetchType) {
		case oracledb.DB_TYPE_NCLOB:
		case oracledb.DB_TYPE_CLOB:
		  this.LOB_COLUMNS = true
	      this.lobTransformations[idx] = this.fetchLobContent
 		  if (this.dbi.DATA_TYPES.SPATIAL_TYPE && this.queryInfo.convertGeoJSONtoWKB === true) {
			return (row,idx)  => {
			  row[idx] = YadamuSpatialLibrary.geoJSONtoWKB(JSON.parse(row[idx]))
			}
          }
		  return null;
	    case oracledb.DB_TYPE_BLOB:	
		  this.LOB_COLUMNS = true
	      this.lobTransformations[idx] = this.fetchLobContent
	      if (this.queryInfo.jsonColumns.includes(idx)) {
 	        return (row,idx)  => {
   		       row[idx] = row[idx].toString('utf8')
			}
          }
 	      return null
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
	
	this.processLobs = !this.LOB_COLUMNS ? YadamuLibrary.NOOP : async (data) => {
	  this.lobTransformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (data[idx] !== null)) {
          transformation(data,idx)
        }
	  })
	  const row = (await Promise.allSettled(data)).map((p) => {
		return p.value ? p.value : null
	  })
	  const results = await Promise.allSettled(data)
	  results.forEach((p,idx) => {
		data[idx] = p.value
	  })
    }  
  }
  
  async doTransform(data) {  
    try {
      // if (this.PIPELINE_STATE.parsed == 1) console.log(data)
      await this.processLobs(data)
      super.doTransform(data)
	  return data
	} catch(err) {
	  console.log(err)
	  const cause = this.dbi.createDatabaseError(this.dbi.DRIVER_ID,err,new Error().stack,this.constructor.name)
	  // cause.ignoreUnhandledRejection = true
	  throw cause.lostConnection() ? cause : err
	}
  }
  
}

export {OracleParser as default }

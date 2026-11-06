
import oracledb             from 'oracledb';

import {
  YadamuError
}                           from '../../core/yadamuException.js'

import YadamuParser         from '../base/yadamuParser.js'
import StringWriter         from '../../util/stringWriter.js';
import BufferWriter         from '../../util/bufferWriter.js';
import YadamulLibrary       from '../../lib/yadamuLibrary.js'
import YadamuSpatialLibrary from '../../lib/yadamuSpatialLibrary.js'

import {OracleError}        from './oracleException.js';

class OracleParser extends YadamuParser {

  constructor(dbi,queryInfo,pipelineState,yadamuLogger) {
    super(dbi,queryInfo,pipelineState,yadamuLogger)
        dbi.ACTIVE_PARSER = this
  }

  freeLob(lob) {
    // Free each temporary LOB as soon as its content has been read instead of leaving it to the garbage collector.
    // When frees reach the server in GC-timed bursts during a fetch, Oracle 19.3 can crash the session with
    // ORA-07445 [_intel_fast_memcmp()+28] (temporary LOB refcount use-after-free; seen on get_WKB() spatial exports).
    // Not called once the connection is lost: the session's temporary LOBs are gone with it, and a close on a half-open
    // connection could hold a libuv thread (needed by the reconnect) until TCP times out.
    // A failed close is reported as a stream 'error'; ignore it rather than crash.
    lob.once('error',() => {})
    lob.destroy()
  }

  fetchLobContent = (row,idx)  => {
    const stack = new Error().stack
    row[idx] = new Promise((resolve,reject) => {
      const lob = row[idx]
      lob.getData().then((data) => { 
        this.freeLob(lob)
  	    row[idx] = data
	    resolve()
      }).catch((e) => {
        const cause = this.dbi.createDatabaseError(e,stack,'LOB.getData()')
        if (cause.lostConnection()) {
          this.LOGGER.qaWarning([this.dbi.DATABASE_VENDOR,'PARSER',this.queryInfo.TABLE_NAME,'CLOB'],'LOB Content unavailable following Lost Connection')
          row[idx] = null
        }
        else {
          this.freeLob(lob)
        }
        reject(cause)
	  })
	})
  }
  
  fetchJSONfromBLOB = (row,idx)  => {
    const stack = new Error().stack
    row[idx] = new Promise((resolve,reject) => {
      const lob = row[idx]
      lob.getData().then((data) => { 
        this.freeLob(lob)
  	    row[idx] = data.toString('utf-8')
	    resolve()
      }).catch((e) => {
        const cause = this.dbi.createDatabaseError(e,stack,'LOB.getData()')
        if (cause.lostConnection()) {
          this.LOGGER.qaWarning([this.dbi.DATABASE_VENDOR,'PARSER',this.queryInfo.TABLE_NAME,'CLOB'],'LOB Content unavailable following Lost Connection')
          row[idx] = null
        }
        else {
          this.freeLob(lob)
        }
        reject(cause)
	  })
	})
  }

  fetchGeoJSONContent = (row,idx)  => {
    const stack = new Error().stack
    row[idx] = new Promise((resolve,reject) => {
      const lob = row[idx]
      lob.getData().then((data) => { 
        this.freeLob(lob)
	    row[idx] = YadamuSpatialLibrary.geoJSONtoWKB(JSON.parse(data))
	    resolve()
      }).catch((e) => {
        const cause = this.dbi.createDatabaseError(e,stack,'LOB.getData()')
        if (cause.lostConnection()) {
          this.LOGGER.qaWarning([this.dbi.DATABASE_VENDOR,'PARSER',this.queryInfo.TABLE_NAME,'CLOB'],'LOB Content unavailable following Lost Connection')
          row[idx] = null
        }
        else {
          this.freeLob(lob)
        }
        reject(cause)
	  })
	})
  }

  async fetchLobColumns(data) {
    await Promise.allSettled(data)
  }

  setColumnMetadata(resultSetMetadata) {

    if (this.PIPELINE_STATE.failed && resultSetMetadata === undefined) {
      // Oracle appears to raise the metadatata event appears to raised and supply undefined metadata if an error (such as A DDL error) terminates the pipeline
      return
    }

    this.processLobContent = YadamulLibrary.NOOP

    this.transformations = resultSetMetadata.map((column,idx) => {
       const dataType = this.queryInfo.DATA_TYPE_ARRAY[idx].toUpperCase()
       switch (column.fetchType) {
         case oracledb.DB_TYPE_NCLOB:
         case oracledb.DB_TYPE_CLOB:
		    this.processLobContent = this.fetchLobColumns
            return (this.dbi.DATA_TYPES.SPATIAL_TYPE && this.queryInfo.convertGeoJSONtoWKB === true) ? this.fetchGeoJSONContent : this.fetchLobContent
         case oracledb.DB_TYPE_BLOB:
		    this.processLobContent = this.fetchLobColumns
            return (this.queryInfo.jsonColumns.includes(idx)) ? this.fetchJSONfromBLOB : this.fetchLobContent
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
  }
  
  
  async doTransform(data) {
    try {
      // if (this.PIPELINE_STATE.parsed == 1) console.log(data)
      await super.doTransform(data)
      // if (this.PIPELINE_STATE.parsed == 1) console.log(data)
      await this.processLobContent(data)
      // if (this.PIPELINE_STATE.parsed == 1) console.log(data)
      return data
    } catch(err) {
      console.log(err)
      const cause = this.dbi.createDatabaseError(err,new Error().stack,this.constructor.name)
      // cause.ignoreUnhandledRejection = true
      throw cause.lostConnection() ? cause : err
    }
  }

}

export {OracleParser as default }

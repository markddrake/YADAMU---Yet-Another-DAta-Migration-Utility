
import { 
  performance 
}                               from 'perf_hooks';
						

import oracledb from 'oracledb';
oracledb.fetchAsString = [ oracledb.DATE, oracledb.NUMBER ]

import YadamuLibrary            from '../../lib/yadamuLibrary.js'
import YadamuSpatialLibrary     from '../../lib/yadamuSpatialLibrary.js'

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuOutputManager      from '../base/yadamuOutputManager.js'

class OracleOutputManager extends YadamuOutputManager {

  /*
  **
  ** Optimization of LOB Usage
  **
  ** Since using LOB causes a 50%+  reduction in throughput only use LOBS where necessary.
  ** The oracle node driver (oracledb) allows strings and buffers to be bound to CLOBS and BLOBS
  ** This requires the LOB to be buffered in the client until it is written to the database
  ** You cannot insert a mixture of rows contains LOBs and rows containing Strings and Buffers using executeMany as the bind specification must explicitly state what is being bound.
  **
  ** Binding LOBS is slower than binding Strings and Buffers
  ** Binding LOBS requires less client side memory than binding Strings and Buffers
  **
  ** The Yadamu Oracle interface allows you to optimize LOB usage via the following parameters
  **    TEMPLOB_BATCH_LIMIT  : A Batch will be regarded as complete when it uses more LOBS than TEMPLOB_BATCH_LIMIT
  **    CACHELOB_MAX_SIZE    : If a String or Buffer is mapped to a CLOB or a BLOB then it will be inserted using a LOB if it exceeeds this value.
  **    CACHELOB_BATCH_LIMIT : A Batch will be regarded as complete when the number of CACHED (String & Buffer) LOBs exceeds this value.
  **
  ** The amount of client side memory required to manage the LOB Cache is approx CACHELOB_MAX_SIZE * CACHELOB_BATCH_LIMIT
  **
  */

  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
    this.tempLobCount = 0;
    this.cachedLobCount = 0;
    this.lobList = []
    this.lobCumlativeTime = 0;
    this.includeTestcase = this.dbi.parameters.EXPORT_TESTCASE === true
  }

  createBatch() {
    return   {
      rows           : []
    , lobRows        : []
    , tempLobCount   : 0
    , cachedLobCount : 0
    , ts             : performance.now()
    }
  }

  resetBatch(batch) {
    batch.rows.length    = 0;
    batch.lobRows.length = 0;
    batch.tempLobCount   = 0
    batch.cachedLobCount = 0
  }

  generateTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
	
    let spatialFormat = this.SPATIAL_FORMAT	
    return targetDataTypes.map((targetDataType,idx) => {

      const dataType = YadamuDataTypes.decomposeDataType(targetDataType);
	  switch (dataType.type.toUpperCase()) {
        case this.dbi.DATA_TYPES.SPATIAL_TYPE:
          // Metadata based decision
		  switch (true) {
            case ((this.dbi.DATABASE_VERSION < 12) && (this.SPATIAL_FORMAT === 'GeoJSON')):
              // SDO_UTIL does not support GeoJSON in 11.x database
              spatialFormat = "WKT"
			  return (col,idx) =>  {
				return YadamuSpatialLibrary.geoJSONtoWKT(col)
              }
            case ((this.dbi.DATABASE_VERSION > 19) && (this.SPATIAL_FORMAT === 'EWKB')):
              spatialFormat = "WKB"
			  return (col,idx) =>  {
				return YadamuSpatialLibrary.ewkbToWKB(col)
              }
			default:
              return null
          }
        case this.dbi.DATA_TYPES.ORACLE_BFILE_TYPE:
          return (col,idx) =>  {
            return typeof col === 'object' ? JSON.stringify(col) : col
          }
        case this.dbi.DATA_TYPES.SET_TYPE:
        case this.dbi.DATA_TYPES.JSON_TYPE:
          // Convert JSON representation to String.
		  switch (this.dbi.DATA_TYPES.storageOptions.JSON_TYPE) {
			case this.dbi.DATA_TYPES.VARCHAR_TYPE:
		    case this.dbi.DATA_TYPES.CLOB_TYPE:
		    case this.dbi.DATA_TYPES.BLOB_TYPE:
              return (col,idx) =>  {
                // JSON must be shipped in Serialized Form
                return typeof col === 'object' ? JSON.stringify(col) : col
              }
		    case this.dbi.DATA_TYPES.BLOB_TYPE:
			  if (this.dbi.DATABASE_VERSION < 19) {
                return (col,idx) =>  {
                  // JSON must be shipped in Serialized Form
                  return typeof col === 'object' ? JSON.stringify(col) : col
                }
			  }
			  return (col,idx) => {
			    return Buffer.isBuffer(col) ? col : Buffer.from(JSON.stringify(col))
			 }
		    default:
			  return null
		  }
          break;
        case this.dbi.DATA_TYPES.BINARY_TYPE:
          return (col,idx) =>  {
            return typeof col === 'boolean' ? new Buffer.from(col === true ? [1] : [0]) :  col
          } 
          break;
        case this.dbi.DATA_TYPES.BOOLEAN_TYPE:
	      switch (this.dbi.DATA_TYPES.storageOptions.BOOLEAN_TYPE) {
		    case 'RAW(1)':
		    default:
              return (col,idx) =>  {
                return YadamuLibrary.booleanToBuffer(col)
              } 
	      }
		  break;
        case this.dbi.DATA_TYPES.DATE_TYPE:
        case this.dbi.DATA_TYPES.TIME_TYPE:
        case this.dbi.DATA_TYPES.DATETIME_TYPE:
          // We want 'YYYY-MM-DD"T"HH24:MI:SS'
          // Assume we get a valid 8601 Value
          // Assume value longer than 19 contains unwanted fractional and or time zone info
          // Assume a value with length 10 is a DATE only value that needs to have 'all:balls' time added
          // Assume a value with length 8 is a TIME only value that needs to be baselined to the Unix epoch date.
          // ### Negative values will screw this up. Consider using regex to try to reduce chances of invalid dates...
          // ### Use Data Peeking to update transformation based on first value encountered
          return (col,idx) =>  {
            if (col instanceof Date) {
              col = col.toISOString()
            }
			// col = col.endsWith('Z') ? col.slice(0,-1) : col
			// col = col.endsWith('00:00') ? col.slice(0,-6) : col
            col = col.length   > 19 ? col.substring(0,19) : col
            col = col.length === 10 ? `${col}T00:00:00` : col
            col = col.length ===  8 ? `1970-01-01T${col}` : col
            return col
          }
          break;
        case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMP_LTZ_TYPE:
          return (col,idx) =>  {
            // A Timestamp not explicitly marked as UTC should be coerced to UTC.
            // Avoid Javascript dates due to lost of precsion.
            // row[bindIdx] = new Date(Date.parse(col.endsWith('Z') ? col : col + 'Z'));
            if (typeof col === 'string') {
		      const val = col.indexOf('T') < 0 ? `${col.substring(0,10)}T00:00:00.000000${col.substring(10)}` : col 
              return (col.endsWith('Z') || col.endsWith('+00:00')) ? val : `${val}Z`;
            }
            if (col instanceof Date) {
              return col.toISOString()
            }
            return col
          }
          break;
        case this.dbi.DATA_TYPES.XML_TYPE :
          /*
          // Cannot passs XMLTYPE as BUFFER: ORA-06553: PLS-307: too many declarations of 'XMLTYPE' match this call
          return (col,idx) =>  {
            // bindRow[idx] = Buffer.from(col);
            return col
          }
          */
          return null
          break;
        default :
          return null
      }
    })

	this.tableInfo._SPATIAL_FORMAT = spatialFormat

  }

  generateLobTransformations() {

    this.lobTransformations = new Array(this.tableInfo.binds.length).fill(null);

    /*
    ** NOTE:  ### this.cachedLobCount tracks the number of LOB column values that have been cached on the client as Strings or Buffers,
    ** rather that cached on the server as temporary LOBs. bindRowAsLOB is set true if any LOB column value in the row exceeds the
    ** maximum size defined for a client cached object, and forces all LOB values in the row to be converted to temporary LOBs
    ** and cached on the server
    **
    */

    if (this.tableInfo.lobColumns === true) {
      this.lobTransformations = this.tableInfo.lobBinds.map((lobBind,idx) => {
        switch (lobBind.type) {
          case oracledb.CLOB:
            return (col,idx) =>  {
              // Determine whether to bind content as string or temporary CLOB
              if (typeof col !== "string") {
                col = JSON.stringify(col);
              }
              this.batch.cachedLobCount++
              this.bindRowAsLOB = this.bindRowAsLOB || (Buffer.byteLength(col,'utf8') > this.dbi.CACHELOB_MAX_SIZE) || Buffer.byteLength(col,'utf8') === 0
              return col;
            }
            break;
          case oracledb.BLOB:
            return (col,idx) =>  {
              /*
			  **
			  ** If necessary convert data to a Buffer. The following actions are preformed based on the type of data
              **
              ** 1. A Buffer: No Action
              ** 2. A JSON Object : Stringify Object and convert to a Buffer
              ** 3. A String containing a Serialized JSON object: Conver to a Buffer
              ** 4. A string containing HexBinary encoded content: Convert to a Buffer
              **
              */
			  if (!Buffer.isBuffer(col)) {
				switch (true) {
				  case (typeof col === 'object'):
				    col = Buffer.from(JSON.stringify(col),'utf-8')
				    break
				  case ((typeof col === "string") && (YadamuDataTypes.decomposeDataType(this.tableInfo.targetDataTypes[idx]).type === 'JSON')):
                    col = Buffer.from(col,'utf-8')
					break
                  case (typeof col === "string"):
                    col = Buffer.from(col,'hex')
					break
				}
			  }
              this.batch.cachedLobCount++
              this.bindRowAsLOB = this.bindRowAsLOB || (col.length > this.dbi.CACHELOB_MAX_SIZE)
              return col
              
            }
            break
          default:
            return null;
        }
      })
    }
  }

  async setTableInfo(tableName) {

    await super.setTableInfo(tableName)
    this.generateLobTransformations()

  }

  trackStringToClob(s) {
    const clob = this.dbi.stringToClob(s).catch((err) => {
      // Suppress Unhandled Rejections that can arise if the pipeline aborts while a LOB operation is in progress.
      if (this.destroyed) return;
      throw err
    })
    this.lobList.push(clob);
    return clob
  }

  trackBufferToBlob(b) {
    const blob = this.dbi.blobFromBuffer(b).catch((err) => {
      // Suppress Unhandled Rejections that can arise if the pipeline aborts while a LOB operation is in progress.
      if (this.destroyed) return;
      throw err
    })
    this.lobList.push(blob);
    return blob;
  }

  checkNumericBinds(row) {

    // Check for cases where NUMBERS are returned as Strings, and update the Bind Mapping for the column in question

    // Check performed on re-ordered rows. Once all numeric columns have been checked disable check

    // Loop backwards to ensure the splice operation has no effect on the idx of the remaining items

    for (let i=this.tableInfo.numericBindPositions.length-1; i >= 0; i--) {
      const bindIdx = this.tableInfo.numericBindPositions[i]
      if (row[bindIdx] !== null) {
        if (typeof row[bindIdx] === 'string') {
          this.tableInfo.binds[bindIdx] = {type: oracledb.STRING, maxSize : 128}
          if (this.tableInfo.lobBinds) {
            this.tableInfo.lobBinds[bindIdx] = {type: oracledb.STRING, maxSize : 128}
          }
        }
        this.tableInfo.numericBindPositions.splice(i,1)
      }
    }
    if (this.tableInfo.numericBindPositions.length === 0) {
      this.checkNumericBinds = (row) => {}
    }
  }
  
  cacheRow(row) {

    /*
    **
    ** Be careful modifying code inside the map operator, it is executed for every column in every row.
    **
    */

    /*
    **
    ** Perform required column transformations
    **
    **   GEOGRAPHY: Oracle 11g: Spatial: Convert GeoJSON to WKT
    **   JSON     : Stringify Objects
    **   CLOB     : Stringify Objects
    **   BLOB     : Convert small HexBinary Strings to Buffers
    **   XMLTYPE  :
    **
    ** After transformations are complete check if any content bind to a CLOB, BLOB or XMLTYPE exceeds the maximum size defined for String and Buffer
    **
    ** If any column mapped to a CLOB, BLOB or XMLTYPE exceeds the maximum size defined for String or Buffer convert all columns bound to CLOB and BLOB into temporary LOBS
    **
    ** When executing a PL/SQL anonymous block that requires CLOB or BLOB binds the CLOB or BLOB binds come at the end of the argument list.
    ** The TableInfo arrays were generated taking this into account
    ** tableInfo.bindOrdering contains the indexes for the columns in row in bind ordering
    ** If tableInfo.lobColumns = true then the contents of row need to be re-ordering a based on tableInfo.bindOrdering
    ** If talbeInfo.lobColuns = false then there are no lobs so there is no need to re-order the row.
    */

    // this.LOGGER.trace([this.constructor.name,this.tableInfo.lobColumns,this.COPY_METRICS.cached],'cacheRow()')
	
    // if (this.COPY_METRICS.received === 1) {console.log('Cache Row (1)\n',row)}

    try {
      this.bindRowAsLOB = false;
      if (this.tableInfo.lobColumns) {
        // Bind Ordering and Row Ordering may be different. LOBS need to follow scalar columns, regardless of the order of the columns in he table. Use map to create a new array in BindOrdering when applying transformations
        row = this.transformations.map((transformation,bindIdx) => {
          const rowIdx = this.tableInfo.bindOrdering[bindIdx]
          if (row[rowIdx] !== null) {
			// Apply the column transformation
            if (transformation !== null) {
              row[rowIdx] = transformation(row[rowIdx],rowIdx);
            }
			// Convert CLOB and BLOB values
            if (this.lobTransformations[bindIdx] !== null) {
              row[rowIdx] = this.lobTransformations[bindIdx](row[rowIdx],rowIdx);
            }
          }
          return row[rowIdx]
        })
      }
      else {
        // Bind Ordering and Row Ordering are the same. Apply transformations directly to ROW where required
        this.rowTransformation(row)
      }
      // Row is now in bindOrdering. Convert CLOB and BLOB to temporaryLOBs where necessary
      if (this.bindRowAsLOB) {
        // Use map combined with Promise.All to convert columns to temporaryLobs
        const lobStartTime = performance.now();
        row = this.tableInfo.lobBinds.map((bind,idx) => {
          if (row[idx] !== null) {
            switch (bind.type) {
              case oracledb.CLOB:
                this.batch.tempLobCount++
                this.batch.cachedLobCount--
                return this.trackStringToClob(row[idx])
                break;
              case oracledb.BLOB:
                this.batch.tempLobCount++
                this.batch.cachedLobCount--
                if (typeof row[idx] === 'string') {
                  return this.trackBufferToBlob(Buffer.from(row[idx],'hex'))
                }
                else {
                  return this.trackBufferToBlob(row[idx])
                }
                break;
              default:
                return row[idx]
            }
          }
          return null;
        })
        this.lobCumlativeTime = this.lobCumlativeTime + (performance.now() - lobStartTime);
        this.batch.lobRows.push(row);
      }
      else {
        this.batch.rows.push(row);
      }
	  this.checkNumericBinds(row)
      this.COPY_METRICS.cached++
      // if (this.COPY_METRICS.received === 1) {console.log('Cache Row (2)\n',row)}
    } catch (cause) {
      this.handleIterativeError('CACHE ONE',cause,this.COPY_METRICS.cached+1,row);
    }

    return this.skipTable

  }

  flushBatch() {
    return ((this.COPY_METRICS.cached === this.BATCH_SIZE) || (this.batch.tempLobCount >= this.dbi.TEMPLOB_BATCH_LIMIT) || (this.batch.cachedLobCount > this.dbi.CACHELOB_BATCH_LIMIT))
  }

  async handleIterativeError(operation,cause,rowNumber,record) {

     // If cause is generated by the SQL layer it alreadys contain SQL and bind information.

    const newRecord = await this.serializeLobColumns(record)
     if (Array.isArray(cause.args)) {
       cause.args = await this.dbi.serializeLobBinds(cause.args)
     }
     await super.handleIterativeError(operation,cause,rowNumber,newRecord)
  }

  async serializeLobColumns(row) {

    return await this.dbi.serializeLobColumns(this.tableInfo,row)

  }

}

export {OracleOutputManager as default }
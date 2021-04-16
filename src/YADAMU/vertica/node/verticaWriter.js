"use strict"

const crypto = require('crypto');
const { performance } = require('perf_hooks');
const { Readable, pipeline} = require('stream')
const fsp = require('fs').promises
const path = require('path')
const Yadamu = require('../../common/yadamu.js');

const YadamuLibrary = require('../../common/yadamuLibrary.js');
const YadamuSpatialLibrary = require('../../common/yadamuSpatialLibrary.js');
const YadamuWriter = require('../../common/yadamuWriter.js');
const StringWriter = require('../../common/stringWriter.js');
const {FileError, FileNotFound, DirectoryNotFound} = require('../../file/node/fileException.js');
const {LeadingTrailingSpaces, ContentTooLarge} = require('./verticaException.js')

class VerticaWriter extends YadamuWriter {

  get STAGING_FILE()               { this._STAGING_FILE         =  this._STAGING_FILE || `YST-${crypto.randomBytes(16).toString("hex").toUpperCase()}`; return this._STAGING_FILE }
  get YADAMU_STAGING_FILE()        { this._YADAMU_STAGING_FILE  = this._YADAMU_STAGING_FILE || path.resolve(path.join(this.dbi.YADAMU_STAGING_FOLDER,this.STAGING_FILE)); return this._YADAMU_STAGING_FILE }
  get VERTICA_STAGING_FILE()       { this._VERTICA_STAGING_FILE = this._VERTICA_STAGING_FILE || path.join(this.dbi.VERTICA_STAGING_FOLDER,this.STAGING_FILE).split(path.sep).join(path.posix.sep); return this._VERTICA_STAGING_FILE }
  
  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
    this.insertMode = 'Copy'
	this.insertBatch = []
  }
 
  setTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
 	  
    const transformations = targetDataTypes.map((targetDataType,idx) => {      
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);

	  if ((YadamuLibrary.isBinaryType(dataType.type)) || ((dataType.type ===  'long') && (dataType.typeQualifier === 'varbinary'))) {
		return (col,idx) =>  {
		 return col.toString('hex')
		}
      }
	  
	  switch (dataType.type.toUpperCase()) {
        case "GEOMETRY":
        case "GEOGRAPHY":
        case "POINT"
		:
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
            return (col,idx)  => {
			  if (Buffer.isBuffer(col)) {
			    return YadamuSpatialLibrary.bufferToWkT(col)
			  }
        	  return YadamuSpatialLibrary.hexBinaryToWkT(col)
			}
          }
          if (this.SPATIAL_FORMAT.endsWith('GeoJSON')) {
            return (col,idx)  => {
			  return YadamuSpatialLibrary.geoJSONtoWKT(col)
			}
          }
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
		  return (col,idx) =>  { 
            // A Timestamp not explicitly marked as UTC is coerced to UTC.
			switch (true) {
              case (typeof col === 'string'):
                if (col.endsWith('Z') && col.length === 28) {
                  return col.slice(0,-2) + 'Z'
                }
                else {
                  if (col.endsWith('+00:00')) {
			        if (col.length > 32) {
					  return col.slice(0,26) + 'Z'
				    }
				  }
				  else {
                    if (col.length === 27) {                                
                      return col.slice(0,-1) + "Z"
                    }
                  }
                }               
                return col
              case (col instanceof Date):
                return col.toISOString()
            }
			return col
          }
        case "INTERVAL":
        case "INTERVAL DAY TO SECONDS":
        case "INTERVAL YEAR TO MONTHS":
		  return (col,idx) => {
			return this.toSQLInterval(col)
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
          break;     	
	    default:
	      return null
      }
    }) 

    // Use a dummy rowTransformation function if there are no transformations required.

    return transformations.every((currentValue) => { currentValue === null}) 
	? (row) => {} 
	: (row) => {
	  let issue = undefined
      transformations.forEach((transformation,idx) => {
        if (transformation !== null && (row[idx] !== null)) {
          row[idx] = transformation(row[idx],idx)
        }
		// Cannot use copy with Empty strings or strings that start or end with whitespace 
		// Copy strips leading and trailing spaces. 
		// Copy strips leadnng and trailing newlines
		// The Empty String "" becomes NULL 
		if (typeof row[idx] === 'string') {
		  if (((row[idx].length === 0) || row[idx].startsWith(' ')) || row[idx].startsWith('\n') || row[idx].endsWith('\n') || ((targetDataTypes[idx].indexOf('varchar') > -1) && (row[idx].endsWith(' ')))) {  		
		    if (this.maxLengths[idx] && (row[idx].length > this.maxLengths[idx])) {
			  issue = new ContentTooLarge(this.tableInfo.columnNames[idx],row[idx].length,this.maxLengths[idx])
		    }
		    issue = new LeadingTrailingSpaces()
		  }
		}
      })
      if (issue) throw issue	  
    }  
  }
  
  setTableInfo(tableName) {
	super.setTableInfo(tableName)	
	this.tableInfo.copy = `${this.tableInfo.copy} from '${this.VERTICA_STAGING_FILE}' PARSER fcsvparser(type='rfc4180', header=false) NULL ''`

    this.maxLengths = this.tableInfo.sizeConstraints.map((sizeConstraint) => {
	  const maxLength = parseInt(sizeConstraint) 
	  return maxLength > 0 ? maxLength : undefined
	})
	 	
	this.rowTransformation  = this.setTransformations(this.tableInfo.targetDataTypes)

  }
  
  cacheRow(row) {
	 
	  
    // if (this.metrics.cached === 1) console.log('verticaWriter',row)
		
	// Use forEach not Map as transformations are not required for most columns. 
	// Avoid uneccesary data copy at all cost as this code is executed for every column in every row.
	
	try {
	  this.rowTransformation(row)
	  this.batch.push(row);	
      this.metrics.cached++
	  return this.skipTable
	} catch (cause) {
	  if (cause instanceof LeadingTrailingSpaces) {
	    this.insertBatch.push(row);
        this.metrics.cached++
	    return this.skipTable
	  }
	  if (cause instanceof ContentTooLarge) {
	    this.handleIterativeError('CACHE ONE',cause,this.metrics.cached+1,row);
		return this.skipTable
	  }
	  throw cause;
	}
  }
    
  reportBatchError(batch,operation,cause) {
    // Use Slice to add first and last row, rather than first and last value.
	super.reportBatchError(operation,cause,batch[0],batch[batch.length-1])
  }
  
  recodeSpatialColumns(batch,msg) {
	const targetFormat = 'WKT'
    this.yadamuLogger.info([this.dbi.DATABASE_VENDOR,this.tableName,`COPY`,this.metrics.cached,this.SPATIAL_FORMAT],`${msg} Converting to "${targetFormat}".`);
    YadamuSpatialLibrary.recodeSpatialColumns(this.SPATIAL_FORMAT,targetFormat,this.tableInfo.targetDataTypes,batch,!this.tableInfo.parserRequired)
  }  

  async writeBatchAsCSV(filename,batch) {
	const sw = new StringWriter();
	this.setCSVTransformations(batch)
	super.writeBatchAsCSV(sw,batch)
	await fsp.writeFile(filename,sw.toString())	
  }

  async reportCopyErrors(results,batch,stack) {
	  
	 const causes = []
	 const failed = []
	 let sizeIssue = 0;
	 results.forEach((r) => {
	   const err = new Error()
	   err.stack =  `${stack.slice(0,5)}: ${r[1]}${stack.slice(5)}`
	   err.recordNumber = r[0]
	   const columnNameOffset = r[1].indexOf('column: [') + 9
	   err.columnName = r[1].substring(columnNameOffset,r[1].indexOf(']',columnNameOffset+1))
	   err.columnIdx = this.tableInfo.columnNames.indexOf(err.columnName)
	   err.columnLength = this.maxLengths[err.columnIdx]
	   err.dataLength = parseInt(r[2])
	   err.tags = []
	   if (err.dataLength > err.columnLength) {
		 err.tags.push("CONTENT_TOO_LARGE")
		 sizeIssue++
	   }
  	   causes.push(err)
	   failed.push(batch[r[0]-1])
	 })
     const err = new Error(`Vertica COPY Failure: ${results.length} records rejected.`);
	 err.tags = []
	 if (causes.length === sizeIssue) {
	    err.tags.push("CONTENT_TOO_LARGE")
	 } 
     err.cause = causes;	 
	 err.sql = this.tableInfo.copy;
	 this.dbi.yadamu.REJECTION_MANAGER.rejectRows(this.tableName,failed)
	 this.yadamuLogger.handleException([...err.tags,this.dbi.DATABASE_VENDOR,this.tableInfo.tableName],err)
  }
  
  stagingFileCleanup(stagingFile,loadSuccessful) {
	switch(true) {
	  case(this.dbi.STAGING_FILE_RETENTION === 'NONE'):
	  case((this.dbi.STAGING_FILE_RETENTION === 'ERROR') && loadSuccessful):
	    // Delete the Staging File. Do not wait for success or failure    
	    fsp.rm(statingFile).then().catch();
      default:
	    // Leave the Staging File 
	}
  }
  
  addArgument(arg) {
	 switch (typeof arg) {
		case 'string':
		  // return `'${arg.replace(/'/g,"''").substring(0,50)}'`
		  return `'${arg.replace(/'/g,"''")}'`
		case 'object':
		  return arg === null ? 'null' : `'${JSON.stringify(arg).replace(/'/g,"''")}'`
		default:
		  return arg
	 }
  }
  
  addOperator(arg,idx) {
	 return `${this.tableInfo.insertOperators[idx].prefix}${this.addArgument(arg)}${this.tableInfo.insertOperators[idx].suffix}`
  }
  
  async _writeBatch(batch,rowCount) {
    this.metrics.batchCount++;

    if ((this.tableInfo.insertMode === 'Copy') && (batch.length > 0))  {
      try {
        await this.dbi.createSavePoint();
	    await this.writeBatchAsCSV(this.YADAMU_STAGING_FILE,batch)
		const stack = new Error().stack
        const rejectedRecordsTableName = `YRT-${crypto.randomBytes(16).toString("hex").toUpperCase()}`;
        const sqlStatement = `${this.tableInfo.copy} REJECTED DATA AS TABLE "${rejectedRecordsTableName}"  NO COMMIT`; 	
        const results = await this.dbi.insertBatch(sqlStatement,rejectedRecordsTableName);
		if (results.length > 0) {
	      await this.reportCopyErrors(results,batch,stack)
		}
		this.endTime = performance.now();
        await this.dbi.releaseSavePoint();
		// this.metrics.written += rowCount;
		this.stagingFileCleanup(this.YADAMU_STAGING_FILE,true);
		const written = batch.length - results.length;
		this.metrics.written += written
		this.metrics.skipped += results.length
      } catch (cause) {
        this.endTime = performance.now();1
		await this.reportBatchError(batch,`COPY`,cause)
        await this.dbi.restoreSavePoint(cause);
		this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.insertMode],`Switching to Iterative mode.`);    
        this.insertBatch.push(...batch)		
        this.tableInfo.insertMode = 'Iterative' 
      }
	  
      this.releaseBatch(batch)
      this.stagingFileCleanup(this.YADAMU_STAGING_FILE,true);

    } 
	
    if (this.insertBatch.length > 0) {
      
	  for (const row in this.insertBatch) {
        try {
	      this.dbi.createSavePoint();
		  const sqlStatement = `${this.tableInfo.dml} (${this.insertBatch[row].map((col,idx) => {return this.tableInfo.insertOperators[idx] === null ? this.addArgument(col): this.addOperator(col,idx)}).join(",")})`
		  const results = await this.dbi.executeSQL(sqlStatement);
          this.dbi.releaseSavePoint();
	   	  this.metrics.written++;
        } catch(cause) {
	  	  this.dbi.restoreSavePoint(cause);
          this.handleIterativeError(`INSERT ONE`,cause,row,this.insertBatch[row]);
          if (this.skipTable) {
            break
		  }
        }
      }
     this.endTime = performance.now();
     this.insertBatch = []
    }
    return this.skipTable   
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
}

module.exports = VerticaWriter;
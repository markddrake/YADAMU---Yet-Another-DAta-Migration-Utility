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
const {WhitespaceIssue, EmptyStringDetected, ContentTooLarge} = require('./verticaException.js')

class VerticaWriter extends YadamuWriter {

  get STAGING_FILE()       { return this.tableInfo.localPath }

  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
  }

  newBatch() {
	super.newBatch()
	this.batch = {
	  copy          : []
	, insert        : []
    }
  }  
    
  releaseBatch(batch) {
	if (Array.isArray(batch.copy)) {
	  batch.copy.length = 0;
	  batch.insert.length = 0;
	}
  }
   
  setTransformations(targetDataTypes) {

    // Set up Transformation functions to be applied to the incoming rows
	
	const stringColumns = []
	
    const transformations = targetDataTypes.map((targetDataType,idx) => {      
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

	// Use a dummy rowTransformation function if there are no transformations required.
	
    return (transformations.every((currentValue) => { currentValue === null}) && stringColumns.length === 0)
	? (row) => {} 
	: (row) => {
      transformations.forEach((transformation,idx) => {
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
  
  setTableInfo(tableName) {
    this.newBatch();
	super.setTableInfo(tableName)	
    this.tableInfo.insertMode = 'Copy'
	this.mergeoutInsertCount = this.dbi.MERGEOUT_INSERT_COUNT;

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
	  this.batch.copy.push(row);	
      this.metrics.cached++
	  return this.skipTable
	} catch (cause) {
	  if (cause instanceof EmptyStringDetected) {
		const emptyStringKey= cause.emptyStringList.join('-')
		this.batch[emptyStringKey] = this.batch[emptyStringKey] || []
		this.batch[emptyStringKey].push(row)
	    this.metrics.cached++
	    return this.skipTable
	  }
	  if (cause instanceof WhitespaceIssue) {
	    this.batch.insert.push(row);
        this.metrics.cached++
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

  async reportCopyErrors(results,batch,stack,statement) {
	  
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
	 err.sql = statement;
	 this.dbi.yadamu.REJECTION_MANAGER.rejectRows(this.tableName,failed)
	 this.yadamuLogger.handleException([...err.tags,this.dbi.DATABASE_VENDOR,this.tableInfo.tableName],err)
  }
  
  cleanupStagingFile(stagingFile,loadSuccessful) {
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
		  return `'${arg.replace(/'/g,"''")}'`
		case 'object':
		  return arg === null ? 'null' : `'${JSON.stringify(arg).replace(/'/g,"''")}'`
		case 'number':
		  switch (arg) {
			case Infinity:
			  return "'inf'";
			case -Infinity:
			  return "'-inf'"
			case NaN:
			  return "'nan'"
			default:
			  return arg
		  }	
		default:
		  return arg
	 }
  }
  
  addOperator(arg,operator) {

	 if ((arg.length > 65000) && (operator.prefix.indexOf('(HEX_TO_BINARY(') > -1)) {
	   operator.prefix = operator.prefix.replace('(HEX_TO_BINARY(','(LONG_HEX_TO_BINARY(')
	 }
	 return `${operator.prefix}${this.addArgument(arg)}${operator.suffix}`
  }
  
  async _writeBatch(batch,rowCount) {
	  
    this.metrics.batchCount++;

	
	// console.log('Write Batch',this.metrics.batchCount,rowCount,'Copy',batch.copy.length,'Insert',batch.insert.length)
	
	const emptyStringDataSets = Object.keys(batch).filter((key) => {return ((key !== 'copy') && (key !== 'insert'))})
	delete emptyStringDataSets.copy
	delete emptyStringDataSets.insert

	let rejectedRecordsTableName = `YRT-${crypto.randomBytes(16).toString("hex").toUpperCase()}`;
	const dataSets = batch.copy.length === 0 ? {} : {
       copy : {
		 sql:   `${this.tableInfo.copy} REJECTED DATA AS TABLE "${rejectedRecordsTableName}"  NO COMMIT`	
	   , errors: rejectedRecordsTableName
	   }
    }		
	
    /*
	**
	** Modify Copy Statement to workaround Empty String is NULL issue
	**
	** https://forum.vertica.com/discussion/242246/preseve-empty-string-when-using-fcsv-parser#latest
    **
	** c2_filler FILLER VARCHAR, c2 AS NVL(c2_filler, ''), 
	**
	*/

	emptyStringDataSets.forEach((id) => {  
	  rejectedRecordsTableName = `YRT-${crypto.randomBytes(16).toString("hex").toUpperCase()}`;
	  const columns = id.split('-')
	  let sqlStatement = this.tableInfo.copy
	  columns.forEach((idx) => {
		const columnName = `"${this.tableInfo.columnNames[idx]}"`
		const fillerName = `"YADAMU_COL_${String(parseInt(idx)+1).padStart(3,"0")}"`
		const fillerSize = this.tableInfo.sizeConstraints[idx]
	    const columnReplacement = `${fillerName} FILLER${fillerSize > 65000 ? ' LONG ': ' '}VARCHAR(${fillerSize}), ${columnName} AS NVL(${fillerName}, '')`
		sqlStatement = sqlStatement.replace(columnName,columnReplacement)
	  }) 
	  dataSets[id] = { 
	    sql: `${sqlStatement} REJECTED DATA AS TABLE "${rejectedRecordsTableName}"  NO COMMIT`	
	  ,	errors : rejectedRecordsTableName
      }
    })
  
    try {
      await this.dbi.createSavePoint();
	  for (const key of Object.keys(dataSets)) {
		await this.writeBatchAsCSV(this.STAGING_FILE,batch[key])
		const stack = new Error().stack
        const results = await this.dbi.insertBatch(dataSets[key].sql,dataSets[key].errors);
		if (results.rejected > 0) {
	      await this.reportCopyErrors(results.errors,batch.copy,stack,dataSets[key].sql)
		}
		this.metrics.written+= results.inserted
		this.metrics.skipped+= results.rejected
		await this.cleanupStagingFile(this.STAGING_FILE,true);
	  }
      this.endTime = performance.now();
      await this.dbi.releaseSavePoint();
    } catch (cause) {
      this.endTime = performance.now();1
      this.cleanupStagingFile(this.STAGING_FILE,true);
	  await this.reportBatchError(batch.copy,`COPY`,cause)
      await this.dbi.restoreSavePoint(cause);
	  this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);  
      for (const key of Object.keys(dataSets)) {	  
        batch.insert.push(...batch[key])		
	  }
      this.tableInfo.insertMode = 'Iterative' 
    }
	 
	
	if (batch.insert.length > 0) {
	  for (const row in batch.insert) {
        try {
	      await this.dbi.createSavePoint();
		  const sqlStatement = `${this.tableInfo.dml} (${batch.insert[row].map((col,idx) => {return ((col === null) || (this.tableInfo.insertOperators[idx] === null)) ? this.addArgument(col) : this.addOperator(col,this.tableInfo.insertOperators[idx])}).join(",")})`
		  let results = await this.dbi.executeSQL(sqlStatement);
          await this.dbi.releaseSavePoint();
	   	  this.metrics.written++;
		  this.mergeoutInsertCount--;
		  if (this.mergeoutInsertCount === 0) {
			results = await this.dbi.executeSQL(this.tableInfo.mergeout);
			/*
			**
			** Monitor MergeOut
			**
			
			const sql = `SELECT OPERATION_START_TIMESTAMP, node_name, operation_status, table_schema, table_name, projection_name, plan_type FROM TUPLE_MOVER_OPERATIONS where table_schema = '${this.tableInfo._SCHEMA_NAME}' and table_name = '${this.tableName}' order by OPERATION_START_TIMESTAMP desc;`
			// console.log(sql)
		    results = await this.dbi.executeSQL(sql);
			console.log(results.rows);
						
			**
			*/

			this.mergeoutInsertCount = this.dbi.MERGEOUT_INSERT_COUNT
		  }
        } catch(cause) {
		  console.log(cause);
	  	  this.dbi.restoreSavePoint(cause);
          this.handleIterativeError(`INSERT ONE`,cause,row,batch.insert[row]);
          if (this.skipTable) {
            break
		  }
        }
      }
      this.endTime = performance.now();
    }
	this.releaseBatch(batch)
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
  
  toSQLIntervalYM(interval) {
    const jsInterval = YadamuLibrary.parse8601Interval(interval)
    return `${jsInterval.years || 0}-${jsInterval.months || 0}`
  }	
  
  toSQLIntervalDMS(interval) {
    const jsInterval = YadamuLibrary.parse8601Interval(interval)
    return `${jsInterval.days || 0}:${jsInterval.hours || 0}:${jsInterval.minutes || 0}:${jsInterval.seconds || 0}`
  }	
}

module.exports = VerticaWriter;

import crypto                   from 'crypto';
import { performance }          from 'perf_hooks';
import fs                       from 'fs';
import fsp                      from 'fs/promises';
import path                     from 'path'

import Yadamu                   from '../../core/yadamu.js';
import YadamuLibrary            from '../../lib/yadamuLibrary.js';
import StringWriter             from '../../util/stringWriter.js';
import YadamuSpatialLibrary     from '../../lib/yadamuSpatialLibrary.js';
import CSVLibrary               from '../loader/csvLibrary.js';
import YadamuWriter             from '../base/yadamuWriter.js';

import {FileError, FileNotFound, DirectoryNotFound} from '../file/fileException.js';
import {WhitespaceIssue, EmptyStringDetected, ContentTooLarge, StagingAreaMisMatch,  VerticaError, VertiaCopyOperationFailure } from './verticaException.js'

class VerticaWriter extends YadamuWriter {

  get STAGING_FILE()       { return this._STAGING_FILE }
  set STAGING_FILE(v)      { this._STAGING_FILE = v }
  
  constructor(dbi,tableName,metrics,status,yadamuLogger) {
    super(dbi,tableName,metrics,status,yadamuLogger)
  }

  async setTableInfo(tableName) {
    await super.setTableInfo(tableName)   
    this.tableInfo.insertMode = 'Copy'
    this.copyStatement = this.tableInfo.copy.dml
    this.mergeoutInsertCount = this.dbi.MERGEOUT_INSERT_COUNT;
  }

  async initializeTable() {
	await super.initializeTable()
	this.STAGING_FILE = this.tableInfo.stagingFileName
  }
     
  async initializePartition(partitionInfo) {
	await super.initializePartition(partitionInfo)
	const partitionId = partitionInfo.partitionNumber.toString().padStart(5,'0')
    this.STAGING_FILE = `${this.STAGING_FILE}-${partitionId}`
  }
  
  reportBatchError(batch,operation,cause) {
    // Use Slice to add first and last row, rather than first and last value.
    super.reportBatchError(operation,cause,batch[0],batch[batch.length-1])
  }
 
  async writeBatchAsCSV(filename,batch) {
    const sw = new StringWriter();
    const csvTransformations = CSVLibrary.getCSVTransformations(batch)
    CSVLibrary.writeBatchAsCSV(sw,batch,csvTransformations)
    let stack
    try {
      stack = new Error().stack
      await fsp.writeFile(filename,sw.toString())
    } catch (e) {
      throw new VerticaError(this.dbi.DRIVER_ID,e,stack,'writeBatchAsCSV')
    }
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
       err.columnLength = this.tableInfo.maxLengths[err.columnIdx]
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
      case((this.dbi.STAGING_FILE_RETENTION === 'FAILED') && loadSuccessful):
        // Delete the Staging File. Do not wait for success or failure    
        fsp.rm(stagingFile).then(()=>{}).catch(()=>{});
      default:
        // Leave the Staging File 
    }
  }
  
  addArgument(arg) {
     switch (typeof arg) {
        case 'string':
          return `'${arg.replace(/'/g,"''")}'`
        case 'object':
          return arg === null         ? 'null'
     		   : Buffer.isBuffer(arg) ? `'${arg.toString('hex')}'` 
			                          : `'${JSON.stringify(arg).replace(/'/g,"''")}'`
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
       operator.prefix = operator.prefix.replace('(HEX_TO_BINARY(','(YADAMU.LONG_HEX_TO_BINARY(')
     }
     return `${operator.prefix}${this.addArgument(arg)}${operator.suffix}`
  }
  
  async _writeBatch(batch,rowCount) {

    // console.log(batch)
      
    const emptyStringDataSets = Object.keys(batch).filter((key) => {return ((key !== 'copy') && (key !== 'insert'))})
    delete emptyStringDataSets.copy
    delete emptyStringDataSets.insert

    let rejectedRecordsTableName = `YRT-${crypto.randomBytes(16).toString("hex").toUpperCase()}`;

    const copyOperations = batch.copy.length === 0 ? {} : {
       copy : {
         sql:   `${this.copyStatement} REJECTED DATA AS TABLE "${rejectedRecordsTableName}"  NO COMMIT` 
       , errors: rejectedRecordsTableName
       }
    }       
    
    /*
    **
    ** Modify Copy Statement to workaround Empty String is NULL issue
    ** A seperate SQL Statment is required for each unqiue combination of columns
    **
    ** https://forum.vertica.com/discussion/242246/preseve-empty-string-when-using-fcsv-parser#latest
    **
    ** c2_filler FILLER VARCHAR, c2 AS NVL(c2_filler, ''), 
    **
    */
    
    emptyStringDataSets.forEach((id) => {  
      rejectedRecordsTableName = `YRT-${crypto.randomBytes(16).toString("hex").toUpperCase()}`;
      const columns = id.split('-')
      let sqlStatement = this.copyStatement
      columns.forEach((idx) => {
        const columnName = `"${this.tableInfo.columnNames[idx]}"`
        const fillerName = `"YADAMU_COL_${(parseInt(idx)+1).toString().padStart(3,"0")}"`
        const fillerSize = this.tableInfo.maxLengths[idx]
        const columnReplacement = `${fillerName} FILLER${fillerSize > 65000 ? ' LONG ': ' '}VARCHAR(${fillerSize}), ${columnName} AS NVL(${fillerName}, '')`
        sqlStatement = sqlStatement.replace(columnName,columnReplacement)
      }) 
      // Add an entry to the Copy Operations table containin the SQL statement for this particular  data set.
      copyOperations[id] = { 
        sql: `${sqlStatement} REJECTED DATA AS TABLE "${rejectedRecordsTableName}"  NO COMMIT`  
      , errors : rejectedRecordsTableName
      }
    })
   
    const batchStagingFileName = `${this.STAGING_FILE}-${parseInt(this.BATCH_METRICS.batchNumber).toString().padStart(5,"0")}`
	// results = await Promise.all(Object.keys(copyOperations).map(async(key,idx) => {
	const keys = Object.keys(copyOperations)
	for (const idx in keys) {
      const key = keys[idx]
	  await this.dbi.createSavePoint();
      let stagingFileName = `${batchStagingFileName}.${(parseInt(idx)+1).toString().padStart(3,"0")}`
	  const stagingFilePath = this.tableInfo.localPath.replace(this.tableInfo.stagingFileName,stagingFileName)
	  try {
        await this.writeBatchAsCSV(stagingFilePath,batch[key])
        const stack = new Error().stack
        const sqlStatement = copyOperations[key].sql.replace(this.tableInfo.stagingFileName,stagingFileName)
        // console.log(sqlStatement)
        // console.log(fs.readFileSync(stagingFilePath).toString('utf8'))
        const results = await this.dbi.insertBatch(sqlStatement,copyOperations[key].errors);
		if (results.rejected > 0) {
          await this.reportCopyErrors(results.errors,batch[key],stack,sqlStatement)
        }
        this.adjustRowCounts(results.inserted)
        this.COPY_METRICS.skipped+= results.rejected
        await this.cleanupStagingFile(stagingFilePath,true);
        await this.dbi.releaseSavePoint(); 
      } catch (cause) {
        if (cause.missingFile && cause.missingFile()) {
          cause = new StagingAreaMisMatch(this.dbi.DRIVER_ID,batchStagingFileName,this.dbi.LOCAL_STAGING_AREA, this.dbi.REMOTE_STAGING_AREA,cause)
        } 
        this.cleanupStagingFile(stagingFilePath,false);
        await this.reportBatchError(batch[key],`COPY`,cause)
        await this.dbi.restoreSavePoint(cause);
        this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.tableInfo.insertMode],`Switching to Iterative mode.`);  
        for (const key of Object.keys(copyOperations)) {     
          batch.insert.push(...batch[key])     
        }
        this.tableInfo.insertMode = 'Iterative' 
      }
    }
    this.endTime = performance.now();
	
    if (batch.insert.length > 0) {
      for (const row in batch.insert) {
        try {
          await this.dbi.createSavePoint();
          const sqlStatement = `${this.tableInfo.dml} (${batch.insert[row].map((col,idx) => {return ((col === null) || (this.tableInfo.insertOperators[idx] === null)) ? this.addArgument(col) : this.addOperator(col,this.tableInfo.insertOperators[idx])}).join(",")})`
          let results = await this.dbi.executeSQL(sqlStatement);
          await this.dbi.releaseSavePoint();
          this.adjustRowCounts(1);
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
}  

export { VerticaWriter as default }
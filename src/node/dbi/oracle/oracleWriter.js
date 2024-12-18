
import { 
  performance 
}                              from 'perf_hooks';

import 
  { Writable
}                              from "stream";

import oracledb                from 'oracledb';

import YadamuLibrary           from '../../lib/yadamuLibrary.js';
import YadamuSpatialLibrary    from '../../lib/yadamuSpatialLibrary.js';

import {
  DatabaseError
}                              from '../../core/yadamuException.js';

import YadamuDataTypes         from '../base/yadamuDataTypes.js'
import YadamuWriter            from '../base/yadamuWriter.js';

class OracleWriter extends YadamuWriter {

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

  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {
    super(dbi,tableName,pipelineState,status,yadamuLogger)
	this.tempLobCount = 0;
	this.cachedLobCount = 0;
	this.partitionInfo = undefined;
    this.triggersDisabled = false
  }

  async beginTransaction() {
    this.tempLobCount = 0
	this.cacheLobCount = 0
	await super.beginTransaction()
  }
  
  async initializeTable() {
	await super.initializeTable()
	this.dml = this.tableInfo.dml
    if (!this.PARTITIONED_TABLE || (this.PARTITIONED_TABLE && (this.tableInfo.PARTITION_NUMBER === 0))) {
	  await this.dbi.disableTriggers(this.dbi.CURRENT_SCHEMA,this.tableInfo.tableName)
	  this.triggersDisabled = true
      return true
    }
  }
  
  isValidPartition(partitionInfo) {
	 return this.tableInfo.partitionMetadata?.includes(partitionInfo.partitionName)
  }
  
  async initializePartition(partitionInfo) {
	await super.initializePartition(partitionInfo)
	this.tableInfo.partitionMetadata = this.tableInfo.partitionMetadata || this.dbi?.partitionMetadata?.hasOwnProperty(this.tableName) ? this.dbi.partitionMetadata[this.tableName] : undefined 
    if (this.isValidPartition(partitionInfo)) {
      this.dml = this.dml.replace(`."${this.tableName}" (`,`."${this.tableName}" PARTITION("${partitionInfo.partitionName}") (`)	
	}
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
  
  /* OVERRIDES */
  
  async beginTransaction() {
    this.tempLobCount = 0
	this.cacheLobCount = 0
	await super.beginTransaction()
  }

  getMetrics() {
	const tableStats = super.getMetrics()
	tableStats.sqlTime = tableStats.sqlTime + this.lobCumlativeTime;
    return tableStats;  
  }

  async serializeLobColumns(row) {
	
	return await this.dbi.serializeLobColumns(this.tableInfo,row)
	
  }   

  async handleIterativeError(operation,cause,rowNumber,record) {

     // If cause is generated by the SQL layer it alrea1dys contain SQL and bind information.

	const newRecord = await this.serializeLobColumns(record)
	 if (Array.isArray(cause.args)) {
	   cause.args = await this.dbi.serializeLobBinds(cause.args)
	 }
	 super.handleIterativeError(operation,cause,rowNumber,newRecord) 
  }

  async retryGeoJSONAsWKT(sqlStatement,binds,batchSize,rowNumber,row) {
    const batch = [await this.serializeLobColumns(row)]
	YadamuSpatialLibrary.recodeSpatialColumns('GeoJSON','WKT',this.tableInfo.targetDataTypes,batch,true)
    try {
	  // Create a bound row by cloning the current set of binds and adding the column value.
	  const boundRow = batch[0].map((col,idx) => {
		return {
		   ...binds[idx]
		 , val: col
		}
	  })
	  sqlStatement = sqlStatement.replace(/DESERIALIZE_GEOJSON/g,'DESERIALIZE_WKTGEOMETRY')
      const results = await this.dbi.executeSQL(sqlStatement,boundRow)
      this.adjustRowCounts(1)
  	  this.dbi.resetExceptionTracking()
    } catch (cause) {
	  this.handleIterativeError('INSERT ONE',cause,rowNumber,batch[0]);
    }
  }
  
  async retryWKBAsWKT(sqlStatement,binds,batchSize,rowNumber,row) {
    const batch = [await this.serializeLobColumns(row)]
	YadamuSpatialLibrary.recodeSpatialColumns('WKB','WKT',this.tableInfo.targetDataTypes,batch,true)
    try {
	  // Create a bound row by cloning the current set of binds and adding the column value.
	  const boundRow = batch[0].map((col,idx) => {
		 return {
		   ...binds[idx]
		 , val: col
		 }
	  })
      const spatialColumnList = this.tableInfo.targetDataTypes.reduce((columnList,dataType,idx) => { if (YadamuDataTypes.isSpatial(dataType)) columnList.push(idx); return columnList},[])        
      spatialColumnList.forEach((colIdx) => {boundRow[colIdx] = {type : oracledb.DB_TYPE_CLOB, maxSize : boundRow[colIdx].val.length, val:boundRow[colIdx].val}})
	  sqlStatement = sqlStatement.replace(/DESERIALIZE_WKBGEOMETRY/g,'DESERIALIZE_WKTGEOMETRY')
      const results = await this.dbi.executeSQL(sqlStatement,boundRow)
      this.adjustRowCounts(1)
	  this.dbi.resetExceptionTracking()
    } catch (cause) {
	  this.handleIterativeError('INSERT ONE',cause,rowNumber,batch[0]);
    }
  }
  
  freeLobList() {
  }
    	 
  async reportBatchError(operation,cause,rows) {
	  
	// If cause is generated by the SQL layer it alreadys contain SQL and bind information.

    const info = {}

    if (this.includeTestcase) {
	  // ### Need to serialize and LOBS and parse JSON objects when generating a testcase.
	  info.testcase = {
        DDL:   this.tableInfo.ddl
      , DML:   this.dml
      , binds: binds
	  , data:  rows.slice(0,9)
	  }
	}
	
	const firstRow = await this.serializeLobColumns(rows[0])
	const lastRow  = await this.serializeLobColumns(rows[rows.length-1])
	
	super.reportBatchError(operation,cause,firstRow,lastRow,info)
  }
   
  avoidMutatingTable(insertStatement) {

    let insertBlock = undefined;
    let selectBlock = undefined;
  
    let statementSeperator = "\nwith\n"
    if (insertStatement.indexOf(statementSeperator) === -1) {
      statementSeperator = "\nselect :1";
      if (insertStatement.indexOf(statementSeperator) === -1) {
         // INSERT INTO TABLE (...) VALUES ... 
        statementSeperator = "\n         values (:1";
        insertBlock = insertStatement.substring(0,insertStatement.indexOf('('));
        selectBlock = `select ${insertStatement.slice(insertStatement.indexOf(':1'),-1)} from DUAL`;   
      }
      else {
         // INSERT INTO TABLE (...) SELECT ... FROM DUAL;
        insertBlock = insertStatement.substring(0,insertStatement.indexOf('('));
        selectBlock = insertStatement.substring(insertStatement.indexOf(statementSeperator)+1);   
      }
    }
    else {
      // INSERT /*+ WITH_PL/SQL */ INTO TABLE(...) WITH PL/SQL SELECT ... FROM DUAL;
      insertBlock = insertStatement.substring(0,insertStatement.indexOf('\\*+'));
      selectBlock = insertStatement.substring(insertStatement.indexOf(statementSeperator)+1);   
    }
       
    const plsqlBlock  = 
`declare
  cursor getRowContent 
  is
  ${selectBlock};
begin
  for x in getRowContent loop
    ${insertBlock}
           values x;
  end loop;
end;`
    return plsqlBlock;
  }
  
  commitWork() {
    // While COMMIT is defined as a multiple of BATCH_SIZE some drivers may write smaller batches.
    return (super.commitWork() || (this.tempLobCount >= this.dbi.COMMIT_TEMPLOB_LIMIT) || (this.cachedLobCount > this.dbi.COMMIT_CACHELOB_LIMIT))
  }
    
  async _writeBatch(batch) {
	 
    // Ideally we used should reuse tempLobs since this is much more efficient that setting them up, using them once and tearing them down.
    // Unfortunately the current implimentation of the Node Driver does not support this, once the 'finish' event is emitted you cannot truncate the tempCLob and write new content to it.
    // So we have to free the current tempLob Cache and create a new one for each batch
	
    // console.log(this.tableInfo,batch)
	
	let rows = undefined;
    let binds = undefined;
    const rowCount = batch.rows.length + batch.lobRows.length
	
    const lobInsert = (batch.lobRows.length > 0)
    if (lobInsert) {
	   // this.batch.lobRows constists of a an array of arrays of pending promises that need to be resolved.
	  batch.lobRows = await Promise.all(batch.lobRows.map(async (row) => { return await Promise.all(row.map((col) => {return col}))})) 
	}
	
	this.tempLobCount+=batch.tempLobCount;
	this.cacheLobCount+=batch.cacheLobCount;
	
    if (this.tableInfo.insertMode === 'Batch') {
      try {
        rows = batch.rows
        binds = this.tableInfo.binds
    	await this.dbi.createSavePoint()
        const results = await this.dbi.executeMany(this.dml,rows,{bindDefs : binds});
		if (lobInsert) {
          rows = batch.lobRows
          binds = this.tableInfo.lobBinds
          const results = await this.dbi.executeMany(this.dml,rows,{bindDefs : binds},batch.tempLobCount);
		  // await Promise.all(this.freeLobList());
          this.freeLobList();
        }         
        this.endTime = performance.now();
        this.adjustRowCounts(rowCount);
        this.releaseBatch(batch)
        return this.skipTable
      } catch (cause) {
		if (cause.spatialWKBPolygonError()) {
		  this.LOGGER.info([this.dbi.DATABASE_VENDOR,this.PIPELINE_STATE.displayName,this.tableInfo.insertMode],`"${cause.message}" encountered while peforming batch operation with WKB content. Switching to Iterative mode.`);          
		}
		else {
		  // Report Batch Error throws cause if there are lost rows.
		  await this.reportBatchError(`INSERT MANY`,cause,rows) 
		}
	    await this.dbi.restoreSavePoint(cause);
		if (cause.errorNum && (cause.errorNum === 4091)) {
          // Mutating Table - Convert to Cursor based PL/SQL Block
          this.LOGGER.info([this.dbi.DATABASE_VENDOR,this.PIPELINE_STATE.displayName,this.tableInfo.insertMode],`Switching to PL/SQL Block.`);          
          this.dml = this.avoidMutatingTable(this.dml);
          try {
            rows = batch.rows
            binds = this.tableInfo.binds
            await this.dbi.createSavePoint()
            const results = await this.dbi.executeMany(this.dml,rows,{bindDefs : binds});
            if (lobInsert) {
              rows = batch.lobRows
              binds = this.tableInfo.lobBinds
              const results = await this.dbi.executeMany(this.dml,rows,{bindDefs : binds});
              // await Promise.all(this.freeLobList());
              this.freeLobList();
            }         
            this.endTime = performance.now();
            this.adjustRowCounts(rowCount)
            this.releaseBatch(batch)
            return this.skipTable
          } catch (cause) {
  		    await this.reportBatchError(batch,`INSERT MANY [PL/SQL]`,cause,rows) 
            await this.dbi.restoreSavePoint(cause);
            this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.PIPELINE_STATE.displayName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
		    this.dbi.resetExceptionTracking()
            this.tableInfo.insertMode = 'Iterative';
          }
        } 
        else {  
          if (!cause.spatialWKBPolygonError()) {
		    this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.PIPELINE_STATE.displayName,this.tableInfo.insertMode],`Switching to Iterative mode.`);          
		  }
   		  this.dbi.resetExceptionTracking()
          this.tableInfo.insertMode = 'Iterative';
        }
	  }
    }

    const allRows  = [batch.rows,batch.lobRows]
	const allBinds = [this.tableInfo.binds,this.tableInfo.lobBinds]
    while (allRows.length > 0) {
	  const rows = allRows.shift();
	  const binds = allBinds.shift();
      for (const row in rows) {
        try {
		  // Create a bound row by cloning the current set of binds and adding the column value.
		  // boundRow = await Promise.all([... new Array(rows[row].length).keys()].map(async (i) => {const bind = Object.assign({},binds[i]); bind.val=await rows[row][i]; return bind}))
		  const boundRow = rows[row].map((col,idx) => {
			return {
			  ...binds[idx]
			, val: col
			}
		  })
          const results = await this.dbi.executeSQL(this.dml,boundRow)
		  this.adjustRowCounts(1)
        } catch (cause) {
		  switch (true) {
		    case ((cause instanceof DatabaseError) && cause.jsonParsingFailed() && cause.includesSpatialOperation()):
			  await this.retryGeoJSONAsWKT(this.dml,binds,allRows.length,row,rows[row])
			  break;
		    case ((cause instanceof DatabaseError) && cause.includesSpatialOperation() && cause.spatialErrorWKB()):
			  await this.retryWKBAsWKT(this.dml,binds,allRows.length,row,rows[row])
			  break;
			default:
		      await this.handleIterativeError('INSERT ONE',cause,row,await this.serializeLobColumns(rows[row]));
		  }
          if (this.skipTable) {
  		    // Truncate the allRows array to terminate the outer loop as well
		    allRows.length = 0
            break;
		  }
        }
	  }
    } 
	
    this.endTime = performance.now();
    // await Promise.all(this.freeLobList());
    this.freeLobList();
    this.releaseBatch(batch)
    return this.skipTable     
  }

  async doDestroy(err) {
	  
    // this.LOGGER.trace([this.constructor.name,this.PIPELINE_STATE.displayName,this.dbi.getWorkerNumber(),this.PIPELINE_STATE.received,this.PIPELINE_STATE.cached,this.PIPELINE_STATE.written,this.PIPELINE_STATE.skipped,this.PIPELINE_STATE.lost],'doDestroy()')

    if (this.triggersDisabled && (!this.PARTITIONED_TABLE  || (this.tableInfo.partitionsRemaining === 0))) {
	  // this.LOGGER.trace([this.constructor.name,'doDestory()','BATCH_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'WAITING')
	  await this.batchCompleted
      // this.LOGGER.trace([this.constructor.name,'doDestory()','BATCH_COMPLETE',this.dbi.getWorkerNumber(),this.tableName],'PROCESSING')
	  await this.endTransaction()
	  await this.dbi.enableTriggers(this.dbi.CURRENT_SCHEMA,this.tableInfo.tableName);
	  this.triggersDisabled = false;
    }
	await super.doDestroy(err)

  }
  
}

export {OracleWriter as default }
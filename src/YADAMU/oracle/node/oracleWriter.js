"use strict"

const { performance } = require('perf_hooks');

const oracledb = require('oracledb');

const YadamuLibrary = require('../../common/yadamuLibrary.js');
const YadamuSpatialLibrary = require('../../common/yadamuSpatialLibrary.js');
const YadamuWriter = require('../../common/yadamuWriter.js');
const {DatabaseError} = require('../../common/yadamuException.js');

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
  **    LOB_BATCH_COUNT  : A Batch will be regarded as complete when it uses more LOBS than LOB_BATCH_COUNT
  **    LOB_MIN_SIZE     : If a String or Buffer is mapped to a CLOB or a BLOB then it will be inserted using a LOB if it exceeeds this value.
  **    LOB_CACHE_COUNT  : A Batch will be regarded as complete when the number of CACHED (String & Buffer) LOBs exceeds this value.
  **
  ** The amount of client side memory required to manage the LOB Cache is approx LOB_MIN_SIZE * LOB_CACHE_COUNT
  **
  */

  constructor(dbi,tableName,ddlComplete,status,yadamuLogger) {
    super({objectMode: true},dbi,tableName,ddlComplete,status,yadamuLogger)
  }
  
  newBatch() {
	super.newBatch()
	this.batch = {
	  rows           : []
	, lobRows        : []
	, tempLobCount   : 0
	, cachedLobCount : 0
    }
  }
  
  releaseBatch(batch) {
	if (Array.isArray(batch.rows)) {
	  batch.rows.length = 0;
	  batch.lobRows.length = 0;
	}
  }
  
  setTableInfo(tableName) {
    this.newBatch();
	this.lobList = []
    this.includeTestcase = this.dbi.parameters.EXPORT_TESTCASE === true
    this.lobCumlativeTime = 0;
	
    super.setTableInfo(tableName)   
	// Set up an Array of Transformation functions to be applied to the incoming rows
	
	this.transformations = this.tableInfo.targetDataTypes.map((targetDataType,idx) => {
      const dataType = YadamuLibrary.decomposeDataType(targetDataType);
      switch (dataType.type.toUpperCase()) {
        case "GEOMETRY":
        case "GEOGRAPHY":
        case '"MDSYS"."SDO_GEOMETRY"':
          // Metadata based decision
          if ((this.dbi.DB_VERSION < 12) && (this.SPATIAL_FORMAT === 'GeoJSON')) {
            // SDO_UTIL does not support GeoJSON in 11.x database
            return (col,jdx) =>  {
			  return YadamuSpatialLibrary.geoJSONtoWKT(col)
            }
          }
          else {
            return null
          }
          break;
		case "BFILE":
		    // Convert JSON representation to String.
        case "SET":
        case "JSON":
          return (col,jdx) =>  {
            // row[idx] = Buffer.from(JSON.stringify(row[idx]))
            // JSON must be shipped in Serialized Form
            return typeof col === 'object' ? JSON.stringify(col) : col
          }
          break;
        case "RAW":
          /*
          if (this.dbi.TREAT_RAW1_AS_BOOLEAN) {
            if (typeof col === 'boolean') {
              return  new Buffer.from(col === true ? [1] : [0])
            }
          */
          return (col,jdx) =>  {
            return typeof col === 'boolean' ? new Buffer.from(col === true ? [1] : [0]) :  col
          }
          break;
        case "BOOLEAN":
          return (col,jdx) =>  {
            return YadamuLibrary.booleanToBuffer(col)
		  }
          break;
        case "DATE":
          return (col,jdx) =>  { 
            if (col instanceof Date) {
              return col.toISOString()
            }
            return col;
          }
          break;
        case "TIMESTAMP":
          return (col,jdx) =>  { 
            // A Timestamp not explicitly marked as UTC should be coerced to UTC.
            // Avoid Javascript dates due to lost of precsion.
            // row[bindIdx] = new Date(Date.parse(col.endsWith('Z') ? col : col + 'Z'));
            if (typeof col === 'string') {
              return (col.endsWith('Z') || col.endsWith('+00:00')) ? col : col + 'Z';
            } 
            if (col instanceof Date) {
              return col.toISOString()
            }
            return col
          }
          break;
        case "XMLTYPE" :
          /*
          // Cannot passs XMLTYPE as BUFFER: ORA-06553: PLS-307: too many declarations of 'XMLTYPE' match this call
		  return (col,jdx) =>  { 
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
 
     // Use a dummy rowTransformation function if there are no transformations required.

	this.rowTransformation = this.transformations.every((currentValue) => { currentValue === null}) ? (row) => {} : (row) => {
      this.transformations.forEach((transformation,idx) => {
        if ((transformation !== null) && (row[idx] !== null)) {
          row[idx] = transformation(row[idx],idx)
        }
      }) 
    }

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
		    return (col,jdx) =>  {
              // Determine whether to bind content as string or temporary CLOB
              if (typeof col !== "string") {
                col = JSON.stringify(col);
              }
              this.batch.cachedLobCount++
              this.bindRowAsLOB = this.bindRowAsLOB || (Buffer.byteLength(col,'utf8') > this.dbi.LOB_MIN_SIZE) || Buffer.byteLength(col,'utf8') === 0
			  return col;
		    }
		    break;
          case oracledb.BLOB:
		    return (col,jdx) =>  {
			  /*
			  **
			  ** At this point we can have one of the following to deal with:
			  ** 
			  ** 1. A Buffer
			  ** 2. A string containing HexBinary encoded content
			  ** 3. A JSON Object
			  **
			  ** If we have a JSON object stringify it.
			  ** If we have a HexBinary representation of a Buffer convert it into a Buffer unless the resuling buffer would exceed the maximu size defined for a client cached object.
			  ** the maximum size of a client cached LOB.
			  ** If we have an object we need to serialize it and convert the serialization into a Buffer
			  */
              // Determine whether to bind content as Buffer or temporary BLOB
			  if ((typeof col === "object") && (!Buffer.isBuffer(col))) {
				col = Buffer.from(JSON.stringify(col),'utf-8')
			  }
              if ((typeof col === "string") /* && ((col.length/2) <= this.dbi.LOB_MIN_SIZE) */) {
				if (YadamuLibrary.decomposeDataType(this.tableInfo.targetDataTypes[idx]).type === 'JSON') {
				  col = Buffer.from(col,'utf-8')
				}
				else {
				  col = Buffer.from(col,'hex')
				}
              }
		      this.batch.cachedLobCount++
			  this.bindRowAsLOB = this.bindRowAsLOB || (col.length > this.dbi.LOB_MIN_SIZE) 
			  return col
			}
            break
          default:
		    return null;
	    }
      })
    }
  }

  async initialize(tableName) {
    await super.initialize(tableName)
    await this.disableTriggers()
 }

  async disableTriggers() {
  
    const sqlStatement = `ALTER TABLE "${this.schema}"."${this.tableInfo.tableName}" DISABLE ALL TRIGGERS`;
    return await this.dbi.executeSQL(sqlStatement,[]);
    
  }

  async enableTriggers() {
   
	try {
  	  this.dbi.checkConnectionState(this.dbi.latestError) 
      const sqlStatement = `ALTER TABLE "${this.schema}"."${this.tableInfo.tableName}" ENABLE ALL TRIGGERS`;
      return await this.dbi.executeSQL(sqlStatement,[]);
	} catch (e) {
	  this.yadamuLogger.error(['DBA',this.dbi.DATABASE_VENDOR,'TRIGGERS',this.tableInfo.tableName],`Unable to re-enable triggers.`);          
      this.yadamuLogger.handleException(['TRIGGERS',this.dbi.DATABASE_VENDOR,],e);          
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
  
  checkBindMappings(row) {
	
	// Check performed on re-ordered rows. When all binds have been checked disable check
    // Clone the array of bind positions as bad things happen if you splice the target of a forEach operation inside the forEach operation
    
    // Loop backwards to the splice operation has no effect on the idx of the remaining items
    
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
      this.checkBindMappings = (row) => {}
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
    ** If tableInfo.lobColumns = true then the contents of row need to be re-ordering a based on tableIno.bindOrdering
	** If talbeInfo.lobColuns = false then there are no lobs so there is no need to re-order the row.
    */
          
    // this.yadamuLogger.trace([this.constructor.name,this.tableInfo.lobColumns,this.metrics.cached],'cacheRow()')
    // if (this.metrics.received === 1) {console.log(row)}
	
	try {          
      this.bindRowAsLOB = false;
	  if (this.tableInfo.lobColumns) {
		// Bind Ordering and Row Ordering are the probably different. Use map to create a new array in BindOrdering when applying transformations
	    row = this.transformations.map((transformation,bindIdx) => {
          const rowIdx = this.tableInfo.bindOrdering[bindIdx]
		  if (row[rowIdx] !== null) {		
            if (transformation !== null) {
			  row[rowIdx] = transformation(row[rowIdx],rowIdx);
			}
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
	  this.checkBindMappings(row)
      this.metrics.cached++
    } catch (cause) {
	  this.handleIterativeError('CACHE ONE',cause,this.metrics.cached+1,row);
    }
	
	return this.skipTable
	
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
 
  batchComplete() {
    return ((this.metrics.cached === this.BATCH_SIZE) || (this.batch.tempLobCount >= this.dbi.BATCH_LOB_COUNT) || (this.batch.cachedLobCount > this.dbi.LOB_CACHE_COUNT))
  }

  async serializeLob(lob) {
	switch (lob.type) {
      case oracledb.CLOB:
        // ### Cannot directly re-read content that has been written to local clob
        return await  this.dbi.clientClobToString(lob)
      case oracledb.BLOB:
        // ### Cannot directly re-read content that has been written to local blob
        return await this.dbi.clientBlobToBuffer(lob)
      default:
        return lob
    } 
  }   
   
  async serializeLobColumns(row) {
	// Convert Lobs back to Strings or Buffers
	// row = await Promise.all(row);
    const newRow = await Promise.all(row.map((col,idx) => {
      if (col instanceof oracledb.Lob) {
	    return this.serializeLob(col)
      }
      return col
    }))
	// Put the Lobs back into the original order
	const columnOrderedRow = []
	this.tableInfo.bindOrdering.forEach((lidx,idx) => {
	  columnOrderedRow[lidx] = newRow[idx]
	})
	return columnOrderedRow 
  }   
       	  
  async reportBatchError(operation,cause,rows) {
	  
	// If cause is generated by the SQL layer it alreadys contain SQL and bind information.

    const info = {}

    if (this.includeTestcase) {
	  // ### Need to serialize and LOBS and parse JSON objects when generating a testcase.
	  info.testcase = {
        DDL:   this.tableInfo.ddl
      , DML:   this.tableInfo.dml
      , binds: binds
	  , data:  rows.slice(0,9)
	  }
	}
	
	const firstRow = await this.serializeLobColumns(rows[0])
	const lastRow  = await this.serializeLobColumns(rows[rows.length-1])
	
	super.reportBatchError(operation,cause,firstRow,lastRow,info)
  }
  
  async serializeLobBinds(binds) {
    return await Promise.all(binds.map(async (bind,idx) => {
      if (bind.val instanceof oracledb.Lob) {
	    bind.val = this.dbi.stringToJSON(await this.serializeLob(bind.val))
      }
      return bind
    }))
  }
	 
  async retryGeoJSONAsWKT(sqlStatement,binds,rowNumber,row) {
    const batch = [await this.serializeLobColumns(row)]
	YadamuSpatialLibrary.recodeSpatialColumns('GeoJSON','WKT',this.tableInfo.targetDataTypes,batch,true)
    try {
	  // Create a bound row by cloning the current set of binds and adding the column value.
	  const boundRow = batch[0].map((col,idx) => {return Object.assign({},binds[idx],{val: col})})
	  sqlStatement = sqlStatement.replace(/DESERIALIZE_GEOJSON/g,'DESERIALIZE_WKTGEOMETRY')
      const results = await this.dbi.executeSQL(sqlStatement,boundRow)
      this.metrics.written++
    } catch (cause) {
	  this.handleIterativeError('INSERT ONE',cause,rowNumber,batch[0]);
    }
  }
  
	 
  async handleIterativeException(operation,cause,batchSize,rowNumber,record) {
	
     // If cause is generated by the SQL layer it alreadys contain SQL and bind information.

	const newRecord = await this.serializeLobColumns(record)
	 if (Array.isArray(cause.args)) {
	   cause.args = await this.serializeLobBinds(cause.args)
	 }
	 await super.handleIterativeException(operation,cause,batchSize,rowNumber,newRecord) 
  }
  
  /*
  **
  ** Temporary LOB optimization via LOB column resuse disabled until oracledb supports reusing temporary lobs  
  ** 
  ** freeLobList() {
  **   return this.lobList.map((lob,idx) => {
  **     return lob.trunc(0);
  **   })
  ** }
  ** 
  ** freeLobList() {
  **   let lobCount = 0;
  **   this.lobList.forEach(async (lob,idx) => {
  **     try {
  **       await lob.close();
  **       lobCount++;
  **     } catch(e) {
  **       this.yadamuLogger.logException([`${this.constructor.name}.freeLobList()`,`${this.tableName}`,`${idx}`],e);
  **     }   
  **   })
  **   if (lobCount > 0) {
  **     this.yadamuLogger.info([`${this.constructor.name}.freeLobList()`,`${this.tableName}`],`Closed ${lobCount} lobs.`); 
  **   }
  ** }
  **
  */
  
  freeLobList() {
  }
     
  getMetrics() {
	const tableStats = super.getMetrics()
	tableStats.sqlTime = tableStats.sqlTime + this.lobCumlativeTime;
    return tableStats;  
  }
  
  async _writeBatch(batch,rowCount) {
      
    // Ideally we used should reuse tempLobs since this is much more efficient that setting them up, using them once and tearing them down.
    // Unfortunately the current implimentation of the Node Driver does not support this, once the 'finish' event is emitted you cannot truncate the tempCLob and write new content to it.
    // So we have to free the current tempLob Cache and create a new one for each batch

    this.metrics.batchCount++;
    let rows = undefined;
    let binds = undefined;
    
    const lobInsert = (batch.lobRows.length > 0)
    if (lobInsert) {
	   // this.batch.lobRows constists of a an array of arrays of pending promises that need to be resolved.
	  batch.lobRows = await Promise.all(batch.lobRows.map(async (row) => { return await Promise.all(row.map((col) => {return col}))})) 
	}

    if (this.insertMode === 'Batch') {
      try {
        rows = batch.rows
        binds = this.tableInfo.binds
        await this.dbi.createSavePoint()
        const results = await this.dbi.executeMany(this.tableInfo.dml,rows,{bindDefs : binds});
		if (lobInsert) {
          rows = batch.lobRows
          binds = this.tableInfo.lobBinds
          const results = await this.dbi.executeMany(this.tableInfo.dml,rows,{bindDefs : binds});
          // await Promise.all(this.freeLobList());
          this.freeLobList();
        }         
        this.endTime = performance.now();
        this.metrics.written += rowCount;
        this.releaseBatch(batch)
        return this.skipTable
      } catch (cause) {
	    await this.reportBatchError(`INSERT MANY`,cause,rows) 
        await this.dbi.restoreSavePoint(cause);
		if (cause.errorNum && (cause.errorNum === 4091)) {
          // Mutating Table - Convert to Cursor based PL/SQL Block
          this.yadamuLogger.info([this.dbi.DATABASE_VENDOR,this.tableName,this.insertMode],`Switching to PL/SQL Block.`);          
          this.tableInfo.dml = this.avoidMutatingTable(this.tableInfo.dml);
          try {
            rows = batch.rows
            binds = this.tableInfo.binds
            await this.dbi.createSavePoint()
            const results = await this.dbi.executeMany(this.tableInfo.dml,rows,{bindDefs : binds});
            if (lobInsert) {
              rows = batch.lobRows
              binds = this.tableInfo.lobBinds
              const results = await this.dbi.executeMany(this.tableInfo.dml,rows,{bindDefs : binds});
              // await Promise.all(this.freeLobList());
              this.freeLobList();
            }         
            this.endTime = performance.now();
            this.metrics.written += rowCount
            this.releaseBatch(batch)
            return this.skipTable
          } catch (cause) {
  		    await this.reportBatchError(batch,`INSERT MANY [PL/SQL]`,cause,rows) 
            await this.dbi.restoreSavePoint(cause);
            this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.insertMode],`Switching to Iterative mode.`);          
            this.insertMode = 'Iterative';
          }
        } 
        else {  
          this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,this.tableName,this.insertMode],`Switching to Iterative mode.`);          
          this.insertMode = 'Iterative';
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
		  const boundRow = rows[row].map((col,idx) => {return Object.assign({},binds[idx],{val: col})})
          const results = await this.dbi.executeSQL(this.tableInfo.dml,boundRow)
		  this.metrics.written++
        } catch (cause) {
		  if ((cause instanceof DatabaseError) && cause.jsonParsingFailed() && cause.includesSpatialOperation()) {
			await this.retryGeoJSONAsWKT(this.tableInfo.dml,binds,row,rows[row])
		  }
		  else {
            this.handleIterativeError('INSERT ONE',cause,row,await this.serializeLobColumns(rows[row]));
		  }
        }
        if (this.skipTable) {
  		  // Truncate the allRows array to terminate the outer loop as well
		  allRows.length = 0
          break;
        }
	  }
    } 
	
    this.endTime = performance.now();
    // await Promise.all(this.freeLobList());
    this.freeLobList();
    this.releaseBatch(batch)
    return this.skipTable     
  }
   
  async finalize(cause) {
    await super.finalize(cause);
	// Re-enable triggers on the current table
	// Skip enablling enableTriggers if tableInfo is not available. If tableInfo is not available an exception must have prevented initialize() from completing successfully. 
	if (this.tableInfo) {
      await this.enableTriggers();
	}
  }
}

module.exports = OracleWriter;
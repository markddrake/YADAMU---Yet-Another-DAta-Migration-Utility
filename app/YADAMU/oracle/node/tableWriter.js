"use strict"

const { performance } = require('perf_hooks');

// const WKX = require('wkx');

const oracledb = require('oracledb');

const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {

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
  **    LOB_BATCH_COUNT : A Batch will be regarded as complete when it uses more LOBS than LOB_BATCH_COUNT
  **    LOB_MIN_SIZE    : If a String or Buffer is mapped to a CLOB or a BLOB then it will be inserted using a LOB if it exceeeds this value.
  **    LOB_CACHE_COUNT  : A Batch will be regarded as complete when the number of CACHED (String & Buffer) LOBs exceeds this value.
  **
  ** The amount of client side memory required to manage the LOB Cache is approx LOB_MIN_SIZE * LOB_CACHE_COUNT
  **
  */

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
    this.lobList = [];
    this.lobBatch = []
    this.tempLpbCount = 0;
    this.cachedLobCount = 0;
    this.dumpOracleTestcase = false;
    this.lobCumlativeTime = 0;
	
    if (this.dbi.dbVersion < 12) {
      this.WKX = require('wkx') 
    }
    
	// Set up an Array of Transformation functions to be applied to the incoming rows
	
    this.transformations = this.tableInfo.dataTypes.map(function(dataType,idx) {          
      switch (dataType.type) {            
        case "GEOMETRY":
          // Metadata based decision
          if ((this.dbi.dbVersion < 12) && (this.tableInfo.spatialFormat === 'GeoJSON')) {
            // GeoJSON not supported by SDO_UTIL in 11.x database
            return function(col,jdx) {
              return this.WKX.Geometry.parseGeoJSON(col).toWKT();
            }
          }
          else {
            return null
          }
          break;
        case "JSON":
          return function(col,jdx) {
            // JSON store as BLOB results in Error: ORA-40479: internal JSON serializer error during export operations
            // row[idx] = Buffer.from(JSON.stringify(row[idx]))
            // Default JSON Storage model is JSON store as CLOB.
            // JSON must be shipped in Serialized Form
            if (typeof col === 'object') {
              return JSON.stringify(col)
            } 
            else {
              return col
            }
          }
          break;
        case "RAW":
          return function(col,jdx) {
            if (typeof col === 'boolean') {
              return  col === true ? '01' : '00'
            }
            return Buffer.from(col,'hex');
          }
          break;
        case "BOOLEAN":
          return function(col,jdx) {
            switch (col) {
              case true:
                 return 'true';
              case false:
                 return 'false';
              default:
                return col;
            }
		  }
          break;
        case "DATE":
          return function(col,jdx) { 
            if (col instanceof Date) {
              return col.toISOString()
            }
            return col;
          }
          break;
        case "TIMESTAMP":
          return function(col,jdx) { 
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
          return function(col,jdx) { 
            // Cannot passs XMLTYPE as BUFFER
            // Reason: ORA-06553: PLS-307: too many declarations of 'XMLTYPE' match this call
            // bindRow[idx] = Buffer.from(col);
            return col
          }
          break;
        default :
          return null
      }
    },this)
 
    this.lobTransformations = new Array(this.tableInfo.binds.length).fill(null);
	
	if (this.tableInfo.lobColumns === true) {     
	  this.lobTransformations = this.tableInfo.lobBinds.map(function(lobBind) {
        switch (lobBind.type) {
          case oracledb.CLOB:
		    return function(col,self) {
              // Determine whether to bind content as string or temporary CLOB
              if (typeof col !== "string") {
                return JSON.stringify(col);
              }
              self.cachedLobCount++
              self.bindRowAsLob = self.bindRowAsLob || (Buffer.byteLength(col,'utf8') > self.dbi.parameters.LOB_MIN_SIZE) 
			  return col;
		    }
		    break;
          case oracledb.BLOB:
		    return function(col,self) {
              // Determine whether to bind content as Buffer or temporary BLOB
              if ((typeof col === "string") || ((col.length/2) <= self.dbi.parameters.LOB_MIN_SIZE)) {
                col = Buffer.from(col,'hex')
              }
              self.cachedLobCount++
              self.bindRowAsLob = self.bindRowAsLob || (col.length > self.dbi.parameters.LOB_MIN_SIZE)  
			  return col
			}
            break
          default:
		    return null;
	    }
      },this)
    }
  }

  async initialize() {
    await this.disableTriggers()
  }

  async finalize() {
    const results = await super.finalize()
    await this.enableTriggers();
    return results;
  }

  async disableTriggers() {
  
    const sqlStatement = `ALTER TABLE "${this.schema}"."${this.tableName}" DISABLE ALL TRIGGERS`;
    return this.dbi.executeSQL(sqlStatement,[]);
    
  }

  async enableTriggers() {
    
    const sqlStatement = `ALTER TABLE "${this.schema}"."${this.tableName}" ENABLE ALL TRIGGERS`;
    return this.dbi.executeSQL(sqlStatement,[]);
    
  }

  async appendRow(row) {
      
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
          
    // if ( (this.batch.length + this.lobBatch.length) === 0 ) {console.log(row)}

    try {
      this.bindRowAsLob = false;
	  if (this.tableInfo.lobColumns) {
		// Bind Ordering and Row Ordering are the probably different. Use map to create a new array in BindOrdering when applying transformations
	    row = this.transformations.map(function (transformation,bindIdx) {
          const rowIdx = this.tableInfo.bindOrdering[bindIdx]
		  if (row[rowIdx] !== null) {			     
            if (transformation !== null) {
			  row[rowIdx] = transformation(row[rowIdx]);
			}
   	        if (this.lobTransformations[bindIdx] !== null) {
  		      row[rowIdx] = this.lobTransformations[bindIdx](row[rowIdx],this);		      
		    }
		  } 			
		  return row[rowIdx]
		},this)		
	  } 
	  else {
		// Bind Ordering and Row Ordering are the same. Apply transformations directly to ROW where required
	    this.transformations.forEach(function (transformation,idx) {
          if ((transformation !== null) && (row[idx] !== null)) {			
	        row[idx] = transformation(row[idx])
          }
	    },this)
      }
	  
	  // Row is now in bindOrdering. Convert CLOB and BLOB to temporaryLOBs where necessary

      if (this.bindRowAsLob) {
		if ((this.batch.length + this.lobBatch.length) === 0)  console.log('Binding LOBS');
	    // Use map combined with Promise.All to convert columns to temporaryLobs
	    const lobStartTime = performance.now();
        row = await Promise.all(this.tableInfo.binds.map(function(bind,idx){
          if (row[idx] !== null) {
            switch (bind.type) {
              case oracledb.CLOB:
                this.templobCount++
                this.cachedLobCount--
                return this.dbi.trackClobFromString(row[idx], this.lobList)                                                                    
                break;
              case oracledb.BLOB:
                this.templobCount++  
                this.cachedLobCount--
                if (typeof row[idx] === 'string') {
                  return this.dbi.trackBlobFromHexBinary(row[idx], this.lobList)                                                                    
                }
                else {
                  return this.dbi.trackBlobFromBuffer(row[idx], this.lobList)
                }
                break;
              default:
                return row[idx]
            }
          }
          return null;
        },this))
		this.lobCumlativeTime = this.lobCumlativeTime + (performance.now() - lobStartTime);
        this.lobBatch.push(row);
      }
      else {
        this.batch.push(row);
      }
      return this.batch.length + this.lobBatch.length;
    } catch (e) {
      const errInfo = this.status.showInfoMsgs ? [this.tableInfo.dml,this.tableInfo.targetDataTypes,JSON.stringify(this.tableInfo.binds)] : []     
      this.dbi.handleInsertError(`${this.constructor.name}.apppendRow()`,this.tableName,this.batch.length+this.lobBatch.length,-1,row,e,errInfo);
    }
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
      
    return (((this.batch.length + this.lobBatch.length) === this.tableInfo.batchSize) || (this.tempLobCount >= this.dbi.parameters.BATCH_LOB_COUNT) || (this.cachedLobCount > this.dbi.parameters.LOB_CACHE_COUNT))
   
  }
   
  hasPendingRows() {
    return (this.batch.length + this.lobBatch.length) > 0
  }
  /*
  **
  ** Temporary LOB optimization via LOB column resuse disabled until oracledb supports reusing temporary lobs  
  ** 
  ** freeLobList() {
  **   return this.lobList.map(function(lob,idx) {
  **     return lob.trunc(0);
  **   },this)
  ** }
  ** 
  ** freeLobList() {
  **   let lobCount = 0;
  **   this.lobList.forEach(async function(lob,idx) {
  **     try {
  **       await lob.close();
  **       lobCount++;
  **     } catch(e) {
  **       this.yadamuLogger.logException([`${this.constructor.name}.freeLobList()`,`${this.tableName}`,`${idx}`],e);
  **     }   
  **   },this)
  **   if (lobCount > 0) {
  **     this.yadamuLogger.info([`${this.constructor.name}.freeLobList()`,`${this.tableName}`],`Closed ${lobCount} lobs.`); 
  **   }
  ** }
  **
  */
  
  freeLobList() {
  }
    
  async serializeLobs(record) {
    const newRecord = await Promise.all(this.tableInfo.binds.map(function(bind,idx) {
      if (record[idx] !== null) {
        switch (bind.type) {
          case oracledb.CLOB:
            // console.log(record[idx])
            // ### Cannot re-read content that has been written to local clob
            // return this.dbi.stringFromClob(record[idx])
            return typeof record[idx] === 'string' ? record[idx] : this.dbi.stringFromLocalClob(record[idx])
          case oracledb.BLOB:
            // console.log(record[idx])
            // ### Cannot re-read content that has been written to local blob
            // return this.dbi.hexBinaryFromBlob(record[idx])
            return this.dbi.hexBinaryFromLocalBlob(record[idx])
          default:
            return record[idx];
        }
      }
      return record[idx];
    },this))
    return newRecord;
  }   
      
  
  resetBatch() {
    this.batch.length = 0;
    this.lobBatch.length = 0;
    
    this.cachedLobCount = 0;
    this.tempLobCount = 0;
  }
  
  async writeBatch() {
      
    // Ideally we used should reuse tempLobs since this is much more efficient that setting them up, using them once and tearing them down.
    // Infortunately the current implimentation of the Node Driver does not support this, once the 'finish' event is emitted you cannot truncate the tempCLob and write new content to it.
    // So we have to free the current tempLob Cache and create a new one for each batch

    this.batchCount++;
    let lobInsert = undefined
    let rows = undefined;
    let binds = undefined;

    if (this.insertMode === 'Batch') {
      try {
        rows = this.batch
        binds = this.tableInfo.binds
        lobInsert = false
        await this.dbi.createSavePoint()
        const results = await this.dbi.executeMany(this.tableInfo.dml,rows,{bindDefs : binds});
        if (this.lobBatch.length > 0) {
          lobInsert = true
          rows = this.lobBatch
          binds = this.tableInfo.lobBinds
          const results = await this.dbi.executeMany(this.tableInfo.dml,rows,{bindDefs : binds});
          // await Promise.all(this.freeLobList());
          this.freeLobList();
        }         
        this.endTime = performance.now();
        this.resetBatch();
        return this.skipTable
      } catch (e) {
        await this.dbi.restoreSavePoint();
        if (e.errorNum && (e.errorNum === 4091)) {
          // Mutating Table - Convert to Cursor based PL/SQL Block
          if (this.status.showInfoMsgs) {
            this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`,`${rows.length}`,`${lobInsert ? 'LOB' : 'Normal'}`],`executeMany() operation raised:\n${e}`);
            this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
            this.yadamuLogger.writeDirect(`${this.tableInfo.targetDataTypes}\n`);
            this.yadamuLogger.writeDirect(`${JSON.stringify(binds)}\n`);
            this.yadamuLogger.writeDirect(`${JSON.stringify(rows[0])}\n...\n${JSON.stringify(rows[rows.length-1])}\n`);
            this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to PL/SQL Block.`);          
          }
          this.tableInfo.dml = this.avoidMutatingTable(this.tableInfo.dml);
          try {
            rows = this.batch
            binds = this.tableInfo.binds
            lobInsert = false
            await this.dbi.createSavePoint()
            const results = await this.dbi.executeMany(this.tableInfo.dml,rows,{bindDefs : binds});
            if (this.lobBatch.length > 0) {
              rows = this.lobBatch
              binds = this.tableInfo.lobBinds
              lobInsert = true
              const results = await this.dbi.executeMany(this.tableInfo.dml,rows,{bindDefs : binds});
              // await Promise.all(this.freeLobList());
              this.freeLobList();
            }         
            this.endTime = performance.now();
            this.resetBatch();
            return this.skipTable
          } catch (e) {
            await this.dbi.restoreSavePoint();
            if (this.status.showInfoMsgs) {
              this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`,`${rows.length}`,`${lobInsert ? 'LOB' : 'Normal'}`],`executeMany() with PL/SQL block raised:\n${e}`);
              this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
              this.yadamuLogger.writeDirect(`${this.tableInfo.targetDataTypes}\n`);
              this.yadamuLogger.writeDirect(`${JSON.stringify(binds)}\n`);
              this.yadamuLogger.writeDirect(`${JSON.stringify(rows[0])}\n...\n${JSON.stringify(rows[rows.length-1])}\n`);
              this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to PL/SQL Block.`);          
              this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to Iterative operations.`);          
            }
            this.insertMode = 'Iterative';
          }
        } 
        else {  
        console.log(e)
          if (this.status.showInfoMsgs) {
            this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`,`${rows.length}`,`${lobInsert ? 'LOB' : 'Normal'}`],`executeMany() operation raised:\n${e}`);
            this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
            this.yadamuLogger.writeDirect(`${this.tableInfo.targetDataTypes}\n`);
            this.yadamuLogger.writeDirect(`${JSON.stringify(binds)}\n`);
            this.yadamuLogger.writeDirect(`${JSON.stringify(rows[0])}\n...\n${JSON.stringify(rows[rows.length-1])}\n`);
            this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to Iterative operations.`);          
            if (this.dumpOracleTestcase) {
              console.log('DDL:')
              console.log(this.tableInfo.ddl)
              console.log('DML:')
              console.log(this.tableInfo.dml)
              console.log('BINDS:')
              console.log(JSON.stringify(binds));
              console.log('DATA:');
              console.log(JSON.stringify(rows.slice(0,9)));
            }
          }
          this.insertMode = 'Iterative';
        }
      }
    }

    if (this.lobBatch.length > 0) {
      if (this.batch.length > this.lobBatch.length) {
        this.batch.forEach(function(row) {
          this.lobBatch.push(row);
        },this)
        rows = this.lobBatch;
      } 
      else {
        this.lobBatch.forEach(function(row) {
          this.batch.push(row);
        },this);
        rows = this.batch;
      }
    }
    else {
      rows = this.batch;
    }

    for (const row in rows) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,rows[row])
      } catch (e) {
        const record = await this.serializeLobs(rows[row])
        const errInfo = this.status.showInfoMsgs ? [this.tableInfo.dml,this.tableInfo.targetDataTypes,JSON.stringify(record)] : []
        this.skipTable = await this.dbi.handleInsertError(`${this.constructor.name}.writeBatch()`,this.tableName,rows.length,row,record,e,errInfo);
        if (this.skipTable) {
          break;
        }
      }
    } 
    // ### Iterative must commit to allow a subsequent batch to rollback.
    this.endTime = performance.now();
    this.resetBatch();
    // await Promise.all(this.freeLobList());
    this.freeLobList();
    return this.skipTable     
  }
  
  async finalize() {
	const tableStats = await super.finalize();
	tableStats.sqlTime = tableStats.sqlTime + this.lobCumlativeTime;
    return tableStats;  
  }
}

module.exports = TableWriter;
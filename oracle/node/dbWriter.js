"use strict";
const Writable = require('stream').Writable
const Readable = require('stream').Readable;

const oracledb = require('oracledb');
oracledb.fetchAsString = [ oracledb.DATE ]

const Yadamu = require('../../common/yadamuCore.js');
const OracleCore = require('./oracleCore.js');
const StatementGenerator = require('./statementGenerator');

const EXPORT_VERSION = 1.0;
const DATABASE_VENDOR = 'Oracle';

let OPTIONS = {
  IDENTIFIER_CASE : null,
  DISABLE_TRIGGERS : true
}

class DBWriter extends Writable {
  
  constructor(conn,schema,batchSize,commitSize,lobCacheSize,mode,status,logWriter,options) {

    super({objectMode: true });
    const self = this;
    
    this.conn = conn;
    this.schema = schema;
    this.batchSize = batchSize;
    this.commitSize = commitSize;
    this.lobCacheSize = lobCacheSize;
    this.mode = mode;
    this.ddlRequired = (mode !== 'DATA_ONLY');    
    this.status = status;
    this.logWriter = logWriter;
    this.logWriter.write(`${new Date().toISOString()}[DBWriter ${DATABASE_VENDOR}]: Ready. Mode: ${this.mode}.\n`)
    
    this.batch = [];
    this.lobList = [];
    
    this.systemInformation = undefined;
    this.metadata = undefined;

    this.statementCache = undefined

    this.tableName  = undefined;
    this.tableInfo  = undefined;
    this.rowCount   = undefined;
    this.startTime  = undefined;
    this.insertMode = 'Empty';
    this.skipTable = true;
    
    this.logDDLIssues   = (status.loglevel && (status.loglevel > 2));
    this.logDDLIssues   = true;
    
    this.statementGenerator = new StatementGenerator(conn, status, logWriter);    
    
    this.sqlSetSavePoint = `SAVEPOINT BATCH_INSERT`;
    this.sqlRollbackSavePoint = `ROLLBACK TO BATCH_INSERT`;

  }      
  
  objectMode() {
    return true; 
  }
  
  setOptions(options) {
    OPTIONS = options
  }
     
  setTable(tableName) {
    switch (OPTIONS.IDENTIFIER_CASE) {
       case 'UPPER':
         this.tableName = tableName.toUpperCase();
         break;
       case 'LOWER':
         this.tableName = tableName.toLowerCase();
         break;         
      default: 
        this.tableName = tableName;
    }             
      
    this.tableInfo =  this.statementCache[this.tableName];
    if (this.tableInfo.lobCount > 0) {
      // If some columns are bound as CLOB or BLOB restrict batchsize based on lobCacheSize
      let lobBatchSize = Math.floor(this.lobCacheSize/this.tableInfo.lobCount);
      this.batchSize = (lobBatchSize > this.batchSize) ? this.batchSize : lobBatchSize;
    }
    this.batchCount = 0;
    this.rowCount = 0;
    this.lobUsage = 0;
    this.batch.length = 0;
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.skipTable = false;
  }
  
  async disableConstraints(tableName) {
  
    const sqlStatement = `begin :log := JSON_IMPORT.DISABLE_CONSTRAINTS(:schema); end;`;
    
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement}\n/\n`);
    }
     
    try {
      const results = await this.conn.execute(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , schema:this.schema});
      const log = JSON.parse(results.outBinds.log);
      if (log !== null) {
        Yadamu.processLog(log, this.status, this.logWriter)
      }
    } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
    }    
  }
  
  async enableConstraints(tableName) {
  
    const sqlStatement = `begin :log := JSON_IMPORT.ENABLE_CONSTRAINTS(:schema); end;`;
     
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement}\n/\n`);
    }
    
    try {
      const results = await this.conn.execute(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , schema:this.schema});
      const log = JSON.parse(results.outBinds.log);
      if (log !== null) {
        Yadamu.processLog(log, this.status, this.logWriter)
      }
    } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
    }    
  }
  
  async disableTriggers(schema,tableName) {
  
    const sqlStatement = `ALTER TABLE "${schema}"."${tableName}" DISABLE ALL TRIGGERS`;
     
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement}\n/\n`);
    }
    
    try {
      const results = await this.conn.execute(sqlStatement);
   } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
    }    
  }
  
  async enableTriggers(schema,tableName) {
  
    const sqlStatement = `ALTER TABLE "${schema}"."${tableName}" ENABLE ALL TRIGGERS`;
     
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${sqlStatement}\n/\n`);
    }
    
    try {
      const results = await this.conn.execute(sqlStatement);
    } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
    }    
  }
  
  async refreshMaterializedViews() {
  
    const sqlStatement = `begin :log := JSON_IMPORT.REFRESH_MATERIALIZED_VIEWS(:schema); end;`;
     
    try {
      const results = await this.conn.execute(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , schema:this.schema});
      const log = JSON.parse(results.outBinds.log);
      if (log !== null) {
        Yadamu.processLog(log, this.status, this.logWriter)
      }
    } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
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
        statementSeperator = "\n	     values (:1";
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

  string2Clob(conn,str,lobList) {
      
    const s = new Readable();
    s.push(str);
    s.push(null);

    return new Promise(async function(resolve,reject) {
      try {
        let tempLob = undefined;
        tempLob = await conn.createLob(oracledb.CLOB);
        lobList.push(tempLob);
        tempLob.on('error',function(err) {reject(err);});
        tempLob.on('finish', function() {resolve(tempLob)});
        s.on('error', function(err) {reject(err);});
        s.pipe(tempLob);  // copies the text to the temporary LOB
      }
      catch (e) {
        reject(e);
      }
    });  
  }
      
  freeLobList() {
    this.lobList.forEach(async function(lob,idx) {
      try {
        await lob.close();
      } catch(e) {
        this.logWriter.write(`LobList[${idx}]: Error ${e}\n`);
      }   
    },this)
  }
      
  
  async writeBatch(status) {
      
    // Ideally we used should reuse tempLobs since this is much more efficient that setting them up, using them once and tearing them down.
    // Infortunately the current implimentation of the Node Driver does not support this, once the 'finish' event is emitted you cannot truncate the tempCLob and write new content to it.
    // So we have to free the current tempLob Cache and create a new one for each batch
    
    try {
      this.insertMode = 'Batch';
      if (status.sqlTrace) {
        status.sqlTrace.write(`${this.sqlSetSavePoint}\n/\n`);
      }
      let results = await this.conn.execute(this.sqlSetSavePoint);
      if (status.sqlTrace) {
        status.sqlTrace.write(`${this.tableInfo.dml}\n/\n`);
      }
      this.batchCount++;
      results = await this.conn.executeMany(this.tableInfo.dml,this.batch,{bindDefs : this.tableInfo.binds});
      const endTime = new Date().getTime();
      // console.log(`Batch:${batchCount}. ${this.batch.length} rows inserted`)
      this.batch.length = 0;
      this.freeLobList();
      return endTime
    } catch (e) {
      if (status.sqlTrace) {
        status.sqlTrace.write(`${this.sqlRollbackSavePoint}\n/\n`);
      }
      let results = await this.conn.execute(this.sqlRollbackSavePoint);
      if (e.errorNum && (e.errorNum === 4091)) {
        // Mutating Table - Convert to Cursor based PL/SQL Block
        status.warningRaised = true;
        this.logWriter.write(`${new Date().toISOString()} [INFO]: Table ${this.tableName}. executeMany(${this.batch.length})) failed. ${e}. Retrying with PL/SQL Block.\n`);
        this.tableInfo.dml = this.avoidMutatingTable(this.tableInfo.dml);
        if (status.sqlTrace) {
          status.sqlTrace.write(`${this.tableInfo.dml}\n/\n`);
        }
        try {
          const results = await this.conn.executeMany(this.tableInfo.dml,this.batch,{bindDefs : this.tableInfo.binds}); 
          const endTime = new Date().getTime();
          this.batch.length = 0;
          return endTime
        } catch (e) {
          await this.conn.rollback();
          if (this.logDDLIssues) {
            this.logWriter.write(`${new Date().toISOString()} [WARNING]: Table (${this.tableName}. executeMany(${this.batch.length}) failed. ${e}. Retrying using execute() loop.\n`);
            this.logWriter.write(`${this.tableInfo.dml}\n`);
            this.logWriter.write(`${this.tableInfo.targetDataTypes}\n`);
            this.logWriter.write(`${JSON.stringify(this.tableInfo.binds)}\n`);
            this.logWriter.write(`${JSON.stringify(this.batch[0])}\n`);
          }
        }
      } 
      else {  
        if (this.logDDLIssues) {
          this.logWriter.write(`${new Date().toISOString()} [WARNING]: Table (${this.tableName}. executeMany(${this.batch.length}) failed. ${e}. Retrying using execute() loop.\n`);
          this.logWriter.write(`${this.tableInfo.dml}\n`);
          this.logWriter.write(`${this.tableInfo.targetDataTypes}\n`);
          this.logWriter.write(`${JSON.stringify(this.tableInfo.binds)}\n`);
          this.logWriter.write(`${JSON.stringify(this.batch[0])}\n`);
        }
      }
    }

    let row = undefined;
    this.insertMode = 'Iterative';
    for (row in this.batch) {
      try {
        let results = await this.conn.execute(this.tableInfo.dml,this.batch[row])
      } catch (e) {
        this.logWriter.write(`${new Date().toISOString()} [ERROR]: Table (${this.tableName}. insert(${row}) failed. Reason: ${e.message}\n`)
        this.status.warningRaised = true;
        if (this.logDDLIssues) {
          this.logWriter.write(`${this.tableInfo.dml}\n`);
          this.logWriter.write(`${this.tableInfo.targetDataTypes}\n`);
          this.logWriter.write(`${JSON.stringify(this.batch[row])}\n`);
        } 
        // Write Record to 'bad' file.
        try {
          if ( this.status.importErrorMgr ) {
            this.status.importErrorMgr.logError(this.tableName,this.batch[row]);
          }
          else {
            this.logWriter.write(`${new Date().toISOString()} [ERROR]: Data [${this.batch[row]}].\n`)               
          }
        } catch (e) {
        //  Catch Max Errors Exceeded Assertion
          await this.conn.rollback();
          this.skipTable = true;
          this.logWriter.write(`${new Date().toISOString()} [ERROR]: Table ${this.tableName}. Skipping table. Row ${row}. Reason: ${e.message}\n`)
        }
      }
    } 
    // console.log(`Iterative:${this.batchCount}. ${this.batch.length} rows inserted`)
    // Iterative must commit to allow a subsequent batch to rollback.
    const endTime = new Date().getTime();
    this.batch.length = 0;
    this.freeLobList();
    return endTime
        
  }

  async _write(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
        case 'systemInformation':
          this.systemInformation = obj.systemInformation;
          await OracleCore.setDateFormatMask(this.conn,this.status,this.systemInformation.vendor);
          break;
        case 'ddl':
          if (this.ddlRequired) {
            await this.statementGenerator.executeDDL(this.schema, this.systemInformation, obj.ddl);
            this.ddlRequired = false;
          }
          break;
        case 'metadata':
          this.metadata = Yadamu.convertIdentifierCase(OPTIONS.IDENTIFIER_CASE,obj.metadata);
          const targetTableInfo = await OracleCore.getTableInfo(this.conn,this.schema,this.status);
          if (targetTableInfo.length > 0) {
             this.metadata = Yadamu.mergeMetadata(OracleCore.generateMetadata(targetTableInfo,false),this.metadata);
          }
          if (Object.keys(this.metadata).length > 0) {   
            this.statementCache = await this.statementGenerator.generateStatementCache(this.schema,this.systemInformation,this.metadata)
          } 
          break;
        case 'table':
          // this.logWriter.write(`${new Date().toISOString()}: Switching to Table "${obj.table}".\n`);
          if (this.tableName === undefined) {
            // First Table - Disable Constraintsf
            await this.disableConstraints();
          }
          else {
            if (this.batch.length > 0) {
              // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batch.length} rows.`);
              this.endTime = await this.writeBatch(this.status);
            }  
            if (OPTIONS.DISABLE_TRIGGERS === true) {
               await this.enableTriggers(this.schema,this.tableName)
            }
            if (!this.skipTable) {
              await this.conn.commit();
              const elapsedTime = this.endTime - this.startTime;            
              this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"][${this.insertMode}]: Rows written ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
            }
          }
          this.setTable(obj.table);
          if (this.status.sqlTrace) {
            this.status.sqlTrace.write(`${this.tableInfo.dml}\n\/\n`)
	      }
          if (OPTIONS.DISABLE_TRIGGERS === true) {
            await this.disableTriggers(this.schema,this.tableName)
          }
          break;
        case 'data': 
          if (this.skipTable) {
            break;
          }
          obj.data = await Promise.all(this.tableInfo.targetDataTypes.map(function(targetDataType,idx) {
            if (obj.data[idx] !== null) {
              const dataType = Yadamu.decomposeDataType(targetDataType);
              if (dataType.type === 'JSON') {
                // JSON store as BLOB results in Error: ORA-40479: internal JSON serializer error during export operations
                // obj.data[idx] = Buffer.from(JSON.stringify(obj.data[idx]))
                // Default JSON Storage model is JSON store as CLOB.
                // JSON must be shipped in Serialized Form
                return JSON.stringify(obj.data[idx])
              } 
              if (this.tableInfo.binds[idx].type === oracledb.CLOB) {
                this.lobUsage++
                // A promise...
                return this.string2Clob(this.conn, obj.data[idx],this.lobList)                                                                    
              }
              switch (dataType.type) {
                case "BLOB" :
                  return Buffer.from(obj.data[idx],'hex');
                case "RAW":
                  return Buffer.from(obj.data[idx],'hex');
                case "BOOLEAN":
                  switch (obj.data[idx]) {
                    case true:
                       return 'true';
                       break;
                    case false:
                       return 'false';
                       break;
                    default:
                      return obj.data[idx]
                  }
                case "DATE":
                  if (obj.data[idx] instanceof Date) {
                    return obj.data[idx].toISOString()
                  }
                case "TIMESTAMP":
                  // A Timestamp not explicitly marked as UTC should be coerced to UTC.
                  // Avoid Javascript dates due to lost of precsion.
                  // return new Date(Date.parse(obj.data[idx].endsWith('Z') ? obj.data[idx] : obj.data[idx] + 'Z'));
                  if (typeof obj.data[idx] === 'string') {
                    return (obj.data[idx].endsWith('Z') || obj.data[idx].endsWith('+00:00')) ? obj.data[idx] : obj.data[idx] + 'Z';
                  }
                  if (obj.data[idx] instanceof Date) {
                    return obj.data[idx].toISOString()
                  }
                case "XMLTYPE" :
                  // Cannot passs XMLTYPE as BUFFER
                  // Reason: ORA-06553: PLS-307: too many declarations of 'XMLTYPE' match this call
                  // obj.data[idx] = Buffer.from(obj.data[idx]);
                default :
                  return obj.data[idx]
              }
            }
          },this))
          this.batch.push(obj.data);
          this.rowCount++;
          // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Batch contains ${this.batch.length} rows.`);
          if (this.batch.length === this.batchSize) { 
              // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Completed Batch contains ${this.batch.length} rows.`);
             this.endTime = await this.writeBatch(this.status);
          }  
          if ((this.rowCount % this.commitSize) === 0) {
             await this.conn.commit();
             // const elapsedTime = this.endTime - this.startTime;
             // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Commit after Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          }
          break;
        default:
      }    
      callback();
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}[DBWriter._write()() "${this.tableName}"]: ${e}\n${e.stack}\n`);
      callback(e);
    }
  }
 
  async _final(callback) {
    try {
      if (this.tableName) {
        if (!this.skipTable) {
          if (this.batch.length > 0) {
            // this.logWriter.write(`${new Date().toISOString()}:_final() Table "${this.tableName}". Final Batch contains ${this.batch.length} rows.`);
           this.endTime = await this.writeBatch(this.status);
          }   
          const elapsedTime = this.endTime - this.startTime;
          this.logWriter.write(`${new Date().toISOString()}[DBWriter "${this.tableName}"][${this.insertMode}]: Rows written ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
        }
        if (OPTIONS.DISABLE_TRIGGERS === true) {
          await this.enableTriggers(this.schema,this.tableName)
        }
        await this.enableConstraints();
        await this.conn.commit();
        await this.refreshMaterializedViews();
      }
      else {
        this.logWriter.write(`${new Date().toISOString()}: No tables found.\n`);
      }  
      callback();
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}[DBWriter._final() "${this.tableName}"]: ${e}\n${e.stack}\n`);
      callback(e);
    } 
  } 
}

module.exports = DBWriter;
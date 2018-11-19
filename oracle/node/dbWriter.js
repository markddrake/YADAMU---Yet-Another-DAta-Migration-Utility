"use strict";
const oracledb = require('oracledb');
const Writable = require('stream').Writable
const Readable = require('stream').Readable;

const Yadamu = require('../../common/yadamuCore.js');
const StatementGenerator = require('./statementGenerator');

class DBWriter extends Writable {
  
  constructor(conn,schema,batchSize,commitSize,lobCacheSize,mode,status,logWriter,options) {

    super({objectMode: true });
       
    const dbWriter = this;
    
    this.conn = conn;
    this.schema = schema;
    this.batchSize = batchSize;
    this.commitSize = commitSize;
    this.lobCacheSize = lobCacheSize;
    this.ddlRequired = (mode !== 'DATA_ONLY');    
    this.status = status;
    this.logWriter = logWriter;
    
    this.batch = [];
    this.lobCache = [];

    this.systemInformation = undefined;
    this.metadata = undefined;

    this.statementCache = undefined

    this.tableName  = undefined;
    this.tableInfo  = undefined;
    this.rowCount   = undefined;
    this.lobUsage   = undefined;
    this.startTime  = undefined;
    this.insertMode = 'Empty';
    this.skipTable = true;
    
    this.logDDLIssues   = (status.loglevel && (status.loglevel > 2));
    this.logDDLIssues   = true;
    
    this.statementGenerator = new StatementGenerator(conn, status, logWriter);    
  }      
  
  async setTable(tableName) {
    this.tableName = tableName
    this.tableInfo =  this.statementCache[tableName];
    if (this.tableInfo.lobIndexList.length > 0) {
      // If some columns are bound as tempCLOB restrict batchsize based on lobCacheSize
      let lobBatchSize = Math.floor(this.lobCacheSize/this.tableInfo.lobIndexList.length);
      this.batchSize = (lobBatchSize > this.batchSize) ? this.batchSize : lobBatchSize;
    }
    this.rowCount = 0;
    this.lobUsage = 0;
    this.batch.length = 0;
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.skipTable = false;
  }
  
  async disableConstraints() {
  
    const sqlStatement = `begin :log := JSON_IMPORT.DISABLE_CONSTRAINTS(:schema); end;`;
     
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
  
  async enableConstraints() {
  
    const sqlStatement = `begin :log := JSON_IMPORT.ENABLE_CONSTRAINTS(:schema); end;`;
     
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

  stringToLob(conn,str,lobCache,lobCacheIndex) {

    const s = new Readable();
    s.push(str);
    s.push(null);

    return new Promise(async function(resolve,reject) {
      try {
        let tempLob = undefined;
        /*
        if (lobCacheIndex >= lobCache.length) {
          tempLob = await  conn.createLob(oracledb.CLOB);
          lobCache.push(tempLob);
        }
        else {
          tempLob = lobCache[lobCacheIndex];
          // tempLob.truncate(0);
          await conn.execute('begin DBMS_LOB.trim(:1,0); end;',[tempLob]);
        }
        */
        tempLob = await  conn.createLob(oracledb.CLOB);
        lobCache.push(tempLob);
        tempLob.on('error',function(err) {reject(err);});
        tempLob.on('finish', function() {resolve(tempLob)});
        s.on('error', function(err) {reject(err);});
        s.pipe(tempLob);  // copies the text to the temporary LOB
      }
      catch (e) {
        console.log('Oops');
        reject(e);
      }
    });  
  }
      
  
  async clearLobCache() {
    this.lobCache.forEach(async function(lob) {
                            try {
                              await lob.close();     
                            } catch (e) {
                              this.logWriter.write(`Error closing LOB: ${e}\n${e.stack}\n`);
                            }                        
    },this)
    this.lobCache.length = 0;
    return this.lobCache.length;
  }
    
  async writeBatch(status) {
      
    // Ideally we used should reuse tempLobs since this is much more efficient that setting them up, using them once and tearing them down.
    // Infortunately the current implimentation of the Node Driver does not support this, once the 'finish' event is emitted you cannot truncate the tempCLob and write new content to it.
    // So we have to free the current tempLob Cache and create a new one for each batch
    
    if (!this.tableInfo.containsObjects) {
      try {
        this.insertMode = 'Batch';
        const results = await this.conn.executeMany(this.tableInfo.dml,this.batch,{bindDefs : this.tableInfo.binds});
        const endTime = new Date().getTime();
        this.batch.length = 0;
        this.lobUsage = await this.clearLobCache();
        return endTime
      } catch (e) {
        await this.conn.rollback();
         if (e.errorNum && (e.errorNum === 4091)) {
          // Mutating Table - Convert to PL/SQL Block
          status.warningRaised = true;
          this.logWriter.write(`${new Date().toISOString()} [WARNING]: Table ${this.tableName} : executeMany(INSERT) failed. ${e}. Retrying with PL/SQL Block.\n`);
          this.tableInfo.dml = this.avoidMutatingTable(this.tableInfo.dml);
          if (status.sqlTrace) {
            status.sqlTrace.write(`${this.tableInfo.dml}\n/\n`);
          }
          try {
            const results = await this.conn.executeMany(this.tableInfo.dml,this.batch,{bindDefs : this.tableInfo.binds});
            const endTime = new Date().getTime();
            this.batch.length = 0;
            this.lobUsage = await this.clearLobCache();
            return endTime
          } catch (e) {
            await this.conn.rollback();
            if (this.logDDLIssues) {
              this.logWriter.write(`${new Date().toISOString()}:_write(${this.tableName},${this.batch.length}) : executeMany() failed. ${e}. Retrying using execute() loop.\n`);
              this.logWriter.write(`${this.tableInfo.dml}\n`);
              this.logWriter.write(`${this.tableInfo.targetDataTypes}\n`);
              this.logWriter.write(`${JSON.stringify(this.tableInfo.binds)}\n`);
            }
          }
        } 
        else {  
          if (this.logDDLIssues) {
            this.logWriter.write(`${new Date().toISOString()}:_write(${this.tableName},${this.batch.length}) : executeMany() failed. ${e}. Retrying using execute() loop.\n`);
            this.logWriter.write(`${this.tableInfo.dml}\n`);
            this.logWriter.write(`${this.tableInfo.targetDataTypes}\n`);
            this.logWriter.write(`${JSON.stringify(this.tableInfo.binds)}\n`);
          }
        }
      }
    }

    let row = undefined;
    this.insertMode = 'Iterative';
    try {
      for (row in this.batch) {
        let results = await this.conn.execute(this.tableInfo.dml,this.batch[row])
      }
      const endTime = new Date().getTime();
      this.batch.length = 0;
      this.lobUsage = await this.clearLobCache();
      return endTime
    } catch (e) {
      await this.conn.rollback();
      this.skipTable = true;
      this.status.warningRaised = true;
      this.conn.rollback();
      this.logWriter.write(`${new Date().toISOString()}: Table ${this.tableName}. Skipping table. Row ${row}. Reason: ${e.message}\n`)
      if (this.logDDLIssues) {
        this.logWriter.write(`${this.tableInfo.dml}\n`);
        this.logWriter.write(`${this.tableInfo.targetDataTypes}\n`);
        this.logWriter.write(`${JSON.stringify(this.batch[row])}\n`);
      }
      this.batch.length = 0;
    }
  }

  async _write(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
        case 'systemInformation':
          this.systemInformation = obj.systemInformation;
          break;
        case 'ddl':
          if (this.ddlRequired) {
            await this.statementGenerator.executeDDL(this.schema, this.systemInformation, obj.ddl);
            this.ddlRequired = false;
          }
          break;
        case 'metadata':
          this.metadata = obj.metadata;
          if (Object.keys(this.metadata).length > 0) {              
            this.statementCache = await this.statementGenerator.generateStatementCache(this.schema,this.systemInformation,obj.metadata)
          } 
          break;
        case 'table':
          // this.logWriter.write(`${new Date().toISOString()}: Switching to Table "${obj.table}".\n`);
          if (this.tableName === undefined) {
            // First Table - Disable Constraints
            await this.disableConstraints();
          }
          else {
            if (this.batch.length > 0) {
              // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batch.length} rows.`);
              this.endTime = await this.writeBatch(this.status);
            }  
            if (!this.skipTable) {
              await this.conn.commit();
              const elapsedTime = this.endTime - this.startTime;            
              this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}"[${this.insertMode}]. Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
            }
          }
          this.setTable(obj.table);
          if (this.status.sqlTrace) {
            this.status.sqlTrace.write(`${this.tableInfo.dml}\n\/\n`)
	      }
          break;
        case 'data': 
          if (this.skipTable) {
            break;
          }
          this.tableInfo.targetDataTypes.forEach(async function(targetDataType,idx) {
                                                                 if (obj.data[idx] !== null) {
                                                                   if (this.tableInfo.binds[idx].type === oracledb.CLOB) {
                                                                     obj.data[idx] = await this.stringToLob(this.conn, obj.data[idx], this.lobCache, this.lobUsage)                                                                    
                                                                     this.lobUsage++
                                                                     return
                                                                   }
                                                                   const dataType = Yadamu.decomposeDataType(targetDataType);
                                                                   switch (dataType.type) {
                                                                     case "BLOB" :
                                                                       obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                                       return;
                                                                     case "RAW":
                                                                       obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                                       return;
                                                                     case "XMLTYPE" :
                                                                       // Cannot passs XMLTYPE as BUFFER
                                                                       // Reason: ORA-06553: PLS-307: too many declarations of 'XMLTYPE' match this call
                                                                       // obj.data[idx] = Buffer.from(obj.data[idx]);
                                                                       return;
                                                                     case "JSON" :
                                                                       // Default JSON Storage model is JSON store as CLOB.
                                                                       // JSON must be shipped in Serialized Form
                                                                       obj.data[idx] = JSON.stringify(obj.data[idx])
                                                                       // JSON store as BLOB results in Error: ORA-40479: internal JSON serializer error during export operations
                                                                       // obj.data[idx] = Buffer.from(JSON.stringify(obj.data[idx]))
                                                                       return;
                                                                     case "DATE":
                                                                     case "TIMESTAMP" :
                                                                       // Javascript assumes a timestamp with no timezone is in the processes time zone. 
                                                                       // There appears to be no easy way of coercing the process to UTC. 
                                                                       // A Timestamp not explicitly marked as UTC should be coerced to UTC.
                                                                       obj.data[idx] = new Date(Date.parse(obj.data[idx].endsWith('Z') ? obj.data[idx] : obj.data[idx] + 'Z'));
                                                                       return;
                                                                     default :
                                                                   }
                                                                 }
          },this)
          this.batch.push(obj.data);
          // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Batch contains ${this.batch.length} rows.`);
          if (this.batch.length === this.tableInfo.batchSize) { 
              // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Completed Batch contains ${this.batch.length} rows.`);
             this.endTime = await this.writeBatch(this.status);
          }  
          this.rowCount++;
          if ((this.rowCount % this.commitSize) === 0) {
             await this.conn.commit();
             const elapsedTime = this.endTime - this.startTime;
             // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Commit after Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          }
          break;
        default:
      }    
      callback();
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}:_write(${this.tableName}): ${e}\n${e.stack}\n`)
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
          this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}"[${this.insertMode}]. Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          await this.enableConstraints(this.conn, this.schema, this.status, this.logWriter);
          await this.clearLobCache();
          await this.conn.commit();
        }
      }
      else {
        this.logWriter.write(`${new Date().toISOString()}: No tables found.\n`);
      }  
      callback();
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}:_final(${this.tableName}): ${e}\n${e.stack}\n`)
      callback(e);
    } 
  } 
}

module.exports = DBWriter;
"use strict"

const oracledb = require('oracledb');

const sqlSetSavePoint = 
`SAVEPOINT BATCH_INSERT`;

const sqlRollbackSavePoint = 
`ROLLBACK TO BATCH_INSERT`;

class TableWriter {

  constructor(dbi,tableName,tableInfo,status,logWriter) {
    this.dbi = dbi;
    this.schema = this.dbi.parameters.TOUSER;
    this.tableName = tableName
    this.tableInfo = tableInfo;
    this.status = status;
    this.logWriter = logWriter;    

    this.batch = [];
    this.lobList = [];

    this.lobUsage = 0;
    this.batchCount = 0;
    this.batchLength = 0;

    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.insertMode = 'Batch';

    this.skipTable = false;
  }

  async disableTriggers() {
  
    const sqlStatement = `ALTER TABLE "${this.schema}"."${this.tableName}" DISABLE ALL TRIGGERS`;
    return this.dbi.executeSQL(sqlStatement,[]);
    
  }

  async initialize() {
    await this.disableTriggers();
  }

  batchComplete() {
    return this.batch.length === this.tableInfo.batchSize;
  }
  
  commitWork(rowCount) {
    return (rowCount % this.tableInfo.commitSize) === 0;
  }
  
  async enableTriggers() {
  
    const sqlStatement = `ALTER TABLE "${this.schema}"."${this.tableName}" ENABLE ALL TRIGGERS`;
    return this.dbi.executeSQL(sqlStatement,[]);
    
  }

  async appendRow(row) {
     
    try {           
      row = await Promise.all(this.tableInfo.targetDataTypes.map(function(targetDataType,idx) {
        if (row[idx] !== null) {
          const dataType = this.dbi.decomposeDataType(targetDataType);
          if (dataType.type === 'JSON') {
            // JSON store as BLOB results in Error: ORA-40479: internal JSON serializer error during export operations
            // row[idx] = Buffer.from(JSON.stringify(row[idx]))
            // Default JSON Storage model is JSON store as CLOB.
            // JSON must be shipped in Serialized Form
            if (typeof row[idx] === 'object') {
              return JSON.stringify(row[idx])
            }
            else {
              return row[idx]
            }
          } 
          if (this.tableInfo.binds[idx].type === oracledb.CLOB) {
            this.lobUsage++
            // A promise...
            return this.dbi.trackClobFromString(row[idx], this.lobList)                                                                    
          }
          switch (dataType.type) {
            case "BLOB" :
              return Buffer.from(row[idx],'hex');
            case "RAW":
              if (typeof row[idx] === 'boolean') {
                row[idx] = (row[idx] === true ? '01' : '00')
              }
              return Buffer.from(row[idx],'hex');
            case "BOOLEAN":
              switch (row[idx]) {
                case true:
                   return 'true';
                   break;
                case false:
                   return 'false';
                   break;
                default:
                  return row[idx]
              }
            case "DATE":
              if (row[idx] instanceof Date) {
                return row[idx].toISOString()
              }
              return row[idx]
            case "TIMESTAMP":
              // A Timestamp not explicitly marked as UTC should be coerced to UTC.
              // Avoid Javascript dates due to lost of precsion.
              // return new Date(Date.parse(row[idx].endsWith('Z') ? row[idx] : row[idx] + 'Z'));
              if (typeof row[idx] === 'string') {
                return (row[idx].endsWith('Z') || row[idx].endsWith('+00:00')) ? row[idx] : row[idx] + 'Z';
              } 
              if (row[idx] instanceof Date) {
                return row[idx].toISOString()
              }
              return row[idx]
            case "XMLTYPE" :
              // Cannot passs XMLTYPE as BUFFER
              // Reason: ORA-06553: PLS-307: too many declarations of 'XMLTYPE' match this call
              // row[idx] = Buffer.from(row[idx]);
              return row[idx]
            default :
              return row[idx]
          }
          return row[idx]
        }
      },this))
      this.batch.push(row);
      return this.batch.length;
    } catch (e) {
      this.logWriter.write(`${new Date().toISOString()}[INFO]: TableWriter.appendRow("${this.tableName}",${this.batch.length}. Row conversion raised ${e}.Skipping Row. Row:\n`);
      this.logWriter.write(`${row}\n`);
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
 
  freeLobList() {
    this.lobList.forEach(async function(lob,idx) {
      try {
        await lob.close();
      } catch(e) {
        this.logWriter.write(`LobList[${idx}]: Error ${e}\n`);
      }   
    },this)
  }
  
  hasPendingRows() {
    return this.batch.length > 0;
  }
      
      
  async serializeLobs(record) {
    const newRecord = await Promise.all(this.tableInfo.targetDataTypes.map(function(targetDataType,idx) {
      if (record[idx] !== null) {
        if (this.tableInfo.binds[idx].type === oracledb.CLOB) {
          // ### Cannot reac contact of local clob...
          // return this.dbi.stringFromClob(record[idx])
          return this.dbi.stringFromLocalClob(record[idx])
        }
      }
      return record[idx];
    },this))
    return newRecord;
  }   
      
  async writeBatch() {
      
    // Ideally we used should reuse tempLobs since this is much more efficient that setting them up, using them once and tearing them down.
    // Infortunately the current implimentation of the Node Driver does not support this, once the 'finish' event is emitted you cannot truncate the tempCLob and write new content to it.
    // So we have to free the current tempLob Cache and create a new one for each batch

    this.batchCount++;
    
    if (this.insertMode === 'Batch') {

      try {
        await this.dbi.executeSQL(sqlSetSavePoint,[])
        const results = await this.dbi.executeMany(this.tableInfo.dml,this.batch,{bindDefs : this.tableInfo.binds});
        this.endTime = new Date().getTime();
        // console.log(`Batch:${this.batchCount}. ${this.batch.length} rows inserted`)
        this.batch.length = 0;
        this.freeLobList();
        return this.skipTable
      } catch (e) {
        await this.dbi.executeSQL(sqlRollbackSavePoint,[])
        if (e.errorNum && (e.errorNum === 4091)) {
          await this.dbi.executeSQL(sqlSetSavePoint,[])
          // Mutating Table - Convert to Cursor based PL/SQL Block
          if (this.status.showInfoMsgs) {
            logWriter.write(`${new Date().toISOString()}[TableWriter.writeBatch("${this.tableName}")][INFO]: Batch size [${this.batch.length}]. executeMany() operation raised:\n${e}\n`);
            this.logWriter.write(`${this.tableInfo.dml}\n`);
            this.logWriter.write(`${this.tableInfo.targetDataTypes}\n`);
            this.logWriter.write(`${JSON.stringify(this.tableInfo.binds)}\n`);
            this.logWriter.write(`${this.batch[0]}\n...\n${this.batch[this.batch.length-1]}\n`);
            this.logWriter.write(`${new Date().toISOString()}[TableWriter.writeBatch("${this.tableName}")][INFO]: . Switching to PL/SQL Block.\n`);          
          }
          this.tableInfo.dml = this.avoidMutatingTable(this.tableInfo.dml);
          if (this.status.sqlTrace) {
            this.status.sqlTrace.write(`${this.tableInfo.dml}\n/\n`);
          }
          try {
            const results = await this.dbi.executeMany(this.tableInfo.dml,this.batch,{bindDefs : this.tableInfo.binds}); 
            this.endTime = new Date().getTime();
            this.batch.length = 0;
            return this.skipTable
          } catch (e) {
            await this.dbi.executeSQL(sqlRollbackSavePoint,[])
            if (this.status.showInfoMsgs) {
              this.logWriter.write(`${new Date().toISOString()}[TableWriter.writeBatch("${this.tableName}")][INFO]: Batch size [${this.batch.rows.length}]. executeMany() with PL/SQL block raised:\n${e}\n`);
              this.logWriter.write(`${this.tableInfo.dml}\n`);
              this.logWriter.write(`${this.tableInfo.targetDataTypes}\n`);
              this.logWriter.write(`${JSON.stringify(this.tableInfo.binds)}\n`);
              this.logWriter.write(`${this.batch[0]}\n...\n${this.batch[this.batch.length-1]}\n`);
              this.logWriter.write(`${new Date().toISOString()}[TableWriter.writeBatch("${this.tableName}")][INFO]: Switching to Iterative operations.\n`);          
            }
            this.insertMode = 'Iterative';
          }
        } 
        else {  
          if (this.status.showInfoMsgs) {
            this.logWriter.write(`${new Date().toISOString()}[TableWriter.writeBatch("${this.tableName}")][INFO]: Batch size [${this.batch.length}]. executeMany() operation raised:\n${e}.\n`);
            this.logWriter.write(`${this.tableInfo.dml}\n`);
            this.logWriter.write(`${this.tableInfo.targetDataTypes}\n`);
            this.logWriter.write(`${JSON.stringify(this.tableInfo.binds)}\n`);
            this.logWriter.write(`${this.batch[0]}\n...\n${this.batch[this.batch.length-1]}\n`);
            this.logWriter.write(`${new Date().toISOString()}[TableWriter.writeBatch("${this.tableName}")][INFO]: Switching to Iterative operations.\n`);          
          }
          this.insertMode = 'Iterative';
        }
      }
    }


    for (const row in this.batch) {
      try {
        const results = await this.dbi.executeSQL(this.tableInfo.dml,this.batch[row])
      } catch (e) {
        const errInfo = this.status.showInfoMsgs ? [this.tableInfo.dml,this.tableInfo.targetDataTypes,JSON.stringify(this.tableInfo.binds)] : []
        const record = await this.serializeLobs(this.batch[row])
        const abort = this.dbi.handleInsertError(this.tableName,record,e,this.batch.length,row,errInfo);
        if (abort) {
          await this.dbi.rollbackTransaction();
          this.skipTable = true;
          break;
        }
      }
    } 
    // console.log(`Iterative:${this.batchCount}. ${this.batch.length} rows inserted`)
    // ### Iterative must commit to allow a subsequent batch to rollback.
    this.endTime = new Date().getTime();
    this.batch.length = 0;
    this.freeLobList();
    return this.skipTable     
  }

  async finalize() {
    if (this.hasPendingRows()) {
      this.skipTable = await this.writeBatch();   
    }
    await this.dbi.commitTransaction();
    await this.enableTriggers();
    return {
      startTime    : this.startTime
    , endTime      : this.endTime
    , insertMode   : this.insertMode
    , skipTable    : this.skipTable
    , batchCount   : this.batchCount
    }    
  }

}

module.exports = TableWriter;
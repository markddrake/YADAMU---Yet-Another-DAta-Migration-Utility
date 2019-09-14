"use strict"

// const WKX = require('wkx');

const oracledb = require('oracledb');

const YadamuWriter = require('../../common/yadamuWriter.js');

class TableWriter extends YadamuWriter {

  constructor(dbi,tableName,tableInfo,status,yadamuLogger) {
    super(dbi,tableName,tableInfo,status,yadamuLogger)
    this.lobUsage = 0;
    this.lobList = [];
    this.dumpOracleTestcase = false;
  }

  async initialize() {
    await this.disableTriggers();
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
    try {           
      row = await Promise.all(this.tableInfo.targetDataTypes.map(function(targetDataType,idx) {          
        if (
        row[idx] !== null) {
          const dataType = this.dbi.decomposeDataType(targetDataType);   
          
          /*
          switch (dataType.type) {
            case "GEOMETRY":
              console.log(WKX.Geometry.parse(Buffer.from(row[idx],'hex')).toWkt())
              break;
            default:
          }      
          */
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
          if (this.tableInfo.binds[idx].type === oracledb.BLOB) {
            this.lobUsage++
            // A promise...
            return this.dbi.trackBlobFromHexBinary(row[idx], this.lobList)                                                                    
          }
          switch (dataType.type) {
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
        return null;
      },this))

      this.batch.push(row);
      return this.batch.length;
    } catch (e) {
      const errInfo = this.status.showInfoMsgs ? [this.tableInfo.dml,this.tableInfo.targetDataTypes,JSON.stringify(this.tableInfo.binds)] : []     
      this.dbi.handleInsertError(`${this.constructor.name}.apppendRow()`,this.tableName,this.batch.length,-1,row,e,errInfo);
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
        this.yadamuLogger.logException([`${this.constructor.name}.freeLobList()`,`${idx}`],e);
      }   
    },this)
  }
  
  async serializeLobs(record) {
    const newRecord = await Promise.all(this.tableInfo.targetDataTypes.map(function(targetDataType,idx) {
      if (record[idx] !== null) {
        switch (this.tableInfo.binds[idx].type) {
          case oracledb.CLOB:
            // console.log(record[idx])
            // ### Cannot re-read content that has been written to local clob
            // return this.dbi.stringFromClob(record[idx])
            return this.dbi.stringFromLocalClob(record[idx])
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
      
  async writeBatch() {
      
    // Ideally we used should reuse tempLobs since this is much more efficient that setting them up, using them once and tearing them down.
    // Infortunately the current implimentation of the Node Driver does not support this, once the 'finish' event is emitted you cannot truncate the tempCLob and write new content to it.
    // So we have to free the current tempLob Cache and create a new one for each batch

    this.batchCount++;
    
    if (this.insertMode === 'Batch') {

      try {
        await this.dbi.createSavePoint()
        const results = await this.dbi.executeMany(this.tableInfo.dml,this.batch,{bindDefs : this.tableInfo.binds});
        this.endTime = new Date().getTime();
        this.batch.length = 0;
        this.freeLobList();
        return this.skipTable
      } catch (e) {
        await this.dbi.restoreSavePoint();
        if (e.errorNum && (e.errorNum === 4091)) {
          await this.dbi.createSavePoint()
          // Mutating Table - Convert to Cursor based PL/SQL Block
          if (this.status.showInfoMsgs) {
            yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`,`${this.batch.length}`],`executeMany() operation raised:\n${e}`);
            this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
            this.yadamuLogger.writeDirect(`${this.tableInfo.targetDataTypes}\n`);
            this.yadamuLogger.writeDirect(`${JSON.stringify(this.tableInfo.binds)}\n`);
            this.yadamuLogger.writeDirect(`${JSON.stringify(this.batch[0])}\n...\n${JSON.stringify(this.batch[this.batch.length-1])}\n`);
            this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to PL/SQL Block.`);          
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
            await this.dbi.restoreSavePoint();
            if (this.status.showInfoMsgs) {
              this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`,`${this.batch.length}`],`executeMany() with PL/SQL block raised:\n${e}`);
              this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
              this.yadamuLogger.writeDirect(`${this.tableInfo.targetDataTypes}\n`);
              this.yadamuLogger.writeDirect(`${JSON.stringify(this.tableInfo.binds)}\n`);
              this.yadamuLogger.writeDirect(`${JSON.stringify(this.batch[0])}\n...\n${JSON.stringify(this.batch[this.batch.length-1])}\n`);
              this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to Iterative operations.`);          
            }
            this.insertMode = 'Iterative';
          }
        } 
        else {  
          if (this.status.showInfoMsgs) {
            this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`,`${this.batch.length}`],`executeMany() operation raised:\n${e}`);
            this.yadamuLogger.writeDirect(`${this.tableInfo.dml}\n`);
            this.yadamuLogger.writeDirect(`${this.tableInfo.targetDataTypes}\n`);
            this.yadamuLogger.writeDirect(`${JSON.stringify(this.tableInfo.binds)}\n`);
            this.yadamuLogger.writeDirect(`${JSON.stringify(this.batch[0])}\n...\n${JSON.stringify(this.batch[this.batch.length-1])}\n`);
            this.yadamuLogger.info([`${this.constructor.name}.writeBatch()`,`"${this.tableName}"`],`Switching to Iterative operations.`);          
            if (this.dumpOracleTestcase) {
              console.log('DDL:')
              console.log(this.tableInfo.ddl)
              console.log('DML:')
              console.log(this.tableInfo.dml)
              console.log('BINDS:')
              console.log(JSON.stringify(this.tableInfo.binds));
              console.log('DATA:');
              console.log(JSON.stringify(this.batch.slice(0,9)));
            }
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
        this.skipTable = await this.dbi.handleInsertError(`${this.constructor.name}.writeBatch()`,this.tableName,this.batch.length,row,record,e,errInfo);
        if (this.skipTable) {
          break;
        }
      }
    } 
    // ### Iterative must commit to allow a subsequent batch to rollback.
    this.endTime = new Date().getTime();
    this.batch.length = 0;
    this.freeLobList();
    return this.skipTable     
  }
}

module.exports = TableWriter;
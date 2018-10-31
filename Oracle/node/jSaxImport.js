"use strict" 

const { Transform } = require('stream');
const { Writable } = require('stream');
const Readable = require('stream').Readable;
const common = require('./common.js');
const clarinet = require('c:/Development/github/clarinet/clarinet.js');
// const clarinet = require('clarinet');
const fs = require('fs');
const oracledb = require('oracledb');

const LOB_STRING_MAX_LENGTH    = 16 * 1024 * 1024;
// const LOB_STRING_MAX_LENGTH    = 64 * 1024;
const BFILE_STRING_MAX_LENGTH  =  2 * 1024;
const STRING_MAX_LENGTH        =  4 * 1024;

const DATA_TYPE_STRING_LENGTH = {
  BLOB          : LOB_STRING_MAX_LENGTH
, CLOB          : LOB_STRING_MAX_LENGTH
, NCLOB         : LOB_STRING_MAX_LENGTH
, OBJECT        : LOB_STRING_MAX_LENGTH
, XMLTYPE       : LOB_STRING_MAX_LENGTH
, ANYDATA       : LOB_STRING_MAX_LENGTH
, BFILE         : BFILE_STRING_MAX_LENGTH
, DATE          : 24
, TIMESTAMP     : 30
, INTERVAL      : 16
}  
  
 
class RowParser extends Transform {
  
  constructor(logWriter, options) {

    super({objectMode: true });  
  
    const rowParser = this;
    
    this.logWriter = logWriter;

    this.saxJParser = clarinet.createStream();
    this.saxJParser.on('error',function(err) {this.logWriter.write(`$(err}\n`);})
    
    this.objectStack = [];
    this.emptyObject = true;
    this.dataPhase = false;     
    
    this.currentObject = undefined;
    this.chunks = [];

    this.jDepth = 0;
       
    this.saxJParser.onkey = function (key) {
      // rowParser.logWriter.write(`onKey(${rowParser.jDepth},${key})\n`);
      
      switch (rowParser.jDepth){
        case 1:
          // Push the completed first level object/array downstream. Replace the current top level object with an empty object of the same type.
          rowParser.push(rowParser.currentObject);
          if (Array.isArray(rowParser.currentObject)) {
             rowParser.currentObject = [];
          }
          else {
             rowParser.currentObject = {};
          }
          if (key === 'data') {
            rowParser.dataPhase = true;
          }
          break;
        case 2:
          if (rowParser.dataPhase) {
            rowParser.push({ table : key});
          }
          break;
        default:
      }
      // Push the current object onto the stack and the current object to the key
      rowParser.objectStack.push(rowParser.currentObject);
      rowParser.currentObject = key;
    };

    this.saxJParser.onopenobject = function (key) {
      // rowParser.logWriter.write(`onOpenObject(${rowParser.jDepth}:, Key:"${key}". ObjectStack:${rowParser.objectStack}\n`);      
      this.emptyObject = (key === undefined);
      
      if (rowParser.jDepth > 0) {
        rowParser.objectStack.push(rowParser.currentObject);
      }
      
      switch (rowParser.jDepth) {
        case 0:
          // Push the completed first level object/array downstream. Replace the current top level object with an empty object of the same type.
          if (rowParser.currentObject !== undefined) {
            rowParser.push(rowParser.currentObject);
          }  
          if (key === 'data') {
            rowParser.dataPhase = true;
          }
          break;
        case 1:
          if (rowParser.dataPhase) {
            rowParser.push({ table : key});
          }
          break;
        default:
      }
      rowParser.objectStack.push({});
      if (key !== undefined) {
        rowParser.currentObject = key;
        rowParser.jDepth++;
      }
    };

    this.saxJParser.onopenarray = function () {

      // rowParser.logWriter.write(`onOpenArray(${rowParser.jDepth}): ObjectStack:${rowParser.objectStack}\n`);
      if (rowParser.jDepth > 0) {
        rowParser.objectStack.push(rowParser.currentObject);
      }
      rowParser.currentObject = [];
      rowParser.jDepth++;
    };


    this.saxJParser.onvaluechunk = function (v) {
      rowParser.chunks.push(v);  
    };
       
    this.saxJParser.onvalue = function (v) {
      
      // rowParser.logWriter.write(`onvalue(${rowParser.jDepth}: ObjectStack:${rowParser.objectStack}\n`);        
      if (rowParser.chunks.length > 0) {
        rowParser.chunks.push(v);
        v = rowParser.chunks.join('');
        // rowParser.logWriter.write(`onvalue(${rowParser.chunks.length},${v.length})\n`);
        rowParser.chunks = []
      }
      
      if (typeof v === 'boolean') {
        v = new Boolean(v).toString();
      }
      
      if (Array.isArray(rowParser.currentObject)) {
          // currentObject is an ARRAY. We got a value so add it to the Array
          rowParser.currentObject.push(v);
      }
      else {
          // currentObject is an Key. We got a value so fetch the parent object and add the KEY:VALUE pair to it. Parent Object becomes the Current Object.
          const parentObject = rowParser.objectStack.pop();
          parentObject[rowParser.currentObject] = v;
          rowParser.currentObject = parentObject;
      }
      // rowParser.logWriter.write(`onvalue(${rowParser.jDepth}: ObjectStack:${rowParser.objectStack}. CurrentObject:${rowParser.currentObject}\n`);        
    }
      
    this.saxJParser.oncloseobject = async function () {
      // rowParser.logWriter.write(`onCloseObject(${rowParser.jDepth}):\nObjectStack:${rowParser.objectStack})\nCurrentObject:${rowParser.currentObject}\n`);      
      
      if ((rowParser.dataPhase) && (rowParser.jDepth === 5)) {
        // Serialize any embedded objects found inside the array that represents a row of data.
        rowParser.currentObject = JSON.stringify(rowParser.currentObject);
      }
      
      rowParser.jDepth--;

      // An object can belong to an Array or a Key
      if (rowParser.objectStack.length > 0) {
        let owner = rowParser.objectStack.pop()
        let parentObject = undefined;
        if (Array.isArray(owner)) {   
          parentObject = owner;
          parentObject.push(rowParser.currentObject);
        }    
        else {
          parentObject = rowParser.objectStack.pop()
          if (!this.emptyObject) {
            parentObject[owner] = rowParser.currentObject;
          }
        }   
        rowParser.currentObject = parentObject;
      }
    }
   
    this.saxJParser.onclosearray = function () {
      // rowParser.logWriter.write(`onclosearray(${rowParser.jDepth}: ObjectStack:${rowParser.objectStack}. CurrentObject:${rowParser.currentObject}\n`);        
      
      let skipObject = false;
      
      if ((rowParser.dataPhase) && (rowParser.jDepth === 4)) {
        // Serialize any embedded objects found inside the array that represents a row of data.
          rowParser.push({ data : rowParser.currentObject});
          skipObject = true;
      }

      rowParser.jDepth--;

      // An Array can belong to an Array or a Key
      if (rowParser.objectStack.length > 0) {
        let owner = rowParser.objectStack.pop()
        let parentObject = undefined;
        if (Array.isArray(owner)) {   
          parentObject = owner;
          if (!skipObject) {
            parentObject.push(rowParser.currentObject);
          }
        }    
        else {
          parentObject = rowParser.objectStack.pop()
          if (!skipObject) {
            parentObject[owner] = rowParser.currentObject;
          }
        }
        rowParser.currentObject = parentObject;
      }   
    }

   }  
   
  _transform(data,enc,callback) {
    this.saxJParser.write(data);
    callback();
  };
}

function processLog(log,status,logWriter) {

  const logDML         = (status.loglevel && (status.loglevel > 0));
  const logDDL         = (status.loglevel && (status.loglevel > 1));
  const logDDLIssues   = (status.loglevel && (status.loglevel > 2));
  const logTrace       = (status.loglevel && (status.loglevel > 3));
    
  log.forEach(function(result) {
                const logEntryType = Object.keys(result)[0];
                const logEntry = result[logEntryType];
                switch (true) {
                  case (logEntryType === "message") : 
                    logWriter.write(`${new Date().toISOString()}: ${logEntry}.\n`)
                    break;
                  case (logEntryType === "dml") : 
                    logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}". Rows ${logEntry.rowCount}. Elaspsed Time ${Math.round(logEntry.elapsedTime)}ms. Throughput ${Math.round((logEntry.rowCount/Math.round(logEntry.elapsedTime)) * 1000)} rows/s.\n`)
                    break;
                  case (logEntryType === "info") :
                    logWriter.write(`${new Date().toISOString()}[INFO]: "${JSON.stringify(logEntry)}".\n`);
                    break;
                  case (logDML && (logEntryType === "dml")) :
                    logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}".\n${logEntry.sqlStatement}.\n`)
                    break;
                  case (logDDL && (logEntryType === "ddl")) :
                    logWriter.write(`${new Date().toISOString()}: Table "${logEntry.tableName}".\n${logEntry.sqlStatement}.\n`) 
                    break;
                  case (logTrace && (logEntryType === "trace")) :
                    logWriter.write(`${new Date().toISOString()} [TRACE]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".\n' : '\n'}${logEntry.sqlStatement}.\n`)
                    break;
                  case (logEntryType === "error"):
	                switch (true) {
		              case (logEntry.severity === 'FATAL') :
                        status.errorRaised = true;
                        logWriter.write(`${new Date().toISOString()} [${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".' : ''} Details: ${logEntry.details}\n${logEntry.sqlStatement}\n`)
				        break
					  case (logEntry.severity === 'WARNING') :
                        status.warningRaised = true;
                        logWriter.write(`${new Date().toISOString()} [${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName + '".' : ''} Details: ${logEntry.details}${logEntry.sqlStatement}\n`)
                        break;
                      case (logDDLIssues) :
                        logWriter.write(`${new Date().toISOString()} [${logEntry.severity}]: ${logEntry.tableName ? 'Table: "' + logEntry.tableName  + '".' : ''} Details: ${logEntry.details}${logEntry.sqlStatement}\n`)
                    } 	
                } 
				if ((status.sqlTrace) && (logEntry.sqlStatement)) {
				  status.sqlTrace.write(`${logEntry.sqlStatement}\n\/\n`)
		        }
  })
}    

async function setCurrentSchema(conn, schema, status, logWriter) {

  const sqlStatement = `begin :log := JSON_IMPORT.SET_CURRENT_SCHEMA(:schema); end;`;
     
  try {
    const results = await conn.execute(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 1024} , schema:schema});
    const log = JSON.parse(results.outBinds.log);
    if (log !== null) {
      processLog(log, status, logWriter)
    }
  } catch (e) {
    logWriter.write(`${e}\n${e.stack}\n`);
  }    
}

async function executeDDL(conn, schema, ddl, status, logWriter) {

  const sqlStatement = `begin :log := JSON_EXPORT_DDL.APPLY_DDL_STATEMENTS(:ddl, :schema); end;`;
      
  try {
    const ddlLob = await common.lobFromJSON(conn,ddl);  
    const results = await conn.execute(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , ddl:ddlLob, schema:schema});
    await ddlLob.close();
    const log = JSON.parse(results.outBinds.log);
    if (log !== null) {
      processLog(log, status, logWriter)
    }
  } catch (e) {
    logWriter.write(`${e}\n${e.stack}\n`);
  }    
}

async function disableConstraints(conn, schema, status, logWriter) {

  const sqlStatement = `begin :log := JSON_IMPORT.DISABLE_CONSTRAINTS(:schema); end;`;
   
  try {
    const results = await conn.execute(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , schema:schema});
    const log = JSON.parse(results.outBinds.log);
    if (log !== null) {
      processLog(log, status, logWriter)
    }
  } catch (e) {
    logWriter.write(`${e}\n${e.stack}\n`);
  }    
}

async function enableConstraints(conn, schema, status, logWriter) {

  const sqlStatement = `begin :log := JSON_IMPORT.ENABLE_CONSTRAINTS(:schema); end;`;
   
  try {
    const results = await conn.execute(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , schema:schema});
    const log = JSON.parse(results.outBinds.log);
    if (log !== null) {
      processLog(log, status, logWriter)
    }
  } catch (e) {
    logWriter.write(`${e}\n${e.stack}\n`);
  }    
}

function generateBinds(tableInfo, dataTypeSizes) {

   const dataTypeArray = JSON.parse('[' +  tableInfo.targetDataTypes.replace(/(\"\.\")/g, '\\".\\"') + ']')
   const dataLengthArray = JSON.parse('[' + dataTypeSizes + ']')

   return dataTypeArray.map(function (dataType,idx) {
     switch (dataType) {
       case 'CLOB':
         // return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
         tableInfo.lobColumns.push(idx);
         return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
       case 'NCLOB':
         // return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
         tableInfo.lobColumns.push(idx);
         return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
       case 'BLOB':
         // return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
         tableInfo.lobColumns.push(idx);
         return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
       case 'XMLTYPE':
         // return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
         tableInfo.lobColumns.push(idx);
         return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
       case 'ANYDATA':
         // return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
         tableInfo.lobColumns.push(idx);
         return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
       case 'NUMBER':
         return { type: oracledb.NUMBER }
       case 'FLOAT':
         return { type: oracledb.NUMBER }
       case 'CHAR':
         return { type :oracledb.STRING, maxSize : parseInt(dataLengthArray[idx])}
       case 'NCHAR':
         return { type :oracledb.STRING, maxSize : parseInt(dataLengthArray[idx]*2)}
       case 'NVARCHAR2':
         return { type :oracledb.STRING, maxSize : parseInt(dataLengthArray[idx]*2)}
       case 'VARCHAR':
         return { type :oracledb.STRING, maxSize : parseInt(ataLengthArray[idx])}
       case 'VARCHAR2':
         return { type :oracledb.STRING, maxSize : parseInt(dataLengthArray[idx])}
       case 'DATE':
         return { type :oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType]}
       case 'RAW':
         return { type :oracledb.STRING, maxSize : parseInt(dataLengthArray[idx])*2}
       case 'RAW(1)':
         return { type :oracledb.STRING, maxSize : 5}
       case 'BFILE':
         return { type :oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH[dataType] }
       default:
         if (dataType.startsWith('NUMBER')) {
           return { type: oracledb.NUMBER }
         }
         if (dataType.startsWith('RAW(')) {
           return { type: oracledb.STRING, maxSize : parseInt(dataType.match(/\((\d+)\)/)[0].slice(1,-1))*2}
         }
         if ((dataType.startsWith('CHAR(')) || (dataType.startsWith('VARCHAR2(')) || (dataType.startsWith('NCHAR(')) || (dataType.startsWith('NVARCHAR2('))){
           return { type: oracledb.STRING, maxSize : parseInt(dataType.match(/\((\d+)\)/)[0].slice(1,-1))}
         }
         if (dataType.startsWith('FLOAT')) {
           return { type: oracledb.NUMBER }
         }
         if (dataType.startsWith('TIMESTAMP')) {
           return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH['TIMESTAMP']  }
         }
         if (dataType.startsWith('INTERVAL')) {
           return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH['TIMESTAMP']  }
         }
         if (dataType.indexOf('.') > -1) {
           // return {type : oracledb.STRING, maxSize : DATA_TYPE_STRING_LENGTH['OBJECT']  }
           tableInfo.lobColumns.push(idx);
           return {type : oracledb.CLOB, maxSize : DATA_TYPE_STRING_LENGTH['OBJECT'] }
         }
         return {type : dataType};
     }
   })

}


async function generateStatementCache(conn, schema, metadata, status, ddlRequired, logWriter) {

  const sqlStatement = `begin :sql := JSON_IMPORT.GENERATE_STATEMENTS(:metadata, :schema);\nEND;`;
    
  try {
    const ddlStatements = [];  
    const metadataLob = await common.lobFromJSON(conn,{metadata: metadata});  
    const results = await conn.execute(sqlStatement,{sql:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 16 * 1024 * 1024} , metadata:metadataLob, schema:schema});
    await metadataLob.close();
    const statementCache = JSON.parse(results.outBinds.sql);
    const tables = Object.keys(metadata); 
    tables.forEach(function(table,idx) {
                     const tableInfo = statementCache[table];
                     ddlStatements[idx] = tableInfo.ddl             
                     tableInfo.lobColumns = [];
                     tableInfo.binds = generateBinds(tableInfo,metadata[table].dataTypeSizing)
                     /*
                     logWriter.write(tableMetadata.tableName)
                     logWriter.write(dataTypeArray)
                     logWriter.write(dataLengthArray);
                     */
    });
    
    if (ddlRequired) {
      const ddl = { ddl : ddlStatements};
      await executeDDL(conn, schema, ddl, status, logWriter);
    }
    
    return statementCache
  } catch (e) {
    logWriter.write(`${e}\n${e.stack}\n`);
  }
}

class DbWriter extends Writable {
  
  constructor(conn,schema,batchSize,commitSize,lobCacheSize,mode,status,logWriter,options) {

    super({objectMode: true });
    
    const dbWriter = this;
    
    this.conn = conn;
    this.schema = schema;
    this.batchSize = batchSize;
    this.commitSize = commitSize;
    this.lobCacheSize = lobCacheSize;
    this.mode = mode;
    this.logWriter = logWriter;
    this.status = status;
    this.ddlRequired = (mode !== 'DATA_ONLY');
    
    this.batch = [];
    this.lobCache = [];

    this.systemInformation = undefined;
    this.metadata = undefined;

    this.statementCache = undefined

    this.tableName = undefined;
    this.tableInfo = undefined;
    this.rowCount = undefined;
    this.startTime = undefined;
    this.skipTable = true;
    
    this.logDDLIssues   = (status.loglevel && (status.loglevel > 2));
    
  }      
  
  async stringToLob(conn,str,lobCache,lobCacheIdx) {
    return new Promise(async function(resolve,reject) {
                                if (str === null) {
                                  resolve(str)
                                }
                                else {
                                  let tempLob;
                                  if (lobCacheIdx >= lobCache.length) {
                                    tempLob = await conn.createLob(oracledb.CLOB);
                                    lobCache.push(tempLob);
                                  }
                                  else {
                                    tempLob = lobCache[lobCacheIdx];
                                    // Cannot reuse templobs to since 'finish' event has already been emitted - need to close it can create new one.
                                    // await conn.execute('begin DBMS_LOB.trim(:1,0); end;',[tempLob]);
                                    await tempLob.close();
                                    tempLob = await conn.createLob(oracledb.CLOB);
                                    lobCache[lobCacheIdx] = tempLob;
                                  }
                                  tempLob.on('error',function(err) {reject(err);});
                                  tempLob.on('finish', function() {resolve(tempLob)});
                                  const s = new Readable();
                                  s.on('error', function(err) {reject(err);});                               
                                  s.push(str);
                                  s.push(null);
                                  s.pipe(tempLob)
                                }
    })
  }
    
  async closeLobCache() {
    this.lobCache.forEach(async function(lob) {
                            try {
                              await lob.close();     
                            } catch (e) {
                              this.logWriter.write(`Error closing LOB: ${e}\n${e.stack}\n`);
                            }
                           
    })
    this.lobCache.length = 0;
  }

  
  async convertLobs(data) {
      
    try {
      const lobList = await Promise.all(this.tableInfo.lobColumns.map(async function (lobIndex,idx) {
                                                                    data[lobIndex] = await this.stringToLob(this.conn, data[lobIndex],this.lobCache, this.tableInfo.lobIndex+idx);
                                                                    return data[lobIndex]
      },this))
      return lobList.length
    } catch (e) {
      this.logWriter.write(`${e}\n${e.stack}\n`);
      throw (e);
    }
  }
    
  async setTable(tableName) {
    this.tableName = tableName
    this.tableInfo =  this.statementCache[tableName];
    this.tableInfo.lobIndex = 0;
    if (this.tableInfo.lobColumns.length > 0) {
      // Adjust Batchsize to accomodate lobCacheSize
      let lobBatchSize = Math.floor(this.lobCacheSize/this.tableInfo.lobColumns.length);
      this.tableInfo.batchSize = (lobBatchSize > this.tableInfo.batchSize) ? this.tableInfo.batchSize : lobBatchSize;
    }
    this.rowCount = 0;
    this.batch.length = 0;
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.skipTable = false;
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
      
  async writeBatch(status) {
    try {
      const results = await this.conn.executeMany(this.tableInfo.dml,this.batch,{bindDefs : this.tableInfo.binds});
      const endTime = new Date().getTime();
      this.batch.length = 0;
      return endTime
    } catch (e) {
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
          return endTime
        } catch (e) {
          if (this.logDDLIssues) {
            this.logWriter.write(`${new Date().toISOString()}:_write(${this.tableName},${this.batch.length}) : executeMany() failed. ${e}. Retrying using execute() loop.\n`);
          }
        }
      }
      else {  
        if (this.logDDLIssues) {
          this.logWriter.write(`${new Date().toISOString()}:_write(${this.tableName},${this.batch.length}) : executeMany() failed. ${e}. Retrying using execute() loop.\n`);
        }
      }
    }

    try {
      for (let row in this.batch) {
        let results = await this.conn.execute(this.tableInfo.dml,this.batch[row],{bindDefs : this.tableInfo.binds})
      }
      const endTime = new Date().getTime();
      this.batch.length = 0;
      return endTime
    } catch (e) {
      this.batch.length = 0;
      this.skipTable = true;
      this.conn.rollback();
      this.status.warningRaised = true;
      this.logWriter.write(`${new Date().toISOString()}: Table ${this.tableName}. Skipping table. Reason: ${e.message}\n`)
      if (this.logDDLIssues) {
        this.logWriter.write(`${this.tableInfo.dml}\n`);
        this.logWriter.write(`${JSON.stringify(this.tableInfo.binds)}\n`);
        this.logWriter.write(`${this.batch}\n`);
      }
    }
    // await this.resetLobCache();
  }

  async _write(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
        case 'systemInformation':
          this.systemInformation = obj.systemInformation;
          break;
        case 'ddl':
          if (this.ddlRequired) {
            await executeDDL(this.conn, this.schema, { systemInformation : this.systemInformation, ddl : obj.ddl}, this.status, this.logWriter);
          }
          break;
        case 'metadata':
          this.metadata = obj.metadata;
          this.statementCache = await generateStatementCache(this.conn, this.schema, this.metadata, this.status, this.ddlRequired, this.logWriter);
          break;
        case 'table':
          // this.logWriter.write(`${new Date().toISOString()}: Switching to Table "${obj.table}".\n`);
          if (this.tableName === undefined) {
            // First Table - Disable Constraints
            await disableConstraints(this.conn, this.schema, this.status, this.logWriter);
          }
          else {
            if (this.batch.length > 0) {
              // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batch.length} rows.`);
              this.endTime = await this.writeBatch(this.status);
            }  
            if (!this.skipTable) {
              const elapsedTime = this.endTime - this.startTime;
              this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
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
          if (this.tableInfo.lobColumns.length > 0) {
            this.tableInfo.lobIndex += await this.convertLobs(obj.data)
          }
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
          this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          await enableConstraints(this.conn, this.schema, this.status, this.logWriter);
          await this.closeLobCache();
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

function processFile(conn, schema, dumpFilePath,batchSize,commitSize,lobCacheSize,mode,status,logWriter) {
  
  return new Promise(function (resolve,reject) {
    try {
      const dbWriter = new DbWriter(conn,schema,batchSize,commitSize,lobCacheSize,mode,status,logWriter);
      dbWriter.on('error',function(err) {logWriter.write(`${err}\n${err.stack}\n`);})
      dbWriter.on('finish', function() { resolve()});
      const rowGenerator = new RowParser(logWriter);
      rowGenerator.on('error',function(err) {logWriter.write(`${err}\n${err.stack}\n`);})
      const readStream = fs.createReadStream(dumpFilePath);    
      readStream.pipe(rowGenerator).pipe(dbWriter);
    } catch (e) {
      logWriter.write(`${e}\n${e.stack}\n`);
      reject(e);
    }
  })
}
    
async function main() {

  let pool;	
  let conn;
  let parameters;
  let logWriter = process.stdout;
    
  const status = {
    errorRaised   : false
   ,warningRaised : false
   ,statusMsg     : 'successfully'
  }

  let results;
  
  try {
      
    process.on('unhandledRejection', function (err, p) {
      logWriter.write(`Unhandled Rejection:\Error:`);
      logWriter.write(`${err}\n${err.stack}\n`);
    })

    parameters = common.processArguments(process.argv,'export');

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }

	if (parameters.SQLTRACE) {
	  status.sqlTrace = fs.createWriteStream(parameters.SQLTRACE);
    }
	
    conn = await common.doConnect(parameters.USERID);
    await setCurrentSchema(conn, parameters.TOUSER, status, logWriter);
    
	const stats = fs.statSync(parameters.FILE)
    const fileSizeInBytes = stats.size
    
    if (parameters.LOGLEVEL) {
       status.loglevel = parameters.LOGLEVEL;
    }
    	
    await processFile(conn, parameters.TOUSER, parameters.FILE, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.LOBCACHESIZE, parameters.MODE, status, logWriter);
    const currentUser = parameters.USERID.split('/')[0]
    await setCurrentSchema(conn, currentUser, status, logWriter);
    
    common.doRelease(conn);						   

    status.statusMsg = status.warningRaised ? 'with warnings' : status.statusMsg;
    status.statusMsg = status.errorRaised ? 'with errors'  : status.statusMsg;
     
    logWriter.write(`Import operation completed ${status.statusMsg}.`);
    if (logWriter !== process.stdout) {
       console.log(`Import operation completed ${status.statusMsg}: See "${parameters.LOGFILE}" for details.`);
    }
  } catch (e) {
    if (logWriter !== process.stdout) {
      console.log(`Import operation failed: See "${parameters.LOGFILE}" for details.`);
      logWriter.write('Import operation failed.\n');
      logWriter.write(`${e}\n`);
    }
    else {
      console.log(`Import operation Failed:`);
      console.log(e);
    }
    if (conn !== undefined) {
      common.doRelease(conn);
    }
  }
  
  if (logWriter !== process.stdout) {
    logWriter.close();
  }

  if (parameters.SQLTRACE) {
    status.sqlTrace.close();
  }
}
    
main()


 
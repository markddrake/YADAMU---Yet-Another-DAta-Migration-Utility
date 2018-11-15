"use strict";
const fs = require('fs');
const mysql = require('mysql')
const Writable = require('stream').Writable
const Readable = require('stream').Readable;
const path = require('path');

const Yadamu = require('../../common/yadamuCore.js');
const RowParser = require('../../common/rowParser.js');
const MySQLCore = require('./mysqlCore.js');
const MySQLShared = require('../../common/mysql/mariadbShared.js');

// Will be set to MySQLShared.generateStatementCache if JSON_TABLE is not supported or generateStatementCacheif JSON_TABLE is supported
// JSON_TABLE is supported starting with MYSQL 8.0

let generateStatementCache = undefined;

async function localGenerateStatementCache(conn, schema, systemInformation, metadata, status, logWriter) {
    
  const sqlStatement = `SET @RESULTS = '{}'; CALL GENERATE_STATEMENTS(?,?,@RESULTS); SELECT @RESULTS "SQL_STATEMENTS";`;                       
 
  let results = await MySQLCore.query(conn,status,sqlStatement,[JSON.stringify({systemInformation: systemInformation, metadata : metadata}),schema]);
  results = results.pop();
  const statementCache = JSON.parse(results[0].SQL_STATEMENTS)
  const tables = Object.keys(metadata); 
  await Promise.all(tables.map(async function(table,idx) {
                                       const tableInfo = statementCache[table];
                                       const columnNames = JSON.parse('[' + metadata[table].columns + ']');

                                       tableInfo.useSetClause = false;
                                       
                                       const setOperators = tableInfo.targetDataTypes.map(function(targetDataType,idx) {
                                                                                            switch (targetDataType) {
                                                                                              case 'geometry':
                                                                                                 tableInfo.useSetClause = true;
                                                                                                return ' "' + columnNames[idx] + '" = ST_GEOMFROMGEOJSON(?)';
                                                                                              default:
                                                                                                return ' "' + columnNames[idx] + '" = ?'
                                                                                            }
                                       })
                                       
                                       if (tableInfo.useSetClause) {
                                         tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('(')) + ` set ` + setOperators.join(',');
                                       }
                                       else {
                                         tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + `  values ?`;
                                         }
                                        
                                       try {
                                         const results = await MySQLCore.query(conn,status,tableInfo.ddl);   
                                       } catch (e) {
                                         logWriter.write(`${e}\n${tableInfo.ddl}\n`)
                                       }  
  }));
  
  // console.log(metadata)
  // console.log(statementCache)
  
  return statementCache;
}

class DbWriter extends Writable {
  
  constructor(conn,schema,batchSize,commitSize,mode,status,logWriter,options) {
    super({objectMode: true });
    const dbWriter = this;
    
    this.conn = conn;
    this.schema = schema;
    this.batchSize = batchSize;
    this.commitSize = commitSize;
    this.mode = mode;
    this.logWriter = logWriter;
    this.status = status;

    this.batch = [];
    
    this.systemInformation = undefined;
    this.metadata = undefined;
    
    this.statementCache = undefined;
    
    this.tableName = undefined;
    this.tableInfo = undefined;
    this.rowCount = undefined; 
    this.startTime = undefined;
    this.skipTable = true;
    
    this.logDDLIssues   = (status.loglevel && (status.loglevel > 2));
    // this.logDDLIssues   = true;
  }      
  
  async setTable(tableName) {
       
    this.tableName = tableName
    this.tableInfo =  this.statementCache[tableName]
    this.rowCount = 0;
    this.batch.length = 0;
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.skipTable = false;
  }
  
   async writeBatch(status) {
    
     try {
      if (this.tableInfo.useSetClause) {
        for (const i in this.batch) {
          try {
            const results = await MySQLCore.query(this.conn,this.status,this.tableInfo.dml,this.batch[i]);
          } catch(e) {
            if (e.errno && ((e.errno === 3616) || (e.errno === 3617))) {
              this.logWriter.write(`${new Date().toISOString()}: Table ${this.tableName}. Skipping Row Reason: ${e.message}\n`)
              this.rowCount--;
            }
            else {
              throw e;
            }
          }    
        }
      }
      else {  
        const results = await MySQLCore.query(this.conn,this.status,this.tableInfo.dml,[this.batch]);
      }
      const endTime = new Date().getTime();
      this.batch.length = 0;
      return endTime
    } catch (e) {
      this.status.warningRaised = true;
      this.logWriter.write(`${new Date().toISOString()}: Table ${this.tableName}. Skipping table. Reason: ${e.message}\n`)
      this.logWriter.write(`${this.tableInfo.dml}[${this.batch.length}]...\n`);
      this.batch.length = 0;
      this.skipTable = true;
      if (this.logDDLIssues) {
        this.logWriter.write(`${this.tableInfo.dml}\n`);
        this.logWriter.write(`${this.batch}\n`);
      }      
    }
  }

  async _write(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
        case 'systemInformation':
          this.systemInformation = obj.systemInformation;
          break;
        case 'metadata':
          this.metadata = obj.metadata;
          if (Object.keys(this.metadata).length > 0) {
            this.statementCache = await generateStatementCache(this.conn, this.schema, this.systemInformation, this.metadata, this.status, this.logWriter);
          }
          break;
        case 'table':
          // this.logWriter.write(`${new Date().toISOString()}: Switching to Table "${obj.table}".\n`);
          if (this.tableName !== undefined) {
            if (this.batch.length > 0) {
              // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batch.length} rows.`);
              this.endTime = await this.writeBatch(this.status);
              await this.conn.commit();
            }  
            if (!this.skipTable) {
              const elapsedTime = this.endTime - this.startTime;
              this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
            }
          }
          this.setTable(obj.table);
          await this.conn.beginTransaction();
          break;
        case 'data': 
          if (this.skipTable) {
            break;
          }
          this.tableInfo.targetDataTypes.forEach(function(targetDataType,idx) {
                                                   const dataType = Yadamu.decomposeDataType(targetDataType);
                                                   if (obj.data[idx] !== null) {
                                                     switch (dataType.type) {
                                                       case "tinyblob" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "blob" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "mediumblob" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "longblob" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "varbinary" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "binary" :
                                                         obj.data[idx] = Buffer.from(obj.data[idx],'hex');
                                                         break;
                                                       case "geometry":
                                                         obj.data[idx] = JSON.stringify(obj.data[idx]);
                                                         break;
                                                       case "json" :
                                                         obj.data[idx] = JSON.stringify(obj.data[idx]);
                                                         break;
                                                       case "timestamp" :
                                                         obj.data[idx] = new Date(Date.parse(obj.data[idx]));
                                                         break;
                                                       default :
                                                     }
                                                   }
                                                 },this)

          this.batch.push(obj.data);
          //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Batch contains ${this.batch.length} rows.`);
          if (this.batch.length  === this.batchSize) {
              //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Completed Batch contains ${this.batch.length} rows.`);
              this.endTime = await this.writeBatch(this.status);
          }  
          this.rowCount++;
          if ((this.rowCount % this.commitSize) === 0) {
             await this.conn.commit();
             const elapsedTime = this.endTime - this.startTime;
             // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Commit after Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
             await this.conn.beginTransaction();
          }
          break;
        default:
      }    
      callback();
    } catch (e) {
      this.logWriter.write(`${e}\n`);
      callback(e);
    }
  }
 
  async _final(callback) {
    try {
      if (this.tableName) {        
        if (!this.skipTable) {
          if (this.batch.length > 0) {
            // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batch.length} rows.`);
           this.endTime = await this.writeBatch();
          }  
          const elapsedTime = this.endTime - this.startTime;
          this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
          await this.conn.commit();
        }
      }          
      else {
        this.logWriter.write(`${new Date().toISOString()}: No tables found.\n`);
      }
      callback();
    } catch (e) {
      this.logWriter.write(`${e}\n`);
      callback(e);
    } 
  } 
}

function processFile(conn, schema, importFilePath, batchSize, commitSize, mode, status, logWriter) {
  
  return new Promise(function (resolve,reject) {
    const dbWriter = new DbWriter(conn,schema,batchSize,commitSize,mode,status,logWriter);
    const rowGenerator = new RowParser(logWriter);
    const readStream = fs.createReadStream(importFilePath);    
    dbWriter.on('finish', function() { resolve()});
    readStream.pipe(rowGenerator).pipe(dbWriter);
  })
}
    
async function main() {

  let pool; 
  let conn;
  let parameters;
  let logWriter = process.stdout;
  let status;
  
  let results;
  
  try {
      
    process.on('unhandledRejection', function (err, p) {
      logWriter.write(`Unhandled Rejection:\Error:`);
      logWriter.write(`${err}\n${err.stack}\n`);
    })
    
    parameters = MySQLCore.processArguments(process.argv,'export');
    status = Yadamu.getStatus(parameters);
    
    if (parameters.LOGFILE) {
      logWriter = fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
    }

    const connectionDetails = {
            host      : parameters.HOSTNAME
           ,user      : parameters.USERNAME
           ,password  : parameters.PASSWORD
           ,database  : parameters.DATABASE
           ,multipleStatements: true
    }

    conn = mysql.createConnection(connectionDetails);
 	await MySQLCore.connect(conn);
    if (await MySQLCore.setMaxAllowedPacketSize(conn,status,logWriter)) {
       conn = mysql.createConnection(connectionDetails);
       await MySQLCore.connect(conn);
    }
    await MySQLCore.configureSession(conn,status);

    const sqlGetVersion = `SELECT @@version`
    results = await MySQLCore.query(conn,status,sqlGetVersion);
    if (results[0]['@@version'] > '6.0') {
       generateStatementCache = localGenerateStatementCache
    }
    else {
      generateStatementCache = MySQLShared.generateStatementCache
    }
  
    // Force 5.7 Code Path
    // generateStatementCache = MySQLShared.generateStatementCache
 
 	results = await MySQLCore.createTargetDatabase(conn,status,parameters.TOUSER);
    
    const stats = fs.statSync(parameters.FILE)
    const fileSizeInBytes = stats.size
    logWriter.write(`${new Date().toISOString()}[Clarinet]: Processing file "${path.resolve(parameters.FILE)}". Size ${fileSizeInBytes} bytes.\n`)
    
    if (parameters.LOGLEVEL) {
       status.loglevel = parameters.LOGLEVEL;
    }
        
    await processFile(conn, parameters.TOUSER, parameters.FILE, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.MODE, status, logWriter);
    await conn.end();
    Yadamu.reportStatus(status,logWriter)
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
      await conn.end();
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
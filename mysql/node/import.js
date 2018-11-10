"use strict";
const fs = require('fs');
const mysql = require('mysql')
const Writable = require('stream').Writable
const Readable = require('stream').Readable;

const Yadamu = require('../../common/yadamuCore.js');
const RowParser = require('../../common/rowParser.js');
const MySQLCore = require('./mysqlCore.js');

const unboundedTypes = ['date','time','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json','set','enum'];
const spatialTypes   = ['geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection'];
const nationalTypes  = ['nchar','nvarchar'];
const integerTypes   = ['tinyint','mediumint','smallint','int','bigint']
   
// Will be set to generateStatementCacheLocal if JSON_TABLE is not supported or generateStatementCacheRemote if JSON_TABLE is supported
// JSON_TABLE is supported starting with MYSQL 8.0

let generateStatementCache = undefined;

function connect(conn) {
    
  return new Promise(function(resolve,reject) {
                       conn.connect(function(err) {
                                      if (err) {
                                        reject(err);
                                      }
                                      resolve();
                                    })
                    })
}   
      
function query(conn,sqlQuery,args) {
    
  return new Promise(function(resolve,reject) {
                       conn.query(sqlQuery,args,function(err,rows,fields) {
                                             if (err) {
                                               reject(err);
                                             }
                                             resolve(rows);
                                           })
                     })
}  

function mapForeignDataType(vendor, dataType, dataTypeLength, dataTypeSize) {
  switch (vendor) {
     case 'Oracle':
       switch (dataType) {
         case 'VARCHAR2':        return 'varchar';
         case 'NVARCHAR2':       return 'varchar';
         case 'NUMBER':          return 'decimal';
         case 'CLOB':            return 'longtext';
         case 'BLOB':            return 'longblob';
         case 'NCLOB':           return 'longtext';
         case 'XMLTYPE':         return 'longtext';
         case 'BFILE':           return 'varchar(2048)';
         case 'ROWID':           return 'varchar(32)';
         case 'RAW':             return 'binary';
         case 'ROWID':           return 'varchar(32)';
         case 'ANYDATA':         return 'longtext';
         default :
           if (dataType.indexOf('TIME ZONE') > -1) {
             return 'timestamp'; 
           }
           if (dataType.indexOf('INTERVAL') === 0) {
             return 'timestamp'; 
           }
           if (dataType.indexOf('XMLTYPE') > -1) { 
             return 'varchar(16)';
           }
           if (dataType.indexOf('.') > -1) { 
             return 'longtext';
           }
           return dataType.toLowerCase();
       }
       break;
     case 'MSSQLSERVER':
       switch (dataType) {
         case 'binary':
           switch (true) {
             case (dataTypeLength > 16777215):   return 'longblob';
             case (dataTypeLength > 65535):      return 'mediumblob';
             default:                            return 'binary';
           }
         case 'bit':                             return 'tinyint(1)';
         case 'char':
           switch (true) {
             case (dataTypeLength === -1):       return 'longtext';
             case (dataTypeLength > 16777215):   return 'longtext';
             case (dataTypeLength > 65535):      return 'mediumtext';
              case (dataTypeLength > 255):       return 'text';
              default:                           return 'char';
           }
         case 'datetime':                        return 'datetime(3)';
         case 'datetime2':                       return 'datetime';
         case 'datetimeoffset':                  return 'datetime';
         case 'geography':                       return 'json';
         case 'geogmetry':                       return 'json';
         case 'hierarchyid':                     return 'varbinary(446)';
         case 'image':                           return 'longblob';
         case 'mediumint':                       return 'int';
         case 'money':                           return 'decimal(19,4)';
         case 'nchar':
           switch (true) {
              case (dataTypeLength === -1):      return 'longtext';
              case (dataTypeLength > 16777215):  return 'longtext';
              case (dataTypeLength > 65535):     return 'mediumtext';
              case (dataTypeLength > 255):       return 'text';
              default:                           return 'char';
           }
         case 'ntext':                           return 'longtext';
         case 'nvarchar':
           switch (true) {
             case (dataTypeLength === -1):       return 'longtext';
             case (dataTypeLength > 16777215):   return 'longtext';
             case (dataTypeLength > 65535):      return 'mediumtext';
             default:                            return 'varchar';
           }             
         case 'real':                            return 'float';
         case 'rowversion':                      return 'binary(8)';
         case 'smalldate':                       return 'datetime';
         case 'smallmoney':                      return 'decimal(10,4)';
         case 'text':                            return 'longtext';
         case 'tinyint':                         return 'smallint';
         case 'uniqueidentifier':                return 'varchar(64)';
         case 'varbinary':
           switch (true) {
             case (dataTypeLength === -1):       return 'longblob';
             case (dataTypeLength > 16777215):   return 'longblob';
             case (dataTypeLength > 65535):      return 'mediumblob';
             default:                            return 'varbinary';
           }
         case 'varchar':
           switch (true) {
             case (dataTypeLength === -1):       return 'longtext';
             case (dataTypeLength > 16777215):   return 'longtext';
             case (dataTypeLength > 65535):      return 'mediumtext';
             default:                            return 'varchar';
           }
         case 'xml':                             return 'longtext';
         default:                                return dataType.toLowerCase();
       }
       break;
     case 'Postgres':                            return dataType.toLowerCase();
       break
     case 'MySQL':
       switch (dataType) {
         case 'set':                             return 'varchar(512)';
         case 'enum':                            return 'varchar(512)';
         default:                                return dataType.toLowerCase();
       }
       break;
     case 'MariaDB':
       switch (dataType) {
         case 'set':                             return 'varchar(512)';
         case 'enum':                            return 'varchar(512)';
         default:                                return dataType.toLowerCase();
       }
       break;
     default:                                    return dataType.toLowerCase();
  }  
}

function getColumnDataType(targetDataType, length, scale) {

   if (RegExp(/\(.*\)/).test(targetDataType)) {
     return targetDataType
   }
   
   if (targetDataType.endsWith(" unsigned")) {
     return targetDataType
   }

   if (unboundedTypes.includes(targetDataType)) {
     return targetDataType
   }

   if (spatialTypes.includes(targetDataType)) {
     return targetDataType
   }

   if (nationalTypes.includes(targetDataType)) {
     return targetDataType
   }

   if (scale) {
     if (integerTypes.includes(targetDataType)) {
       return targetDataType + '(' + length + ')';
     }
     return targetDataType + '(' + length + ',' + scale + ')';
   }                                                   

   if (length) {
     if (targetDataType === 'double')  {
       return targetDataType
     }
     if (length)
     return targetDataType + '(' + length + ')';
   }

   return targetDataType;     
}
    
function generateStatements(vendor, schema, metadata) {
    
   let useSetClause = false;
   
   const columnNames = metadata.columns.split(',');
   const dataTypes = metadata.dataTypes
   const sizeConstraints = metadata.sizeConstraints
   const targetDataTypes = [];
   const setOperators = []

   const columnClauses = columnNames.map(function(columnName,idx) {    
                                           
                                           const dataType = {
                                                    type : dataTypes[idx]
                                                 }    
                                           
                                           const sizeConstraint = sizeConstraints[idx]
                                           if (sizeConstraint.length > 0) {
                                              const components = sizeConstraint.split(',');
                                              dataType.length = parseInt(components[0])
                                              if (components.length > 1) {
                                                dataType.scale = parseInt(components[1])
                                              }
                                           }
                                        
                                           let targetDataType = mapForeignDataType(vendor,dataType.type,dataType.length,dataType.scale);
                                       
                                           targetDataTypes.push(targetDataType);
                                           
                                           switch (targetDataType) {
                                             case 'geometry':
                                                useSetClause = true;
                                                setOperators.push(' "' + columnName + '" = ST_GEOMFROMGEOJSON(?)');
                                                break;
                                                
                                             default:
                                               setOperators.push(' "' + columnName + '" = ?')
                                           }
                                           return `${columnName} ${getColumnDataType(targetDataType,dataType.length,dataType.scale)}`
                                        })
                                      
    const createStatement = `create table if not exists "${schema}"."${metadata.tableName}"(\n  ${columnClauses.join(',')})`;
    let insertStatement = `insert into "${schema}"."${metadata.tableName}"`;
    if (useSetClause) {
      insertStatement += ` set` + setOperators.join(',');
    }
    else {
      insertStatement += `(${metadata.columns}) values ?`;
    }
    return { ddl : createStatement, dml : insertStatement, targetDataTypes : targetDataTypes, useSetClause : useSetClause}
}

async function generateStatementCacheLocal(conn, schema, systemInformation, metadata, status, logWriter) {

  const statementCache = {}
  const tables = Object.keys(metadata); 
  await Promise.all(tables.map(async function(table,idx) {
                                       const tableMetadata = metadata[table];
                                       const sql = generateStatements(systemInformation.vendor, schema,tableMetadata);
                                       statementCache[table] = sql;
                                       try {
                                         if (status.sqlTrace) {
                                           status.sqlTrace.write(`${statementCache[table].ddl};\n--\n`);
                                         }
                                         const results = await conn.query(statementCache[table].ddl);   
                                       } catch (e) {
                                         logWriter.write(`${e}\n${statementCache[table].ddl}\n`)
                                       }
  }))
  return statementCache;
}

async function generateStatementCacheRemote(conn, schema, systemInformation, metadata, status,logWriter) {
    
  const sqlStatement = `SET @RESULTS = '{}'; CALL GENERATE_STATEMENTS(?,?,@RESULTS); SELECT @RESULTS "SQL_STATEMENTS";`;                       
 
  let results = await query(conn,sqlStatement,[JSON.stringify({systemInformation: systemInformation, metadata : metadata}),schema]);
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
                                        
                                       if (status.sqlTrace) {
                                         status.sqlTrace.write(`${tableInfo.ddl};\n--\n`);
                                       }
                                       try {
                                         const results = await query(conn,tableInfo.ddl);   
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
    this.status = status;
    this.logWriter = logWriter;

    this.systemInformation = undefined;
    this.metadata = undefined;
    this.statementCache = undefined;
    
    this.tableName = undefined;
    this.tableInfo = undefined;
    this.rowCount = undefined; 
    this.startTime = undefined;
    this.skipTable = true;
    
    this.batch = [];
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
            const results = await query(this.conn,this.tableInfo.dml,this.batch[i]);
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
        const results = await query(this.conn,this.tableInfo.dml,[this.batch]);
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
          this.statementCache = await generateStatementCache(this.conn, this.schema, this.systemInformation, this.metadata, this.status, this.logWriter)
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
          if (this.status.sqlTrace) {
             this.status.sqlTrace.write(`${this.tableInfo.dml};\n--\n`);
          }
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

function processFile(conn, schema, dumpFilePath, batchSize, commitSize, mode, status, logWriter) {
  
  return new Promise(function (resolve,reject) {
    const dbWriter = new DbWriter(conn,schema,batchSize,commitSize,mode,status,logWriter);
    const rowGenerator = new RowParser(logWriter);
    const readStream = fs.createReadStream(dumpFilePath);    
    dbWriter.on('finish', function() { resolve()});
    readStream.pipe(rowGenerator).pipe(dbWriter);
  })
}
    
async function main() {

  let pool; 
  let conn;
  let parameters;
  let sqlTrace;
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
    
    parameters = MySQLCore.processArguments(process.argv,'export');

    if (parameters.LOGFILE) {
      logWriter = fs.createWriteStream(parameters.LOGFILE);
    }

    if (parameters.SQLTRACE) {
      status.sqlTrace = fs.createWriteStream(parameters.SQLTRACE);
    }
    
    const connectionDetails = {
            host      : parameters.HOSTNAME
           ,user      : parameters.USERNAME
           ,password  : parameters.PASSWORD
           ,database  : parameters.DATABASE
           ,multipleStatements: true
    }

    conn = mysql.createConnection(connectionDetails);
    await connect(conn);

    const maxAllowedPacketSize = 1 * 1024 * 1024 * 1024;
    results = await query(conn,`SELECT @@max_allowed_packet`);
    
    if (parseInt(results[0]['@@max_allowed_packet']) <  maxAllowedPacketSize) {
        logWriter.write(`${new Date().toISOString()}: Increasing MAX_ALLOWED_PACKET to 1G.\n`);
        results = await query(conn,`SET GLOBAL max_allowed_packet=${maxAllowedPacketSize}`);
        await conn.end();
        conn = mysql.createConnection(connectionDetails);
        await connect(conn);
    }    

    results = await query(conn,`SELECT @@version`);
    if (results[0]['@@version'] > '8.0') {
       generateStatementCache = generateStatementCacheRemote
    }
    else {
      generateStatementCache = generateStatementCacheLocal
    }
  
    // Force 5.7 Code Path
    // generateStatementCache = generateStatementCacheLocal
 
    results = await query(conn,`SET SESSION SQL_MODE=ANSI_QUOTES`);
    results = await query(conn,`CREATE DATABASE IF NOT EXISTS "${parameters.TOUSER}"`); 
    
    const stats = fs.statSync(parameters.FILE)
    const fileSizeInBytes = stats.size
    
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
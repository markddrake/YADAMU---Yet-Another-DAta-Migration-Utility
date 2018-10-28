"use strict"
const { Transform } = require('stream');
const { Writable } = require('stream');
const mysql = require('mysql')
const common = require('./common.js');
const clarinet = require('c:/Development/github/clarinet/clarinet.js');
// const clarinet = require('clarinet');
const fs = require('fs');

const unboundedTypes = ['tinyint','smallint','mediumint','int','set','enum','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json'];
const spatialTypes = ['geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection'];
const nationalTypes = ['nchar','nvarchar'];
   
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

async function createTables(conn, schema, metadata, status) {
    
  const sqlStatement = `SET @RESULTS = '{}'; CALL GENERATE_STATEMENTS(?,?,@RESULTS); SELECT @RESULTS "SQL_STATEMENTS";`;					   
 
  let results = await query(conn,sqlStatement,[schema,JSON.stringify({metadata : metadata})]);
  results = results.pop();
  const statementCache = JSON.parse(results[0].SQL_STATEMENTS)
  const tables = Object.keys(metadata); 
  tables.forEach(async function(table,idx) {
                   const tableMetadata = metadata[table];
                   statementCache[table].dml = statementCache[table].dml.substr(0,statementCache[table].dml.indexOf(') select')+1) + "\nvalues ?";
                   try {
                     if (status.sqlTrace) {
                       status.sqlTrace.write(`${statementCache[table].ddl};\n--\n`);
                     }
                     const results = await query(conn,statementCache[table].ddl);   
                   } catch (e) {
                     console.log(e);
                   }  
  });
  
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

    this.mysqltemInformation = undefined;
    this.metadata = undefined;
    this.statementCache = undefined;
    
    this.tableName = undefined;
    this.insertStatement = undefined;
    this.rowCount = undefined; 
    this.startTime = undefined;
    this.skipTable = true;
    
    this.batch = [];
  }      
  
  async setTable(tableName) {
       
    this.tableName = tableName
    this.insertStatement =  this.statementCache[tableName].dml;
    this.rowCount = 0;
    this.batch.length = 0;
    this.tableLobIndex = 0;
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.skipTable = false;
  }
  
   async writeBatch(status) {
    try {
      const results = await query(this.conn,this.insertStatement,[this.batch]);
      const endTime = new Date().getTime();
      await this.conn.commit();
      this.batch.length = 0;
      return endTime
    } catch (e) {
      this.batch.length = 0;
      this.skipTable = true;
      this.status.warningRaised = true;
      this.logWriter.write(`${new Date().toISOString()}: Table ${this.tableName}. Skipping table. Reason: ${e.message}\n`)
      if (this.logDDLIssues) {
        this.logWriter.write(`${this.insertStatement}\n`);
        this.logWriter.write(`${JSON.stringify(this.args)}\n`);
        this.logWriter.write(`${this.batch}\n`);
      }      
    }
  }

  async _write(obj, encoding, callback) {
    try {
      switch (Object.keys(obj)[0]) {
        case 'mysqltemInformation':
          this.mysqltemInformation = obj.mysqltemInformation;
          break;
        case 'metadata':
          this.metadata = obj.metadata;
          this.statementCache = await createTables(this.conn, this.schema, this.metadata, this.status, this.logWriter);
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
             this.status.sqlTrace.write(`${this.insertStatement} ${this.args.slice(0,-1)};\n--\n`);
          }
          break;
        case 'data': 
          if (this.skipTable) {
            break;
          }
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
      const elapsedTime = new Date().getTime() - this.startTime;
      if (this.batch.length > 0) {
        // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batch.length} rows.`);
        this.endTime = await this.writeBatch();
        await this.conn.commit();
      }  
      if (this.tableName) {        
        if (!this.skipTable) {
          const elapsedTime = this.endTime - this.startTime;
          this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Rows ${this.rowCount}. Elaspsed Time ${Math.round(elapsedTime)}ms. Throughput ${Math.round((this.rowCount/Math.round(elapsedTime)) * 1000)} rows/s.\n`);
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
    
    parameters = common.processArguments(process.argv,'export');

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
  
    results = await query(conn,`SET SESSION SQL_MODE=ANSI_QUOTES`);
    results = await query(conn,`CREATE DATABASE IF NOT EXISTS "${parameters.TOUSER}"`);	
    
	const stats = fs.statSync(parameters.FILE)
    const fileSizeInBytes = stats.size
	
    if (parameters.LOGLEVEL) {
       status.loglevel = parameters.LOGLEVEL;
    }
    	
    await processFile(conn, parameters.TOUSER, parameters.FILE, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.MODE, status, logWriter);
    
    await conn.end();

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


 
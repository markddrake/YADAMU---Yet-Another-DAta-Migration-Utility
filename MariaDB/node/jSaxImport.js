"use strict"
const { Transform } = require('stream');
const { Writable } = require('stream');
const mariadb = require('mariadb');
const common = require('./common.js');
const clarinet = require('c:/Development/github/clarinet/clarinet.js');
// const clarinet = require('clarinet');
const fs = require('fs');

const unboundedTypes = ['tinyint','smallint','mediumint','int','set','enum','tinytext','mediumtext','text','longtext','tinyblob','mediumblob','blob','longblob','json'];
const spatialTypes = ['geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection'];
const nationalTypes = ['nchar','nvarchar'];
   
function mapForeignDataType(dataType, dataTypeLength, dataTypeSize) {
  switch (dataType) {
    // TODO : Enable Roundtrip for ENUM and SET
	case 'set':
      return 'text';
	case 'enum':
     return 'text';
    // Oracle Mappings
	case 'VARCHAR2':
     return 'varchar';
	case 'NVARCHAR2':
     return 'nvarchar';
	case 'NUMBER':
      return 'decimal';
	case 'CLOB':
      return 'text';
	case 'NCLOB':
      return 'text';
	case 'BFILE':
      return 'varchar(2048)';
	case 'ROWID':
      return 'varchar(32)';
	case 'RAW':
      return 'varbinary';
 	case 'ROWID':
      return 'varchar(32)';
 	case 'ANYDATA':
      return 'longtext';
  // SQLServer Mapppings
	case 'nchar':
      return 'char';
	case 'tinyint':
      return 'tinyint unsigned';
	case 'bit':
      return 'tinyint(1)';
	case 'real':
      return 'float';
	case 'numeric':
      return 'decimal';
	case 'money':
      return 'decimal';
	case 'smallmoney':
      return 'decimal';
	case 'char':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longtext';
        case (dataTypeLength > 16777215):
          return 'longtext';
        case (dataTypeLength > 65535):
          return 'mediumtext';
         case (dataTypeLength > 255):
           return 'text';
         default:
           return 'char';
      }
	case 'nchar':
      switch (true) {
         case (dataTypeLength === -1):
           return 'longtext';
         case (dataTypeLength > 16777215):
           return 'longtext';
         case (dataTypeLength > 65535):
           return 'mediumtext';
         case (dataTypeLength > 255):
           return 'text';
         default:
          return 'char';
      }
	case 'nvarchar':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longtext';
        case (dataTypeLength > 16777215):
          return 'longtext';
        case (dataTypeLength > 65535):
          return 'mediumtext';
        default:
          return 'varchar';
      }
	case 'varchar':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longtext';
        case (dataTypeLength > 16777215):
          return 'longtext';
        case (dataTypeLength > 65535):
          return 'mediumtext';
        default:
           return 'varchar';
      }
	case 'datetime2':
      return 'datatime';
	case 'smalldate':
      return 'datatime';
	case 'datetimeoffset':
      return 'datatime';
	case 'rowversion':
      return 'datatime';
	case 'binary':
      switch (true) {
        case (dataTypeLength > 16777215):
          return 'longblob';
        case (dataTypeLength > 65535):
          return 'mediumblob';
        case (dataTypeLength > 255):
          return 'blob';
        default:
          return 'tinyblob';
      }
	case 'varbinary':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longblob';
        case (dataTypeLength > 16777215):
          return 'longblob';
        case (dataTypeLength > 65535):
          return 'mediumblob';
        default:
          return 'varbinary';
      }
	case 'text':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longtext';
        case (dataTypeLength > 16777215):
          return 'longtext';
        case (dataTypeLength > 65535):
          return 'mediumtext';
        case (dataTypeLength > 255):
          return 'text';
        default:
          return 'char';
      }
	case 'ntext':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longtext';
        case (dataTypeLength > 16777215):
          return 'longtext';
        case (dataTypeLength > 65535):
          return 'mediumtext';
        case (dataTypeLength > 255):
          return 'text';
        default:
          return 'char';
      }
	case 'image':
      switch (true) {
        case (dataTypeLength === -1):
          return 'longblob';
        case (dataTypeLength > 16777215):
          return 'longblob';
        case (dataTypeLength > 65535):
          return 'mediumblob';
        case (dataTypeLength > 255):
          return 'blob';
        default:
          return 'tinyblob';
      }
	case 'uniqueidentifier':
      return 'varchar(64)';
	case 'hierarchyid':
      return 'varbinary(446)';
	case 'xml':
      return 'longtext';
	case 'geography':
      return 'longtext';
	default:
   	  if (dataType.indexOf('TIME ZONE') > -1) {
	    return 'timestamp';	
      }
  	  if (dataType.indexOf('XMLTYPE') > -1) { 
	    return 'longtext';
      }
	  if (dataType.indexOf('.') > -1) { 
	    return 'longtext';
      }
      if (spatialTypes.includes(dataType)) {
        return 'text';
      }
   	  if ((dataType.indexOf('INTERVAL') === 0)) {
	    return 'varchar(16)';
      }
	  return dataType.toLowerCase();
  }
}
    
function generateStatements(schema, metadata) {
    
   const columnNames = metadata.columns.split(',');
   const dataTypes = metadata.dataTypes.split(',');
   const sizeConstraints = JSON.parse('[' + metadata.dataTypeSizing.replace(/\"\.\"/g, '\",\"') + ']');
   const argsList = [];
   const columnClauses = columnNames.map(function(columnName,idx) {    
                                           const dataType = dataTypes[idx].replace(/\"/g, "");
                                           const sizeConstraint = sizeConstraints[idx].replace(/\"/g, "");
                                           let dataLength = null;
                                           let dataScale = null;
                                           let qualifier = ''
                                         
                                           if (sizeConstraint.length > 0) {
                                             dataLength = sizeConstraint;
                                             const scaleOffset = dataLength.indexOf(',');
                                             if (scaleOffset > -1) {
                                               dataScale = parseInt(dataLength.substring(scaleOffset+1))
                                               dataLength = parseInt(dataLength.substring(0,scaleOffset))
                                             }
                                             else {
                                               dataLength = parseInt(dataLength);
                                             }
                                           }
                                           let targetDataType = mapForeignDataType(dataType,dataLength,dataScale);
   
                                           switch (true) {
                                              case (targetDataType.indexOf('(') > -1):
                                                break;
                                              case (targetDataType.endsWith(" unsigned")):
                                                break;
                                              case unboundedTypes.includes(targetDataType):
                                                break;
                                              case spatialTypes.includes(targetDataType):
                                                break;
                                              case nationalTypes.includes(targetDataType):
                                                targetDataType = targetDataType + '(' + dataLength + ')';
                                                break;
                                              case (dataScale != null):
                                                targetDataType = targetDataType + '(' + dataLength + ',' + dataScale + ')';
                                                break;
                                              case (dataLength != null):
                                                switch (true) {
                                                  case (targetDataType === 'double'):
                                                    targetDataType = targetDataType + '(' + dataLength + ',0)';
                                                    break;
                                                  default:
                                                    targetDataType = targetDataType + '(' + dataLength + ')';
                                                    break;
                                                }
                                                break;
                                              default:
                                           }
                                           
                                           switch (true) {
                                             default:
                                               argsList[idx] = '?';
                                           }
                                            
                                           return `${columnName} ${targetDataType} ${qualifier}\n `;
                                        })
                                       
    
    const createStatement = `create table if not exists "${schema}"."${metadata.tableName}"(\n  ${columnClauses.join(',')})`;
    const insertStatement = `insert into "${schema}"."${metadata.tableName}"(${metadata.columns}) values`

    // The extra comma on the end of the args list allows it to be easily replicated when performing batch inserts
    
    return { ddl : createStatement, dml : { sql : insertStatement, args : '(' + argsList.join(',') + '),'}}
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

async function generateStatementCache(conn, schema, metadata, status, logWriter) {
    
  const ddlStatements = []
  const dmlStatements = {};
  const tables = Object.keys(metadata); 
  tables.forEach(function(table,idx) {
                   const tableMetadata = metadata[table];
                   const sql = generateStatements(schema,tableMetadata);
                   ddlStatements[idx] = sql.ddl;
                   dmlStatements[table] =  sql.dml
  });
  
  for (let i=0; i<ddlStatements.length;i++) {
    try {
      if (status.sqlTrace) {
        status.sqlTrace.write(`${ddlStatements[i]};\n--\n`);
      }
      const results = await conn.query(ddlStatements[i]);   
    } catch (e) {
      logWriter.write(`${e}\n${statementCache[table].ddl}\n`)
    }
  }
  return dmlStatements;
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
    this.insertStatement = undefined;
    this.args = undefined;
    this.rowCount = undefined; 
    this.startTime = undefined;
    this.skipTable = true;
    
    this.batch = [];
    this.batchRowCount = 0;
  }      
  
  async setTable(tableName) {
       
    this.tableName = tableName
    this.insertStatement =  this.statementCache[tableName].sql;
    this.args =  this.statementCache[tableName].args;
    this.rowCount = 0;
    this.batch.length = 0;
    this.tableLobIndex = 0;
    this.startTime = new Date().getTime();
    this.endTime = undefined;
    this.skipTable = false;
  }
  
   async writeBatch(status) {
    try {
      // Slice removes the unwanted last comma from the replicated args list.
      const args = this.args.repeat(this.batchRowCount).slice(0,-1); 
      const results = await this.conn.query(this.insertStatement+args,this.batch);
      const endTime = new Date().getTime();
      this.batch.length = 0;
      this.batchRowCount = 0;
      return endTime
    } catch (e) {
      this.batch.length = 0;
      this.batchRowCount = 0;
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
        case 'systemInformation':
          this.systemInformation = obj.systemInformation;
          break;
        case 'metadata':
          this.metadata = obj.metadata;
          this.statementCache = await generateStatementCache(this.conn, this.schema, this.metadata, this.status, this.logWriter);
          break;
        case 'table':
          // this.logWriter.write(`${new Date().toISOString()}: Switching to Table "${obj.table}".\n`);
          if (this.tableName !== undefined) {
            if (this.batchRowCount > 0) {
              // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batchRowCount} rows.`);
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
          this.batch.push(...obj.data);
          this.batchRowCount++;
          //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Batch contains ${this.batchRowCount} rows.`);
          if (this.batchRowCount  === this.batchSize) {
              //  this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Completed Batch contains ${this.batchRowCount} rows.`);
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
          if (this.batchRowCount > 0) {
            // this.logWriter.write(`${new Date().toISOString()}: Table "${this.tableName}". Final Batch contains ${this.batchRowCount} rows.`);
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
           ,port      : parameters.PORT ? parameters.PORT : 3307
           ,database  : parameters.DATABASE
           ,multipleStatements: true
    }
    
    pool = mariadb.createPool(connectionDetails);
    conn = await pool.getConnection();
    const maxAllowedPacketSize = 1 * 1024 * 1024 * 1024;
    results = await conn.query(`SHOW variables like 'max_allowed_packet'`);

    if (parseInt(results[0].Value) <  maxAllowedPacketSize) {
        logWriter.write(`${new Date().toISOString()}: Increasing MAX_ALLOWED_PACKET to 1G.\n`);
        results = await conn.query(`SET GLOBAL max_allowed_packet=${maxAllowedPacketSize}`);
        await conn.end();
        await pool.end();
        pool = mariadb.createPool(connectionDetails);
        conn = await pool.getConnection();
    }
  
    results = await conn.query(`SET SESSION SQL_MODE=ANSI_QUOTES`);
    results = await conn.query(`CREATE DATABASE IF NOT EXISTS "${parameters.TOUSER}"`);	
    
	const stats = fs.statSync(parameters.FILE)
    const fileSizeInBytes = stats.size
	
    if (parameters.LOGLEVEL) {
       status.loglevel = parameters.LOGLEVEL;
    }
    	
    await processFile(conn, parameters.TOUSER, parameters.FILE, parameters.BATCHSIZE, parameters.COMMITSIZE, parameters.MODE, status, logWriter);
    
    await conn.end();
    await pool.end();

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
    if (pool !== undefined) {
	  await pool.end();
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


 
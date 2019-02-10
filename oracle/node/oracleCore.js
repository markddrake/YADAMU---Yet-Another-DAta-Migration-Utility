"use strict" 
const fs = require('fs');
const Readable = require('stream').Readable;

const oracledb = require('oracledb');
oracledb.fetchAsString = [ oracledb.DATE ]

const Yadamu = require('../../common/yadamuCore.js');

const dateFormatMasks = {
        Oracle      : 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
       ,MSSQLSERVER : 'YYYY-MM-DD"T"HH24:MI:SS.###"Z"'
       ,Postgres    : 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
       ,MySQL       : 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
       ,MariaDB     : 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
}

const timestampFormatMasks = {
        Oracle      : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"'
       ,MSSQLSERVER : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"'
       ,Postgres    : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"+00:00"'
       ,MySQL       : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"'
       ,MariaDB     : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"'
}

function getDateFormatMask(vendor) {
    
 return dateFormatMasks[vendor]
 
}

function getTimeStampFormatMask(vendor) {
    
 return timestampFormatMasks[vendor]
 
}

async function setDateFormatMask(conn,status,vendor) {
 
  let sqlStatement = `ALTER SESSION SET NLS_DATE_FORMAT = '${dateFormatMasks[vendor]}'`
  if (status.sqlTrace) {
     status.sqlTrace.write(`${sqlStatement}\n/\n`);
  }
  let result = await conn.execute(sqlStatement);

  sqlStatement = `ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '${timestampFormatMasks[vendor]}'`
  if (status.sqlTrace) {
     status.sqlTrace.write(`${sqlStatement}\n/\n`);
  }
  result = await conn.execute(sqlStatement);

}
 
async function configureConnection(conn,status) {
  let sqlStatement = `ALTER SESSION SET TIME_ZONE = '+00:00'`
  if (status.sqlTrace) {
     status.sqlTrace.write(`${sqlStatement}\n/\n`);
  }
  let result = await conn.execute(sqlStatement);

  await setDateFormatMask(conn,status,'Oracle');
  
  sqlStatement = `ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM'`
  if (status.sqlTrace) {
     status.sqlTrace.write(`${sqlStatement}\n/\n`);
  }
  result = await conn.execute(sqlStatement);

  sqlStatement = `ALTER SESSION SET NLS_LENGTH_SEMANTICS = 'CHAR'`
  if (status.sqlTrace) {
     status.sqlTrace.write(`${sqlStatement}\n/\n`);
  }
  result = await conn.execute(sqlStatement);

}    

async function getConnection(connectionDetails,status) {
   	
  if (typeof connectionDetails === 'string') {
    connectionDetails = convertConnectionString(connectionDetails)
  }
	
  const conn = await oracledb.getConnection(connectionDetails)
  await configureConnection(conn,status);
  return conn;
}

function convertConnectionString(connectionString) {
    
  const user = Yadamu.convertQuotedIdentifer(connectionString.substring(0,connectionString.indexOf('/')));
  let password = connectionString.substring(connectionString.indexOf('/')+1);
  let connectString = '';
  if (password.indexOf('@') > -1) {
	connectString = password.substring(password.indexOf('@')+1);
	password = password.substring(password,password.indexOf('@'));
  }
  return {
      user          : user,
      password      : password,
      connectString : connectString
  }
}

async function getConnectionPool(connectionDetails) {
    
  if (typeof connectionDetails === 'string') {
    connectionDetails = convertConnectionString(connectionDetails)
  }
  const pool = await oracledb.createPool(connectionDetails)
  return pool;
}

async function getConnectionFromPool(pool,status) {

  const conn = pool.getConnection();
  await configureConnection(conn.status);
  return conn;
  
}

async function releaseConnection(conn) {
  try {
    await conn.close();
  } catch (e) {
    console.log(e);
  }
};

function closeTempLob (tempLob) {
 templob.close();
};

function lobFromStream (conn,inStream) {

  return new Promise(async function(resolve,reject) {
    const tempLob =  await conn.createLob(oracledb.BLOB);
    tempLob.on('error',function(err) {reject(err);});
    tempLob.on('finish', function() {resolve(tempLob);});
    inStream.on('error', function(err) {reject(err);});
    inStream.pipe(tempLob);  // copies the text to the temporary LOB
  });  
};

function lobFromFile (conn,filename) {
   const inStream = fs.createReadStream(filename);
   return lobFromStream(conn,inStream);
};

function lobFromJSON(conn,json) {
  
  const s = new Readable();
  s.push(JSON.stringify(json));
  s.push(null);
   
  return lobFromStream(conn,s);
};

async function writeClobToFile(lob, filename) {
	
  return new Promise(function(resolve,reject) {
    lob.setEncoding('utf8');  // set the encoding so we get a 'string' not a 'buffer'
    var errorHandled = false;

    lob.on('error', function(err) {lob.close(function() {reject(err);})});
    lob.on('finish', function() {resolve();});
	  
    var outStream = fs.createWriteStream(filename);
    outStream.on(
      'error',
      function(err) {
        // console.log("outStream.on 'error' event");
        if (!errorHandled) {
          errorHandled = true;
          lob.close(function() {
            reject(err);
          });
        }
      });

    // Switch into flowing mode and push the LOB to the file
    lob.pipe(outStream);
  });
};

async function setCurrentSchema(conn, schema, status, logWriter) {

  const sqlStatement = `begin :log := JSON_IMPORT.SET_CURRENT_SCHEMA(:schema); end;`;
     
  try {
    const results = await conn.execute(sqlStatement,{log:{dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 1024} , schema:schema});
    const log = JSON.parse(results.outBinds.log);
    if (log !== null) {
      Yadamu.processLog(log, status, logWriter)
    }
  } catch (e) {
    logWriter.write(`${e}\n${e.stack}\n`);
  }    
}

function processArguments(args) {

   const parameters = {
	                 FILE         : "export.json"
                    ,MODE         : "DDL_AND_DATA"
                    ,BATCHSIZE    : 1000000
                    ,COMMITSIZE   : 1000000
                    ,LOBCACHESIZE : 512
   }

   process.argv.forEach(function (arg) {
	   
	 if (arg.indexOf('=') > -1) {
       const parameterName = arg.substring(0,arg.indexOf('='));
	   const parameterValue = arg.substring(arg.indexOf('=')+1);
	    switch (parameterName.toUpperCase()) {
	      case 'FILE':
	        parameters.FILE = parameterValue;
			break;
	      case 'USERID':
  	        parameters.USERID = parameterValue;
			break;
	      case 'OWNER':
		    parameters.OWNER = Yadamu.processValue(parameterValue);
			break;
	      case 'FROMUSER':
		    parameters.FROMUSER = Yadamu.processValue(parameterValue);
			break;
	      case 'TOUSER':
		    parameters.TOUSER = Yadamu.processValue(parameterValue);
			break;
	      case 'LOGFILE':
		    parameters.LOGFILE = parameterValue;
			break;
	      case 'SQLTRACE':
		    parameters.SQLTRACE = parameterValue;
			break;
	      case 'LOGLEVEL':
		    parameters.LOGLEVEL = parameterValue;
			break;
	      case 'DUMPFILE':
		    parameters.DUMPFILE = parameterValue.toUpperCase();
			break;
	      case 'BATCHSIZE':
		    parameters.BATCHSIZE = parseInt(parameterValue)
			break;
	      case 'COMMITSIZE':
		    parameters.COMMITSIZE = parseInt(parameterValue)
			break;	      
	      case 'LOBCACHESIZE':
		    parameters.LOBCACHESIZE = parseInt(parameterValue)
			break;	      
          case 'MODE':
		    parameters.MODE = parameterValue.toUpperCase();
			break;
		  default:
		    console.log(`Unknown parameter: "${parameterName}".`)			
	   }
	 }
   })
   
   return parameters;
}

module.exports.getConnection          = getConnection
module.exports.releaseConnection      = releaseConnection
module.exports.closeTempLob           = closeTempLob
module.exports.lobFromStream          = lobFromStream
module.exports.lobFromFile            = lobFromFile
module.exports.lobFromJSON            = lobFromJSON
module.exports.writeClobToFile        = writeClobToFile
module.exports.processArguments       = processArguments
module.exports.setDateFormatMask      = setDateFormatMask
module.exports.getDateFormatMask      = getDateFormatMask
module.exports.getTimeStampFormatMask = getTimeStampFormatMask
module.exports.setCurrentSchema       = setCurrentSchema


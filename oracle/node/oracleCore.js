"use strict" 
const fs = require('fs');
const oracledb = require('oracledb');
const Readable = require('stream').Readable;

const Yadamu = require('../../common/yadamuCore.js');

async function doConnect(connectionString,status) {
	
  const user = Yadamu.convertQuotedIdentifer(connectionString.substring(0,connectionString.indexOf('/')));
  let password = connectionString.substring(connectionString.indexOf('/')+1);
  let connectString = '';
  if (password.indexOf('@') > -1) {
	connectString = password.substring(password.indexOf('@')+1);
	password = password.substring(password,password.indexOf('@'));
  }
  const conn = await oracledb.getConnection(
  {
      user          : user,
      password      : password,
      connectString : connectString
  });
  const sqlStatement = `ALTER SESSION SET TIME_ZONE = '+00:00'`
  if (status.sqlTrace) {
     status.sqlTrace.write(`${sqlStatement}\n/\n`);
  }
  const result = await conn.execute(sqlStatement);
  return conn;
}

function doRelease(conn) {
  conn.close(function (err) {
    if (err)
      console.error(err.message);
  });
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
	      case 'DUMPLOG':
		    parameters.DUMPLOG = parameterValue.toUpperCase();
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

module.exports.doConnect              = doConnect
module.exports.doRelease              = doRelease
module.exports.closeTempLob           = closeTempLob
module.exports.lobFromStream          = lobFromStream
module.exports.lobFromFile            = lobFromFile
module.exports.lobFromJSON            = lobFromJSON
module.exports.writeClobToFile        = writeClobToFile
module.exports.processArguments       = processArguments
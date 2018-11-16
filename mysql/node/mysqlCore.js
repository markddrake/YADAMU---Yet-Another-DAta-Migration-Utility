"use strict";

const Yadamu = require('../../common/yadamuCore.js');

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
      
function query(conn,status,sqlQuery,args) {
    
  return new Promise(function(resolve,reject) {
                       if (status.sqlTrace) {
                         status.sqlTrace.write(`${sqlQueryPacketSize};\n--\n`);
                       }
                       conn.query(sqlQuery,args,function(err,rows,fields) {
                                             if (err) {
                                               reject(err);
                                             }
                                             resolve(rows);
                                           })
                     })
}  

async function configureSession(conn,status) {

   const sqlAnsiQuotes = `SET SESSION SQL_MODE=ANSI_QUOTES`;
   await query(conn,status,sqlAnsiQuotes);
   
   const sqlTimeZone = `SET TIME_ZONE = '+00:00'`;
   await query(conn,status,sqlTimeZone);

}

async function setMaxAllowedPacketSize(conn,status,logWriter) {

  const maxAllowedPacketSize = 1 * 1024 * 1024 * 1024;
  const sqlQueryPacketSize = `SELECT @@max_allowed_packet`;
  const sqlSetPacketSize = `SET GLOBAL max_allowed_packet=${maxAllowedPacketSize}`
    
  let results = await query(conn,status,sqlQueryPacketSize);
    
  if (parseInt(results[0]['@@max_allowed_packet']) <  maxAllowedPacketSize) {
    logWriter.write(`${new Date().toISOString()}: Increasing MAX_ALLOWED_PACKET to 1G.\n`);
    results = await query(conn,status,sqlSetPacketSize);
    await conn.end();
    return true;
  }    
  return false;
}

async function createTargetDatabase(conn,status,schema) {    	
	const sqlStatement = `CREATE DATABASE IF NOT EXISTS "${schema}"`;					   
	const results = await query(conn,status,sqlStatement,schema);
	return results;
}


function processArguments(args,operation) {

   const parameters = {
	                 FILE : "export.json"
                    ,MODE : "DDL_AND_CONTENT"
                    ,BATCHSIZE  : 100
                    ,COMMITROWS : 1000
   }

   process.argv.forEach(function (arg) {
	   
	 if (arg.indexOf('=') > -1) {
       const parameterName = arg.substring(0,arg.indexOf('='));
	   const parameterValue = arg.substring(arg.indexOf('=')+1);
	    switch (parameterName.toUpperCase()) {
	      case 'DATABASE':
	      case '--DATABASE':
  	        parameters.DATABASE = parameterValue;
			break;
	      case 'HOSTNAME':
	      case '--HOSTNAME':
  	        parameters.HOSTNAME = parameterValue;
			break;
	      case 'HOSTNAME':
	      case '--HOSTNAME':
  	        parameters.HOSTNAME = parameterValue;
			break;
	      case 'PORT':
	      case '--PORT':
	        parameters.PORT = parameterValue;
			break;
	      case 'PASSWORD':
	      case '--PASSWORD':
	        parameters.PASSWORD = parameterValue;
			break;
	      case 'USERNAME':
	      case '--USERNAME':
	        parameters.USERNAME = parameterValue;
			break;
	      case 'FILE':
	      case '--FILE':
	        parameters.FILE = parameterValue;
			break;
	      case 'OWNER':
	      case '--OWNER':
		    parameters.OWNER = Yadamu.processValue(parameterValue);
			break;
	      case 'FROMUSER':
	      case '--FROMUSER':
		    parameters.FROMUSER = Yadamu.processValue(parameterValue);
			break;
	      case 'TOUSER':
	      case '--TOUSER':
		    parameters.TOUSER = Yadamu.processValue(parameterValue);
			break;
	      case 'LOGFILE':
	      case '--LOGFILE':
		    parameters.LOGFILE = parameterValue;
			break;
	      case 'SQLTRACE':
	      case '--SQLTRACE':
		    parameters.SQLTRACE = parameterValue;
			break;
	      case 'LOGLEVEL':
	      case '--LOGLEVEL':
		    parameters.LOGLEVEL = parameterValue;
			break;
	      case 'DUMPLOG':
	      case '--DUMPLOG':
		    parameters.DUMPLOG = parameterValue.toUpperCase();
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

module.exports.processArguments        = processArguments
module.exports.connect                 = connect
module.exports.configureSession        = configureSession
module.exports.query                   = query
module.exports.setMaxAllowedPacketSize = setMaxAllowedPacketSize
module.exports.createTargetDatabase    = createTargetDatabase

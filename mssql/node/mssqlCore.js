"use strict";
const sql = require('mssql');

const Yadamu = require('../../common/yadamuCore.js');

function processArguments(args,operation) {

 const parameters = {
	 FILE : "export.json"
					,PORT : 1433
					,OWNER : 'dbo'
					,TOUSER : 'dbo'
					,FROMUSER : 'dbo'
 ,MODE : "DDL_AND_CONTENT"
 ,BATCHSIZE : 500
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

async function getConnectionPool(parameters,status) {

  const config = {
          server          : parameters.HOSTNAME
         ,user            : parameters.USERNAME
         ,database        : parameters.DATABASE
         ,password        : parameters.PASSWORD
         ,port            : parameters.PORT
         ,requestTimeout  : 2 * 60 * 60 * 10000
         ,options   : {
             encrypt: false // Use this if you're on Windows Azure
          }
        }
        
    const pool = new sql.ConnectionPool(config);
    await pool.connect()
    const statement = `SET QUOTED_IDENTIFIER ON`
    if (status.sqlTrace) {
      status.sqlTrace.write(`${statement}\n\/\n`)
    }
    await pool.query(statement);
    return pool;
    
}


module.exports.processArguments  = processArguments
module.exports.getConnectionPool = getConnectionPool
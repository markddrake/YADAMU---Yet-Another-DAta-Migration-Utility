"use strict";
const {Client} = require('pg')

const Yadamu = require('../../common/yadamuCore.js');

function processArguments(args) {

   const parameters = {
	                 FILE     : "export.json"
                    ,MODE     : "DDL_AND_CONTENT"
					,USERNAME : 'postgres'
					,PASSWORD : null
					,HOSTNAME : 'localhost'
					,DATABASE : 'postgres'
					,PORT     : 5432
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
	      case 'DUMPFILE':
	      case '--DUMPFILE':
		    parameters.DUMPFILE = parameterValue.toUpperCase();
			break;
  	  	  case 'MODE':
	      case '--MODE':
		    parameters.MODE = parameterValue.toUpperCase();
			break;
		  default:
		    console.log(`Unknown parameter: "${parameterName}".`)			
	   }
	 }
   })
   
   return parameters;
}

async function testConnection(connectionDetails) {

  const pgClient = new Client(connectionDetails);
  await pgClient.connect();
  return pgClient;
}

async function getClient(parameters,logWriter,status) {

  const connectionDetails = {
    user      : parameters.USERNAME
   ,host      : parameters.HOSTNAME
   ,database  : parameters.DATABASE
   ,password  : parameters.PASSWORD
   ,port      : parameters.PORT
  }

  const pgClient = new Client(connectionDetails);
  await pgClient.connect();

  pgClient.on('notice',function(n){ 
	                     const notice = JSON.parse(JSON.stringify(n));
                         switch (notice.code) {
                           case '42P07': // Table exists on Create Table if not exists
                             break;
                           case '00000': // Table not found on Drop Table if exists
		                     break;
                           default:
                             logWriter.write(`${new Date().toISOString()}[Notice]:${n}\n`);
                         }
  })
  
  const setTimezone = `set timezone to 'UTC'`
  if (status.sqlTrace) {
    status.sqlTrace.write(`${setTimezone}\n\/\n`)
  }
  await pgClient.query(setTimezone);
  
  const setIntervalFormat =  `SET intervalstyle = 'iso_8601';`;
  if (status.sqlTrace) {
    status.sqlTrace.write(`${setIntervalFormat}\n\/\n`)
  }
  await pgClient.query(setIntervalFormat);

  return pgClient;
  
  
}

module.exports.processArguments   = processArguments
module.exports.testConnection     = testConnection
module.exports.getClient          = getClient

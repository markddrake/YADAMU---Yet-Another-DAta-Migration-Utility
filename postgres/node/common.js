module.exports.processArguments     = processArguments

function convertQuotedIdentifer(parameterValue) {

  if ((parameterValue.indexOf('"') === 0) && (parameterValue.substring(1).indexOf('"') === parameterValue.length - 2)) {
    return parameterValue.substring(1,parameterValue.length-1);
  }
  else {
    return parameterValue.toUpperCase()
  }	
}

function processValue(parameterValue) {

  if ((parameterValue.indexOf('(') === 0) && (parameterValue.indexOf(')') === parameterValue.length - 1)) {
	console.log(parameterValue)
	let parameterValues = parameterValue.substring(1,parameterValue.length-1).split(',');
	parameterValues = parameterValues.map(function(value) {
      return convertQutotedIdentifer(value);
	})
	console.log(parameterValues)
	return parameterValues
  }
  else {
    return convertQuotedIdentifer(parameterValue);
  }
}
	
function processArguments(args,operation) {

   const parameters = {
	                 FILE     : "export.json"
                    ,MODE     : "DDL_AND_CONTENT"
					,USERNAME : 'postgress'
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
		    parameters.OWNER = processValue(parameterValue);
			break;
	      case 'FROMUSER':
	      case '--FROMUSER':
		    parameters.FROMUSER = processValue(parameterValue);
			break;
	      case 'TOUSER':
	      case '--TOUSER':
		    parameters.TOUSER = processValue(parameterValue);
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
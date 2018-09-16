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
	                 FILE : "export.json"
                    ,MODE : "DDL_AND_CONTENT"
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
		    parameters.OWNER = processValue(parameterValue);
			break;
	      case 'FROMUSER':
		    parameters.FROMUSER = processValue(parameterValue);
			break;
	      case 'TOUSER':
		    parameters.TOUSER = processValue(parameterValue);
			break;
	      case 'LOGFILE':
		    parameters.LOGFILE = parameterValue;
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
module.exports.doConnect            = doConnect
module.exports.doRelease            = doRelease
module.exports.createTempLob        = createTempLob
module.exports.closeTempLob         = closeTempLob
module.exports.loadTempLobFromFile  = loadTempLobFromFile
module.exports.writeClobToFile      = writeClobToFile
module.exports.processArguments     = processArguments

const oracledb = require('oracledb');
const fs = require('fs');

function doConnect(connectionString) {
	
  const user = connectionString.substring(0,connectionString.indexOf('/'));
  let password = connectionString.substring(connectionString.indexOf('/')+1);
  let connectString = '';
  if (password.indexOf('@') > -1) {
	connectString = password.substring(password.indexOf('@')+1);
	password = password.substring(password,password.indexOf('@'));
  }
  return oracledb.getConnection(
  {
      user          : user,
      password      : password,
      connectString : connectString
  });
};

function doRelease(conn) {
  conn.close(function (err) {
    if (err)
      console.error(err.message);
  });
};

async function createTempLob(conn) {
  return conn.createLob(oracledb.BLOB);
};


function closeTempLob (tempLob) {
 templob.close();
};

function loadTempLobFromFile (conn,filename) {

  var errorHandled = false;

  return new Promise(async function(resolve,reject) {

    const tempLob =  await createTempLob(conn);
	
    tempLob.on(
      'close',
      function() {
        console.log("templob.on 'close' event");
    });

    tempLob.on(
      'error',
      function(err) {
        // console.log("templob.on 'error' event");
        if (!errorHandled) {
          errorHandled = true;
  		  console.log(err);
		  reject(err);
        }
    });

    tempLob.on(
      'finish',
      function() {
		// console.log(`loadTempLobFromFile("${filename}"): Read ${tempLob.iLob.offset-1} bytes.`);
        // The data was loaded into the temporary LOB
        if (!errorHandled) {
          resolve(tempLob);
        }
    });

    // console.log('Reading from ' + filename);
    var inStream = fs.createReadStream(filename);
    inStream.on(
      'error',
      function(err) {
        // console.log("inStream.on 'error' event");
        if (!errorHandled) {
        errorHandled = true;
		console.log(err);
        reject(err);
      }
    });
    inStream.pipe(tempLob);  // copies the text to the temporary LOB
  });  
};

async function writeClobToFile(lob, filename) {
	
  return new Promise(function(resolve,reject) {
    lob.setEncoding('utf8');  // set the encoding so we get a 'string' not a 'buffer'
    var errorHandled = false;

    lob.on(
      'error',
      function(err) {
        // console.log("lob.on 'error' event");
        if (!errorHandled) {
          errorHandled = true;
          lob.close(function() {
            reject(err);
          });
        }
      });

   lob.on(
      'end',
      function() {
        resolve();
      });
	  
    lob.on(
     'close',
      function() {
        // console.log("lob.on 'close' event");
        if (!errorHandled) {
          return
	    }
      });

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
                    ,MODE : "DDL_AND_DATA"
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
	      case 'SQLTRACE':
		    parameters.SQLTRACE = parameterValue;
			break;
	      case 'LOGLEVEL':
		    parameters.LOGLEVEL = parameterValue;
			break;
	      case 'DUMPLOG':
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
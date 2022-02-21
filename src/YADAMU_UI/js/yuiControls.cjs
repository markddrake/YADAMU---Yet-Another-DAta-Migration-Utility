"use strict"

// This file is required by the index.html file and will
// be executed in the renderer process for that window.
// All of the Node.js APIs are available in this process.

const electron = require('electron')
const { dialog } = require('@electron/remote')
const { ipcRenderer} = electron
const fs = require('fs')

window.$ = window.jQuery = require('jquery')

window.validSource = false;
window.validTarget = false;

window.configuration = { 
  connections          : {
    source             : undefined
  ,	target             : undefined
  }
, schemas              : {
    source             : undefined
  , target             : undefined	
  }
  ,"jobs"                            : [{
     "source"                       : {
        "connection"                : "source"
      , "schema"                    : "source"
      }
     ,"target"                      : {
        "connection"                : "target"
      , "schema"                    : "target"
      }
     ,"parameters"                  : {
        "MODE"                      : "DATA_ONLY"
      }
  }]
}

ipcRenderer.on('load-config', function (event, configuration) {
  loadConfiguration(configuration)
})

$('#source-tab a').on('click', function (e) {
  disableCopy(e.target.href.substring(e.target.href.indexOf('#')+1)+'-status')
})

$('#source-oracle-caseSensitive').change(function (e) {
  if (this.checked) {
	$('#source-oracle-user').removeClass('text-uppercase');
	$('#source-oracle-schema').removeClass('text-uppercase');
  }
  else {
	$('#source-oracle-user').addClass('text-uppercase');
	$('#source-oracle-schema').addClass('text-uppercase');
  }
})

$('#target-oracle-caseSensitive').change(function (e) {
  if (this.checked) {
	$('#target-oracle-user').removeClass('text-uppercase');
	$('#target-oracle-schema').removeClass('text-uppercase');
  }
  else {
	$('#target-oracle-user').addClass('text-uppercase');
	$('#target-oracle-schema').addClass('text-uppercase');
  }
})

function resetSourceState(statusID) {
  disableCopy(statusID);
  window.validSource = false;
} 

function resetTargetState(statusID) {
  disableCopy(statusID);
  window.validTarget = false;
} 

function disableCopy(statusID) {
  const buttonCopy = document.getElementById('doCopy');
  
  buttonCopy.disabled = true;
  
  const buttonSave = document.getElementById('save-config');
  buttonSave.disabled = true;
  
  const status = document.getElementById(statusID);
  if (status) {
    status.classList.remove('bi-check-circle')
    status.classList.remove('bi-times-circle')
    status.classList.add('bi-question-circle')
  }
}

function setCopyState() {
	
  const buttonCopy = document.getElementById('doCopy');
  buttonCopy.disabled = ((window.validSource === false) || (window.validTarget === false)) 

  const buttonSave = document.getElementById('save-config');
  buttonSave.disabled = ((window.validSource === false) || (window.validTarget === false)) 
  
}

async function selectSourceFile(control) {
	
  const options = {
    title : "Select File to upload", 
 
	filters :[
		{name: 'Exports', extensions: ['json', 'exp', 'dmp', 'dump']},
		{name: 'All Files', extensions: ['*']}
	],
	properties: ['openFile']
  }	
  const browseResults = await dialog.showOpenDialog(null,options)
  if (browseResults.cancelled !== true) {
    document.getElementById('source-filename').value = browseResults.filePaths[0]
    const parameters = {
	  FILE : document.getElementById('source-filename').value
    }
	ipcRenderer.send('source-filename',parameters);
	window.validSource = true;
	setCopyState();
  }
}

async function selectTargetFile(control) {
	
  const options = {
    title : "Select File to save", 
 
	filters :[
		{name: 'Exports', extensions: ['json', 'exp', 'dmp', 'dump']},
		{name: 'All Files', extensions: ['*']}
	],
	properties: ['openFile']
  }	
  const browseResults = await dialog.showSaveDialog(null,options)
  if (browseResults.cancelled !== true) {
    document.getElementById('target-filename').value = browseResults.filePath
    const parameters = {
	  FILE : document.getElementById('target-filename').value
    }
	ipcRenderer.send('target-filename',parameters);
	window.validTarget = true;
	setCopyState();
  }
}

function updateConfiguration(connectionType,connectionProperties,parameters) {

  const pwdRedacated = Object.assign({},connectionProperties);
  delete pwdRedacated.password
  
  const mode = connectionType.substring(0,connectionType.indexOf('-'));
  const db = connectionType.substring(connectionType.indexOf('-')+1);
  
  window.configuration.connections[mode] = { [db] : pwdRedacated }
  const schemaInfo = {}
  
  switch (mode) {
	case 'source':
	  switch (db) {
		case 'mssql' :
		  schemaInfo.database = connectionProperties.database
		  schemaInfo.schema = parameters.FROM_USER;
	      break; 
	    case 'snowflake':
		  schemaInfo.database = connectionProperties.database
		  schemaInfo.owner = parameters.FROM_USER;
	      break; 
        default:
		  schemaInfo.schema = parameters.FROM_USER;
      }
	  window.configuration.schemas.source = schemaInfo
	  break;
	case 'target':
	  switch (db) {
		case 'mssql' :
		  schemaInfo.database = connectionProperties.database
		  schemaInfo.owner = parameters.TO_USER;
	      break; 
	    case 'snowflake':
		  schemaInfo.database = connectionProperties.database
		  schemaInfo.schema = parameters.TO_USER;
	      break; 
        default:
		  schemaInfo.schema = parameters.TO_USER;
      }
	  window.configuration.schemas.target = schemaInfo
	  break;
    default:
  }
	  
}

function testConnection(button,status,connection,connectionProperties,parameters) {
 
  let valid = false;

  status.classList.remove('bi-question-circle')
  status.classList.remove('bi-check-circle')
  status.classList.remove('bi-times-circle')

  const state = ipcRenderer.sendSync(connection,connectionProperties,parameters);
  
  if (state === 'success') {
    valid = true
    status.classList.add('bi-check-circle')
	updateConfiguration(connection,connectionProperties,parameters)
  }
  else  {
	valid = false
    status.classList.add('bi-times-circle')
    dialog.showErrorBox('Connection Error', state)
  }
  button.disabled = false;	 
  return valid;  
}

function validateOracleSource(button) {

  button.disabled = true;
  const status = document.getElementById('source-oracle-status')
  const connectionProperties = {
    user              : document.getElementById('source-oracle-user').value
  , connectString     : document.getElementById('source-oracle-connectString').value
  , password          : document.getElementById('source-oracle-password').value
  };
  
  const parameters = {
	FROM_USER : document.getElementById('source-oracle-schema').value
  }
 

  if (document.getElementById('source-oracle-caseSensitive').checked === false) {
	connectionProperties.user = connectionProperties.user.toUpperCase();
    parameters.FROM_USER = parameters.FROM_USER.toUpperCase();
  }	
 
  window.validSource = testConnection(button,status,'source-oracle',{ oracle: connectionProperties},parameters)  
  setCopyState()
  
}

function validateOracleTarget(button) {

  button.disabled = true;
  const status = document.getElementById('target-oracle-status')
  const connectionProperties = {
    user              : document.getElementById('target-oracle-user').value
  , connectString     : document.getElementById('target-oracle-connectString').value
  , password          : document.getElementById('target-oracle-password').value
  };
  
  const parameters = {
	TO_USER : document.getElementById('target-oracle-schema').value
  }
 
  if (document.getElementById('target-oracle-caseSensitive').checked === false) {
	connectionProperties.user = connectionProperties.user.toUpperCase();
    parameters.TO_USER = parameters.TO_USER.toUpperCase();
  }	
 
  window.validTarget = testConnection(button,status,'target-oracle',{ oracle: connectionProperties},parameters)  
  setCopyState()
  
}

function validatePostgresSource(button) {

  button.disabled = true;
  const status = document.getElementById('source-postgres-status')
  const connectionProperties = {
    user      : document.getElementById('source-postgres-user').value
  , host      : document.getElementById('source-postgres-host').value
  , database  : document.getElementById('source-postgres-database').value
  , password  : document.getElementById('source-postgres-password').value
  , port      : document.getElementById('source-postgres-port').value
  };
  
  const parameters = {
	FROM_USER : document.getElementById('source-postgres-schema').value
  }
 
  window.validSource = testConnection(button,status,'source-postgres',{ postgres: connectionProperties},parameters)  
  setCopyState()

}

function validatePostgresTarget(button) {

  button.disabled = true;
  const status = document.getElementById('target-postgres-status')
  const connectionProperties = {
    user      : document.getElementById('target-postgres-user').value
  , host      : document.getElementById('target-postgres-host').value
  , database  : document.getElementById('target-postgres-database').value
  , password  : document.getElementById('target-postgres-password').value
  , port      : document.getElementById('target-postgres-port').value
  };
  
  const parameters = {
	TO_USER : document.getElementById('target-postgres-schema').value
  }
 
  window.validTarget = testConnection(button,status,'target-postgres',{ postgres: connectionProperties},parameters)  
  setCopyState()

}

function validateMsSQLSource(button) {

  button.disabled = true;
  const status = document.getElementById('source-mssql-status')
  const connectionProperties = {
    user                       : document.getElementById('source-mssql-user').value
  , server                     : document.getElementById('source-mssql-host').value
  , database                   : document.getElementById('source-mssql-database').value
  , password                   : document.getElementById('source-mssql-password').value
  , requestTimeout             : 360000000
  , options                    : {
      encrypt                  : false 
    , abortTransactionOnError  : false
    }
  };
  
  const parameters = {
	FROM_USER : document.getElementById('source-mssql-schema').value
  }
 
  window.validSource = testConnection(button,status,'source-mssql',{ mssql: connectionProperties},parameters)  
  setCopyState()

}

function validateMsSQLTarget(button) {

  button.disabled = true;
  const status = document.getElementById('target-mssql-status')
  const connectionProperties = {
    user                       : document.getElementById('target-mssql-user').value
  , server                     : document.getElementById('target-mssql-host').value
  , database                   : document.getElementById('target-mssql-database').value
  , password                   : document.getElementById('target-mssql-password').value
  , requestTimeout             : 360000000
  , options                    : {
      encrypt                  : false 
    , abortTransactionOnError  : false
    }
  };
  
  const parameters = {
	TO_USER : document.getElementById('target-mssql-schema').value
  }
 
  window.validTarget = testConnection(button,status,'target-mssql',{ mssql: connectionProperties},parameters)  
  setCopyState()

}

function validateMySQLSource(button) {

  button.disabled = true;
  const status = document.getElementById('source-mysql-status')
  const connectionProperties = {
    user                       : document.getElementById('source-mysql-user').value
  , host                       : document.getElementById('source-mysql-host').value
  , database                   : document.getElementById('source-mysql-database').value
  , password                   : document.getElementById('source-mysql-password').value
  , port                       : document.getElementById('source-mysql-port').value
  , multipleStatements         : true
  , typeCast                   : true
  , supportBigNumbers          : true
  , bigNumberStrings           : true          
  , dateStrings                : true
  };
  
  const parameters = {
	FROM_USER : document.getElementById('source-mysql-schema').value
  }
 
  window.validSource = testConnection(button,status,'source-mysql',{ mysql: connectionProperties},parameters)  
  setCopyState()

}

function validateMySQLTarget(button) {

  button.disabled = true;
  const status = document.getElementById('target-mysql-status')
  const connectionProperties = {
    user                       : document.getElementById('target-mysql-user').value
  , host                       : document.getElementById('target-mysql-host').value
  , database                   : document.getElementById('target-mysql-database').value
  , password                   : document.getElementById('target-mysql-password').value
  , port                       : document.getElementById('target-mysql-port').value
  , multipleStatements         : true
  , typeCast                   : true
  , supportBigNumbers          : true
  , bigNumberStrings           : true          
  , dateStrings                : true
  };
  
  const parameters = {
	TO_USER : document.getElementById('target-mysql-schema').value
  }
 
  window.validTarget = testConnection(button,status,'target-mysql',{ mysql: connectionProperties},parameters)  
  setCopyState()

}

function validateMariaDBSource(button) {

  button.disabled = true;
  const status = document.getElementById('source-mariadb-status')
  const connectionProperties = {
    user                       : document.getElementById('source-mariadb-user').value
  , host                       : document.getElementById('source-mariadb-host').value
  , database                   : document.getElementById('source-mariadb-database').value
  , password                   : document.getElementById('source-mariadb-password').value
  , port                       : document.getElementById('source-mariadb-port').value
  , multipleStatements         : true
  , typeCast                   : true
  , supportBigNumbers          : true
  , bigNumberStrings           : true          
  , dateStrings                : true
  };
  
  const parameters = {
	FROM_USER : document.getElementById('source-mariadb-schema').value
  }
 
  window.validSource = testConnection(button,status,'source-mariadb',{ mariadb: connectionProperties},parameters)  
  setCopyState()

}

function validateMariaDBTarget(button) {

  button.disabled = true;
  const status = document.getElementById('target-mariadb-status')
  const connectionProperties = {
    user                       : document.getElementById('target-mariadb-user').value
  , host                       : document.getElementById('target-mariadb-host').value
  , database                   : document.getElementById('target-mariadb-database').value
  , password                   : document.getElementById('target-mariadb-password').value
  , port                       : document.getElementById('target-mariadb-port').value
  , multipleStatements         : true
  , typeCast                   : true
  , supportBigNumbers          : true
  , bigNumberStrings           : true          
  , dateStrings                : true
  };
  
  const parameters = {
	TO_USER : document.getElementById('target-mariadb-schema').value
  }
 
  window.validTarget = testConnection(button,status,'target-mariadb',{ mariadb: connectionProperties},parameters)  
  setCopyState()

}

function validatesnowflakeSource(button) {

  button.disabled = true;
  const status = document.getElementById('source-snowflake-status')
  const connectionProperties = {
    account                    : document.getElementById('source-snowflake-account').value
  , username                   : document.getElementById('source-snowflake-user').value
  , password                   : document.getElementById('source-snowflake-password').value
  , warehouse                  : document.getElementById('source-snowflake-warehouse').value
  , database                   : document.getElementById('source-snowflake-database').value
  };
  
  const parameters = {
	YADAMU_DATABASE : connectionProperties.database 
  , FROM_USER : document.getElementById('source-snowflake-schema').value
  }
 
  window.validSource = testConnection(button,status,'source-snowflake',{ snowflake: connectionProperties},parameters)  
  setCopyState()

}

function validatesnowflakeTarget(button) {

  button.disabled = true;
  const status = document.getElementById('target-snowflake-status')
  const connectionProperties = {
    account                    : document.getElementById('target-snowflake-account').value
  , username                   : document.getElementById('target-snowflake-user').value
  , password                   : document.getElementById('target-snowflake-password').value
  , warehouse                  : document.getElementById('target-snowflake-warehouse').value
  , database                   : document.getElementById('target-snowflake-database').value
  };
  
  const parameters = {
	YADAMU_DATABASE : connectionProperties.database 
  , TO_USER : document.getElementById('target-snowflake-schema').value
  }
 
  window.validTarget = testConnection(button,status,'target-snowflake',{ snowflake: connectionProperties},parameters)  
  setCopyState()

}

function validateMongoDBSource(button) {

  button.disabled = true;
  const status = document.getElementById('source-mongodb-status')
  const connectionProperties = {
    host                       : document.getElementById('source-mongodb-host').value
  , port                       : document.getElementById('source-mongodb-port').value
  , database                   : document.getElementById('source-mongodb-database').value
  };
  
  const parameters = {
  }
  
  window.validSource = testConnection(button,status,'source-mongodb',{ mongodb: connectionProperties},parameters)  
  setCopyState()

}

function validateMongoDBTarget(button) {

  button.disabled = true;
  const status = document.getElementById('target-mongodb-status')
  const connectionProperties = {
    host                       : document.getElementById('target-mongodb-host').value
  , port                       : document.getElementById('target-mongodb-port').value
  , database                   : document.getElementById('target-mongodb-database').value
  };
  
  const parameters = {
  }

  window.validTarget = testConnection(button,status,'target-mongodb',{ mongodb: connectionProperties},parameters)  
  setCopyState()

}

function doCopy() {
  const result = ipcRenderer.sendSync('copy');
  if (result === 'success') {
	dialog.showMessageBox(undefined, {type : 'info', title : 'Copy Operation status', message : 'Operation completed successfully'})
  }
  else  {
    dialog.showErrorBox('Operation Failed', result)
  } 
}

function resetLog() {
  ipcRenderer.send('reset-log');
}

function writeLog() {
  ipcRenderer.send('write-log',new Date().toISOString());
}

function openLogWindow() {
  ipcRenderer.send('show-log');  
}

function setInitialValue(name,initialValue) {

  const cntrl = document.getElementById(name)
  if (cntrl !== null) {
	 cntrl.value = initialValue
  }
}

function loadConfiguration(configuration) {
	
	const job = configuration.jobs[0];
	const sourceConnectionID = job.source.connection
	const sourceConnection = configuration.connections[sourceConnectionID]
	const sourceSchemaID = job.source.schema;
	const sourceSchemaInfo = configuration.schemas[sourceSchemaID]
	const targetConnectionID = job.target.connection
	const targetConnection = configuration.connections[targetConnectionID]
	const targetSchemaID = job.target.schema;
	const targetSchemaInfo = configuration.schemas[targetSchemaID]
	
	const sourceDatabase = Object.keys(sourceConnection)[0]
	const targetDatabase = Object.keys(targetConnection)[0]

    const sourcePrefix = 'source-' + sourceDatabase + '-';
    const targetPrefix = 'target-' + targetDatabase + '-';
	
	Object.keys(sourceConnection[sourceDatabase]).forEach((key) => {setInitialValue(sourcePrefix + key,sourceConnection[sourceDatabase][key])})
	Object.keys(targetConnection[targetDatabase]).forEach((key) => {setInitialValue(targetPrefix + key,targetConnection[targetDatabase][key])})
	
	document.getElementById(sourcePrefix+'schema').value = sourceSchemaInfo.schema
	document.getElementById(targetPrefix+'schema').value = targetSchemaInfo.schema

	$('#source-tab a[href="#source-tab-' + sourceDatabase + '"]').tab('show')	
	$('#target-tab a[href="#target-tab-' + targetDatabase + '"]').tab('show')	
}

async function saveConfiguration(control) {
	
  const options = {
    title : "Save Log Windows contents as", 
 
	filters :[
		{name: 'JSON', extensions: ['json']},
		{name: 'Config', extensions: ['cfg']},
		{name: 'All Files', extensions: ['*']}
	],
	properties: ['openFile']
  }	
  const browseResults = await dialog.showSaveDialog(null,options)
  if (browseResults.cancelled !== true) {
    fs.writeFileSync(browseResults.filePath,JSON.stringify(window.configuration," ",2));
  }
}



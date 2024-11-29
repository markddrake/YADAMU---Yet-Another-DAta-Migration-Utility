"use strict"

// This file is required by the index.html file and will
// be executed in the renderer process for that window.
// All of the Node.js APIs are available in this process.

const electron = require('electron')
const { dialog } = require('@electron/remote')
const { ipcRenderer} = electron
const fs = require('fs')

window.validSource = false;
window.validTarget = false;
window.encryption = false;

window.configuration = { 
  connections          : {
    source             : {}
  ,	target             : {}
  }
, schemas              : {
    source             : {}
  , target             : {}	
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

function onLoad() {

   ipcRenderer.on('load-config', (event, configuration) => {
     loadConfiguration(configuration)
   })
   
   /*
   document.querySelector('#source-tab a').on('click', (e) => {
     disableCopy(e.target.href.substring(e.target.href.indexOf('#')+1)+'-status')
   })
   */
   
   document.querySelector('#source-oracle-caseSensitive').change((e) => {
     if (this.checked) {
   	   document.querySelector('#source-oracle-user').removeClass('text-uppercase');
   	   document.querySelector('#source-oracle-schema').removeClass('text-uppercase');
     }
     else {
       document.querySelector('#source-oracle-user').addClass('text-uppercase');
   	   document.querySelector('#source-oracle-schema').addClass('text-uppercase');
     }
   })
   
   document.querySelector('#target-oracle-caseSensitive').change((e) => {
     if (this.checked) {
       document.querySelector('#target-oracle-user').removeClass('text-uppercase');
   	   document.querySelector('#target-oracle-schema').removeClass('text-uppercase');
     }
     else {
   	   document.querySelector('#target-oracle-user').addClass('text-uppercase');
   	   document.querySelector('#target-oracle-schema').addClass('text-uppercase');
     }
   })
}

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

function selectFile(event,filePath) {
	
  const button = event.target.closest('button')
  if (filePath) {
    document.querySelector(`#${button.dataset.target}`).value = filePath[0]
    const parameters = {
	  FILE : filePath[0]
    }
	ipcRenderer.send('source-filename',parameters);
	window.validSource = true;
	setCopyState();
  }
}

function getSourceFileOptions() {
 return {
    title : "Select File to upload", 
 
	filters :[
		{name: 'Exports', extensions: ['json', 'exp', 'dmp', 'dump']},
		{name: 'All Files', extensions: ['*']}
	],
	properties: ['openFile']
  }	
 
}

function selectSourceFile(event) {
   const filePath = dialog.showOpenDialogSync(null,getSourceFileOptions()) 
   selectFile(event,filePath)
}

function getTargetFileOptions() {
  return {
    title : "Select File to save", 
 
	filters :[
		{name: 'Exports', extensions: ['json', 'exp', 'dmp', 'dump']},
		{name: 'All Files', extensions: ['*']}
	],
	properties: []
  }	
}

function selectTargetFile(event) {
   const filePath = dialog.showSaveDialogSync(null,getTargetFileOptions())
   selectFile(event,filePath)
}

function updateConfiguration(role,rdbms,connectionProperties,parameters) {

  const pwdRedacated = {
    ... connectionProperties
  }
  delete pwdRedacated.password
  
  window.configuration.connections[role] = { [rdbms] : pwdRedacated }
  const schemaInfo = {}
  
  switch (role) {
	case 'source':
	  switch (rdbms) {
		case 'mssql' :
		  schemaInfo.database = pwdRedacated.database
		  schemaInfo.schema = parameters.FROM_USER;
	      break; 
	    case 'snowflake':
		  schemaInfo.database = pwdRedacated.database
		  schemaInfo.owner = parameters.FROM_USER;
	      break; 
        default:
		  schemaInfo.schema = parameters.FROM_USER;
      }
	  window.configuration.schemas.source = schemaInfo
	  break;
	case 'target':
	  switch (rdbms) {
		case 'mssql' :
		  schemaInfo.database = pwdRedacated.database
		  schemaInfo.owner = parameters.TO_USER;
	      break; 
	    case 'snowflake':
		  schemaInfo.database = pwdRedacated.database
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

function testConnection(button,role,connectionProperties,parameters) {
 
  let valid = false;

  const status = document.querySelector(`#${role}-${button.dataset.rdbms}-state`)
  
  status.classList.remove('bi-question-circle')
  status.classList.remove('bi-check-circle')
  status.classList.remove('bi-times-circle')

  const state = ipcRenderer.sendSync(role,button.dataset.rdbms,connectionProperties,parameters);
  if (state === 'success') {
    valid = true
    status.classList.add('bi-check-circle')
	updateConfiguration(role,button.dataset.rdbms,connectionProperties,parameters)
  }
  else  {
	valid = false
    status.classList.add('bi-times-circle')
    dialog.showErrorBox('Connection Error', state)
  }
  
  button.disabled = false;	 
  return valid;  
}

function getFormData(event) {
  const form = event.target.form || event.target.closest('form')
  if (form) {
    const formValid = form.reportValidity()
    if (!formValid) return 
	const formData = Object.fromEntries(new FormData(form).entries())
	return formData
  }
}
	


function validateConnection(event,role,key) {

  const button = event.target.closest('button')
  button.disabled = true;
  
  const prefix = `${role}-${button.dataset.rdbms}`
  
  const connectionProperties = getFormData(event)
  
  const parameters = {
	[key] : connectionProperties.schema
  }
  
  delete connectionProperties.schema
 
  if (document.querySelector(`#${prefix}-caseSensitive`) && !document.querySelector(`#${prefix}-caseSensitive`) .checked) {
	connectionProperties.user = connectionProperties.user.toUpperCase();
    parameters[key] = parameters[key].toUpperCase();
  }	
 
  return testConnection(button,role,connectionProperties,parameters)  
  
}

function validateSourceConnection(event) {
  window.validSource = validateConnection(event,'source','FROM_USER')
  setCopyState()
}

function validateTargetConnection(event) {
  window.validTarget = validateConnection(event,'target','TO_USER')
  setCopyState()
}

function doCopy() {

  const parameters = {
	ENCRYPTION : window.encryption
  }
 
  const result = ipcRenderer.sendSync('copy',parameters);
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

	document.querySelector('#source-tab a[href="#source-tab-' + sourceDatabase + '"]').tab('show')	
	document.querySelector('#target-tab a[href="#target-tab-' + targetDatabase + '"]').tab('show')	
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



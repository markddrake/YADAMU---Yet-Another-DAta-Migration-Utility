"use strict"

// This file is required by the index.html file and will
// be executed in the renderer process for that window.
// All of the Node.js APIs are available in this process.


const electron = require('electron')
const { dialog } = require('@electron/remote')
const { ipcRenderer} = electron
const fs = require('fs');

window.$ = window.jQuery = require('jquery')

ipcRenderer.on('reset-log',(event) => {
  document.getElementById("log").innerHTML = "";
})

ipcRenderer.on('write-log',(event,msg) => {
  document.getElementById("log").innerHTML += msg;
})

function resetLog() {
  ipcRenderer.send('reset-log');
}

async function saveLogFile(control) {
	
  const options = {
    title : "Save Log Windows contents as", 
 
	filters :[
		{name: 'Log', extensions: ['log']},
		{name: 'Text', extensions: ['txt']},
		{name: 'All Files', extensions: ['*']}
	],
	properties: ['openFile']
  }	
  const browseResults = await dialog.showSaveDialog(null,options)
  if (browseResults.cancelled !== true) {
    fs.writeFileSync(browseResults.filePath,document.getElementById('log').innerText);
  }
}


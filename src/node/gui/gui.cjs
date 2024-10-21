
// Modules to control application life and create native browser window
const path = require('path')

const {app, BrowserWindow, ipcMain} = require('electron')
const remoteMain = require('@electron/remote/main')
remoteMain.initialize()

app.on('ready', main)

app.on('activate', async () => {
  // On macOS it's common to re-create a window in the app when the
  // dock icon is clicked and there are no other windows open.
  if (mainWindow === null) {
    await createWindow()
  }
})

// Quit when all windows are closed.

app.on('window-all-closed', () => {
  // On macOS it is common for applications and their menu bar
  // to stay active until the user quits explicitly with Cmd + Q
  if (process.platform !== 'darwin') {
    finalize(yadamu)
    app.quit()
  }
})    

async function finalize(yadamu) {
	
  await yadamu.close();
  app.quit();
  
}

async function main() {

  try {
	// Override default Electron processing of uncaughtException
    process.on('uncaughtException', function (e) { console.log(e);finalize()});
	let importedModule = await import('./yadamuGUI.js')
	YadamuGUI = importedModule.default
	importedModule = await import('./logWriter.js');
	LogWriter = importedModule.default
	importedModule = await import('../lib/yadamuLibrary.js')
	YadamuLibrary = importedModule.default
	importedModule = await import('../dbi/file/fileDBI.js')
	FileDBI = importedModule.default
	electronCmd = new YadamuGUI();
    try {
      const commamd = electronCmd.getCommand()
      switch (commamd) {
        case 'IMPORT':
	      await electronCmd.doImport();
		  await finalize()
          break;
        case 'UPLOAD':
          await electronCmd.doUpload();
          await finalize()
          break;
        case 'EXPORT':
          await electronCmd.doExport();
          await finalize()
          break;
        case 'COPY':
          await electronCmd.executeJobs();
          await finalize()
          break;
        case 'TEST':
          await electronCmd.doTests();
          await finalize()
          break;
  	    default:
		  yadamu = electronCmd.getYadamu()
		  yadamuLogger = yadamu.LOGGER
	      await createWindow(commamd,electronCmd.loadConfigurationFile())
      } 
	} catch (e) {
      console.log(e)
      await finalize()
	}
  } catch (e) {
    console.log(e)
    await finalize()
  } 

}

async function createWindow (operation,configuration) {

  // Create the browser window.

  mainWindow = new BrowserWindow({
    width: 800, 
    height: 600,    
    webPreferences: {
      nodeIntegration: true
	, contextIsolation: false
	//,  preload: path.join(__dirname,'YADAMU_UI/js/preload.js')
    } 
  })
  // and load the index.html of the app.

  remoteMain.enable(mainWindow.webContents)     
  
  mainWindow.webContents.on('did-finish-load',function(event) {
    if (operation === 'INIT') {
      mainWindow.webContents.send('load-config',configuration);
    }
  })

  mainWindow.loadFile('./html/gui/index.html')

  logWindow = new BrowserWindow({ 
    parent: mainWindow, 
	show:false, 
	webPreferences: { 
	  nodeIntegration: true
	, contextIsolation: false
    // ,preload: path.resolve(__dirname,'YADAMU_UI/js/preload.js')	 
	}
  })
  
  remoteMain.enable(logWindow.webContents)     

  logWindow.loadFile('./html/gui/logWindow.html')
  logWindow.on('close', function (event) {
    event.preventDefault();
	logWindow.hide();
  })
  
  logWindow.on('closed', function (event) {
	yadamuLogger.switchOutputStream(process.stdout);
  })

  logWriter = new LogWriter(logWindow);
  yadamuLogger.switchOutputStream(logWriter);

  // Open the DevTools.
  // mainWindow.webContents.openDevTools()

  // Emitted when the window is closed.
  mainWindow.on('closed', function () {
    // Dereference the window object, usually you would store windows
    // in an array if your app supports multi windows, this is the time
    // when you should delete the corresponding element.
    mainWindow = null
  })
}

async function validateConnection(dbiClassPath,connectionSettings,parameters) {
  const DBI = await import(dbiClassPath)
  const connectionInfo = {
    ...connectionSettings
  }
  const dbi = new DBI.default(yadamu,null,connectionInfo,parameters);
  await dbi.testConnection()
  return dbi
}

ipcMain.on('source-oracle', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateConnection('../dbi/oracle/oracleDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-oracle', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateConnection('../dbi/oracle/oracleDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('source-postgres', async (event, connectionProps, parameters) => {
  try{
    sourceDBI = await validateConnection('../dbi/postgres/postgresDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-postgres', async function (event, connectionProps, parameters) {
  try{
    targetDBI = await validateConnection('../dbi/postgres/postgresDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('source-vertica', async function (event, connectionProps, parameters) {
  try{
    sourceDBI = await validateConnection('../dbi/vertica/verticaDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-vertica', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateConnection('../dbi/vertica/verticaDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('source-yugabyte', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateConnection('../dbi/yugabyte/yugabyteDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-yugabyte', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateConnection('../dbi/yugabyte/yugabyteDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('source-cockroach', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateConnection('../dbi/cockroach/cockroachDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-cockroach', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateConnection('../dbi/cockroach/cockroachDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})


ipcMain.on('source-ibmdb2', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateConnection('../dbi/db2/db2DBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
    console.log(e)
	event.returnValue = e.message
  }
})

ipcMain.on('target-ibmdb2', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateConnection('../dbi/db2/db2DBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('source-mssql', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateConnection('../dbi/mssql/mssqlDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-mssql', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateConnection('../dbi/mssql/mssqlDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('source-mysql', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateConnection('../dbi/mysql/mysqlDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-mysql', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateConnection('../dbi/mysql/mysqlDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('source-mariadb', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateConnection('../dbi/mariadb/mariadbDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-mariadb', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateConnection('../dbi/mariadb/mariadbDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('source-snowflake', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateConnection('../dbi/snowflake/snowflakeDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-snowflake', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateConnection('../dbi/snowflake/snowflakeDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('source-mongodb', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateConnection('../dbi/mongodb/mongoDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  
  }
})

ipcMain.on('target-mongodb', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateConnection('../dbi/mongodb/mongoDBI.js',connectionProps,parameters)
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

function setFileReader(parameters) {
  const fileReader = new FileDBI(yadamu);
  fileReader.setParameters(parameters);
  return fileReader;
}

ipcMain.on('source-filename',function (event, parameters) {
   sourceDBI = setFileReader(parameters);
})

function setFileWriter(parameters) {
  const fileWriter = new FileDBI(yadamu);
  fileWriter.setParameters(parameters);
  return fileWriter;
}

ipcMain.on('target-filename',function (event, parameters) {
   targetDBI = setFileWriter(parameters);
})


ipcMain.on('copy', async function (event, parameters) {
	
	try {
	  const startTime = new Date().getTime();
	  yadamu.reset(parameters)
	  // TODO Allow user to specify encryption algorithim and key
	  yadamu.parameters.ENCRYPTION = false;
	  
      await yadamu.doPumpOperation(sourceDBI,targetDBI)
	  const elapsedTime = new Date().getTime() - startTime;
	  const status = yadamu.STATUS
	  switch (status.operationSuccessful) {
        case true:
	      yadamuLogger.info(['YADAMU-GUI'],`Operation completed "${status.statusMsg}"`);
          event.returnValue = 'success'
	      break;
	    case false:
	      yadamuLogger.error(['YADAMU-GUI'],`Operation failed. "${status.err.message}"`);
   	      event.returnValue = status.err.message
		  break;
	    default:
	  }
      yadamuLogger.info(['YADAMU-GUI'],`Elapsed time: ${YadamuLibrary.stringifyDuration(elapsedTime)}.`);
	  
	} catch (e) {console.log(e)}
})

ipcMain.on('show-log', function (event) {
  logWindow.show();
})

ipcMain.on('reset-log', function (event) {
  logWindow.webContents.send('reset-log');
})

ipcMain.on('write-log', async function (event, msg) {
  logWindow.webContents.send('write-log',msg);
})


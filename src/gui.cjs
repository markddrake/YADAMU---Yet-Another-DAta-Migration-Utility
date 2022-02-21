
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
	
  await electronCmd.close();
  app.quit();
  
}

async function main() {

  try {
	// Override default Electron processing of uncaughtException
    process.on('uncaughtException', function (e) { console.log(e);finalize()});
	let importedModule = await import('./node/gui/yadamuGUI.js')
	YadamuGUI = importedModule.default
	importedModule = await import('./node/gui/logWriter.js');
	LogWriter = importedModule.default
	importedModule = await import('./node/lib/yadamuLibrary.js')
	YadamuLibrary = importedModule.default
	importedModule = await import('./node/dbi/file/fileDBI.js')
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

async function validateOracle(connectionProps,parameters) {

  const OracleDBI = await import('./dbi/oracle/oracleDBI.js')
  const oracleDBI = new OracleDBI.default(yadamu);
  await oracleDBI.testConnection(connectionProps,parameters)
  return oracleDBI
}

 
ipcMain.on('source-oracle', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateOracle(connectionProps,parameters);
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-oracle', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateOracle(connectionProps,parameters);
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

async function validatePostgres(connectionProps,parameters) {

  const PostgresDBI = await import('./dbi/postgres/postgresDBI.js')
  const postgresDBI = new PostgresDBI.default(yadamu);
  await postgresDBI.testConnection(connectionProps,parameters)
  return postgresDBI
}

ipcMain.on('source-postgres', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validatePostgres(connectionProps,parameters);
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-postgres', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validatePostgres(connectionProps,parameters);
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

async function validateMsSQL(connectionProps,parameters) {

  const MsSQLDBI = await import('./dbi/mssql/mssqlDBI.js')
  const mssqlDBI = new MsSQLDBI.default(yadamu);
  await mssqlDBI.testConnection(connectionProps,parameters)
  return mssqlDBI
}

ipcMain.on('source-mssql', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateMsSQL(connectionProps,parameters);
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-mssql', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateMsSQL(connectionProps,parameters);
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

async function validateMySQL(connectionProps,parameters) {

  const MySQLDBI = await import('./dbi/mysql/mysqlDBI.js')
  const mysqlDBI = new MySQLDBI.default(yadamu);
  await mysqlDBI.testConnection(connectionProps,parameters)
  return mysqlDBI
}

ipcMain.on('source-mysql', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateMySQL(connectionProps,parameters);
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-mysql', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateMySQL(connectionProps,parameters);
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

async function validateMariaDB(connectionProps,parameters) {

  const MariaDBI = await import('./dbi/mariadb/mariadbDBI.js')
  const mariaDBI = new MariaDBI.default(yadamu);
  await mariaDBI.testConnection(connectionProps,parameters)
  return mariaDBI
}

ipcMain.on('source-mariadb', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateMariaDB(connectionProps,parameters);
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-mariadb', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateMariaDB(connectionProps,parameters);
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

async function validatesnowflake(connectionProps,parameters) {

  const SnowflakeDBI = await import('./dbi/snowflake/snowflakeDBI.js')
  const snowflakeDBI = new SnowflakeDBI.default(yadamu);
  await snowflakeDBI.testConnection(connectionProps,parameters)
  return snowflakeDBI
}

ipcMain.on('source-snowflake', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validatesnowflake(connectionProps,parameters);
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-snowflake', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validatesnowflake(connectionProps,parameters);
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

async function validateMongoDB(connectionProps,parameters) {

  const MongoDBI = await import('./dbi/mongodb/mongoDBI.js')
  const mongoDBI = new MongoDBI.default(yadamu);
  await mongoDBI.testConnection(connectionProps,parameters)
  return mongoDBI
}

ipcMain.on('source-mongodb', async function (event, connectionProps, parameters) {
  try{
	sourceDBI = await validateMongoDB(connectionProps,parameters);
    event.returnValue = 'success'
  } catch (e) {
	event.returnValue = e.message
  }
})

ipcMain.on('target-mongodb', async function (event, connectionProps, parameters) {
  try{
	targetDBI = await validateMongoDB(connectionProps,parameters);
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


ipcMain.on('copy', async function (event) {
	
	try {
	  const startTime = new Date().getTime();
      await yadamu.doPumpOperation(sourceDBI,targetDBI)
	  const elapsedTime = new Date().getTime() - startTime;
	  const status = yadamu.STATUS
	  switch (status.operationSuccessful) {
        case true:
	      yadamuLogger.info([`YadamuUI.onCopy()`,`${status.operation}`],`Operation completed ${status.statusMsg}. Elapsed time: ${YadamuLibrary.stringifyDuration(elapsedTime)}.`);
          event.returnValue = 'success'
	      break;
	    case false:
	      yadamuLogger.error([`${this.constructor.name}`,`.on('copy')`],`Copy operation failed. Elapsed Time: ${YadamuLibrary.stringifyDuration(elapsedTime)}s.`);
   	      event.returnValue = status.err.message
		  break;
	    default:
	  }
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


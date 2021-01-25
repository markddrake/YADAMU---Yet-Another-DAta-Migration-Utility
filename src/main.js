// Modules to control application life and create native browser window
const {app, BrowserWindow, ipcMain} = require('electron')
const path = require('path');

const LogWriter = require('./YADAMU_UI/node/logWriter.js');

const YadamuLibrary = require('./YADAMU/common/yadamuLibrary.js');
const Yadamu = require('./YADAMU/common/yadamu.js')
const YadamuGUI = require('./YADAMU/common/yadamuGUI.js')
const FileDBI = require('./YADAMU/file/node/fileDBI.js');

let sourceDBI = undefined
let targetDBI = undefined;


// Keep a global reference of the window object, if you don't, the window will
// be closed automatically when the JavaScript object is garbage collected.

async function finalize(yadamu) {
	
  await electronCmd.close();
  app.quit();
  
}

async function main() {

  try {
	// Override default Electron processing of uncaughtException
    process.on('uncaughtException', function (e) { console.log(e);finalize()});
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
	      createWindow(commamd,electronCmd.loadConfigurationFile())
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

function createWindow (operation,configuration) {
  // Create the browser window.

  mainWindow = new BrowserWindow({
    width: 800, 
    height: 600,    
    webPreferences: {
      nodeIntegration: true
  }})
  // and load the index.html of the app.

  mainWindow.webContents.on('did-finish-load',function(event) {
    if (operation === 'INIT') {
      mainWindow.webContents.send('load-config',configuration);
    }
  })

  mainWindow.loadFile('./YADAMU_UI/html/index.html')

  logWindow = new BrowserWindow({ parent: mainWindow, show:false, webPreferences: { nodeIntegration: true }})
  logWindow.loadFile('./YADAMU_UI/html/logWindow.html')
  logWindow.on('close', function (event) {
    event.preventDefault();
	logWindow.hide();
  })
  
  logWindow.on('closed', function (event) {
	yadamuLogger.switchOutputStream(process.stdout);
  })

  const logWriter = new LogWriter(logWindow);
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

// This method will be called when Electron has finished
// initialization and is ready to create browser windows.
// Some APIs can only be used after this event occurs.
app.on('ready', main)

// Quit when all windows are closed.
app.on('window-all-closed', function () {
  // On macOS it is common for applications and their menu bar
  // to stay active until the user quits explicitly with Cmd + Q
  if (process.platform !== 'darwin') {
	finalize(yadamu)
    app.quit()
  }
})

app.on('activate', function () {
  // On macOS it's common to re-create a window in the app when the
  // dock icon is clicked and there are no other windows open.
  if (mainWindow === null) {
    createWindow()
  }
})

// In this file you can include the rest of your app's specific main process
// code. You can also put them in separate files and require them here.

async function validateOracle(connectionProps,parameters) {

  const OracleDBI = require('./YADAMU/oracle/node/oracleDBI')
  const oracleDBI = new OracleDBI(yadamu);
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

  const PostgresDBI = require('./YADAMU/postgres/node/postgresDBI')
  const postgresDBI = new PostgresDBI(yadamu);
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

  const MsSQLDBI = require('./YADAMU/mssql/node/mssqlDBI')
  const mssqlDBI = new MsSQLDBI(yadamu);
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

  const MySQLDBI = require('./YADAMU/mysql/node/mysqlDBI')
  const mysqlDBI = new MySQLDBI(yadamu);
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

  const MariaDBI = require('./YADAMU/mariadb/node/mariadbDBI')
  const mariaDBI = new MariaDBI(yadamu);
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

  const SnowflakeDBI = require('./YADAMU/snowflake/node/snowflakeDBI')
  const snowflakeDBI = new SnowflakeDBI(yadamu);
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

  const MongoDBI = require('./YADAMU/mongodb/node/mongoDBI')
  const mongoDBI = new MongoDBI(yadamu);
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


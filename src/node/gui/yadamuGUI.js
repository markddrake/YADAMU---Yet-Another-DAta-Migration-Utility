0
import path from 'path'

import electron from 'electron';
const { app, BrowserWindow,ipcMain } = electron;

import electronRemote from '@electron/remote/main/index.js'
const {initialize, enable} = electronRemote

//Initialize remoteMain...
initialize()

import YadamuCLI            from '../cli/yadamuCLI.js'
import YadamuLibrary        from '../lib/yadamuLibrary.js'
import FileDBI              from '../dbi/file/fileDBI.js'

import LogWriter            from './logWriter.js'
	
class YadamuGUI extends YadamuCLI {
	
  #SUPPORTED_COMMANDS = Object.freeze(['IMPORT','EXPORT','COPY','LOAD','UNLOAD']);
  get SUPPORTED_COMMANDS() { return this.#SUPPORTED_COMMANDS }
  
  getCommand() {

	let command
	
	for (let argv of process.argv) {
	  argv = typeof argv === 'string' ? (argv.startsWith('--') ? argv.substring(2) : argv).toUpperCase() : argv
	  if (this.SUPPORTED_COMMANDS.includes(argv)) {
		if (command) {
		  throw new Error(`Specifiy only one of ${this.SUPPORTED_COMMANDS}.`)
		}
		command = argv
	  }
	}
	command = command || this.constructor.name.toUpperCase()
    this.validateParameters(command)
	return command 
  }
  
  constructor() {
    super()
 	this.yadamu = this.getYadamu() 
    this.yadamuLogger = this.yadamu.LOGGER
	this.initializeAPP()
	this.initializeIPC()
  }
  
  async runCommand() {
	try {
      this.command = this.getCommand()
      switch (this.command) {
        case 'EXPORT':
          await this.doExport();
          await this.finalize()
          break;
        case 'IMPORT':
  	      await this.doImport();
          await this.finalize()
          break;
        case 'UPLOAD':
          await this.doUpload();
          await this.finalize()
          break;
        case 'COPY':
          await this.doCopy();
          await this.finalize()
          break;
        case 'LOAD':
          await this.doLoad();
          await this.finalize()
          break;
        case 'UNLOAD':
          await this.doUnload();
          await this.finalize()
          break;
        default:
          await this.createWindow(this.command,this.loadConfigurationFile())
      }
    } catch (e) {
	  YadamuLibrary.reportError(e)
    }
  }
  
  initializeAPP() {

    app.whenReady().then(() => {
	  this.runCommand()
	});

    app.on('activate', async () => {
      // On macOS it's common to re-create a window in the app when the
      // dock icon is clicked and there are no other windows open.
      if (this.mainWindow === null) {
        await this.createWindow()
      }
    })

    // Quit when all windows are closed.

    app.on('window-all-closed', () => {
      // On macOS it is common for applications and their menu bar
      // to stay active until the user quits explicitly with Cmd + Q
      if (process.platform !== 'darwin') {
        this.finalize()
        app.quit()
      }
    })    
  }

  initializeIPC() {
	  
    ipcMain.on('source', async (event, rdbms, connectionProps, parameters) => {
      try{		  
    	this.sourceDBI = await this.validateConnection(rdbms,connectionProps,parameters)
        event.returnValue = 'success'
      } catch (e) {
    	event.returnValue = e.message
      }
    })
    
    ipcMain.on('target', async (event, rdbms, connectionProps, parameters) => {
      try{
    	this.targetDBI = await this.validateConnection(rdbms,connectionProps,parameters)
        event.returnValue = 'success'
      } catch (e) {
    	event.returnValue = e.message
      }
    })
    
    ipcMain.on('copy', async (event, parameters) => {
    	
    	try {
    	  const startTime = new Date().getTime();
    	  this.yadamu.reset(parameters)
    	  // TODO Allow user to specify encryption algorithim and key
    	  this.yadamu.parameters.ENCRYPTION = false;
    	  
          const metrics = await this.yadamu.doPumpOperation(this.sourceDBI,this.targetDBI)
    	  const elapsedTime = new Date().getTime() - startTime;
    	  const status = this.yadamu.STATUS
    	  switch (status.operationSuccessful) {
            case true:
    	      this.yadamuLogger.info(['YADAMU-GUI'],`Operation completed "${status.statusMsg}"`);
              event.returnValue = 'success'
    	      break;
    	    case false:
    	      this.yadamuLogger.error(['YADAMU-GUI'],`Operation failed. "${status.err.message}"`);
       	      event.returnValue = status.err.message
    		  break;
    	    default:
    	  }
          this.yadamuLogger.info(['YADAMU-GUI'],`Elapsed time: ${YadamuLibrary.stringifyDuration(elapsedTime)}.`);
    	  
    	} catch (e) {console.log(e)}
    })
    
    ipcMain.on('source-filename',(event, parameters) => {
       this.sourceDBI = this.setFileReader(parameters);
    })

    ipcMain.on('target-filename',(event, parameters) => {
       this.targetDBI = this.setFileWriter(parameters);
    })
    
    ipcMain.on('show-log', (event) => {
      this.logWindow.show();
    })
    
    ipcMain.on('reset-log', (event) => {
      this.logWindow.webContents.send('reset-log');
    })
    
    ipcMain.on('write-log', async (event, msg) => {
      this.logWindow.webContents.send('write-log',msg);
    })
    
  }

  setFileReader(parameters) {
    const fileReader = new FileDBI(this.yadamu);
    fileReader.setParameters(parameters);
    return fileReader;
  }
        
  setFileWriter(parameters) {
    const fileWriter = new FileDBI(this.yadamu);
    fileWriter.setParameters(parameters);
    return fileWriter;
  }
    
    
  async finalize() {
    await this.yadamu.close();
    app.quit();
  }
    
  async createWindow(operation,configuration) {
    
    // Create the browser window.
    
    this.mainWindow = new BrowserWindow({
      width: 800, 
      height: 600,    
      webPreferences: {
        nodeIntegration: true
      , contextIsolation: false
      //,  preload: path.join(__dirname,'YADAMU_UI/js/preload.js')
      } 
    })
  
    // Enable remote access to mainWindow
    enable(this.mainWindow.webContents);
    // and load the index.html of the app.

    this.mainWindow.loadFile('./html/gui/index.html')
	// console.log('Loaded')
	
    this.mainWindow.webContents.once('did-finish-load', () => {
      if (operation === 'INIT') {
        ipcRenderer.send('load-config',configuration)
      }
    })
      
    
    this.logWindow = new BrowserWindow({ 
      parent: this.mainWindow, 
      show:false, 
   	  webPreferences: { 
    	nodeIntegration: true
      , contextIsolation: false
      // ,preload: path.resolve(__dirname,'YADAMU_UI/js/preload.js')	 
      }
    })
       
    // Enable remote access to logWindow
    enable(this.logWindow.webContents);

    this.logWindow.loadFile('./html/gui/logWindow.html')
    this.logWindow.on('close', (event) => {
      event.preventDefault();
      this.logWindow.hide();
    })
      
    this.logWindow.on('closed', (event) => {
   	  this.yadamuLogger.switchOutputStream(process.stdout);
    })
    
    const logWriter = new LogWriter(this.logWindow);
    this.yadamuLogger.switchOutputStream(logWriter);
    
    
    // Emitted when the window is closed.
    this.mainWindow.on('closed', () => {
      // Dereference the window object, usually you would store windows
      // in an array if your app supports multi windows, this is the time
      // when you should delete the corresponding element.
      this.mainWindow = null
    })
  }
    
  async validateConnection(driver,connectionSettings,parameters) {
	const dbi = await this.getDatabaseInterface(this.yadamu,driver,{[driver] : connectionSettings},parameters);
    await dbi.testConnection()
    return dbi
  }
	

}

export { YadamuGUI as default}

import express      from 'express'
import cookieParser from 'cookie-parser'
import YadamuService from './service.js'
import YadamuLogger from '../core/yadamuLogger.js'

const yadamuService = new YadamuService();
const yadamuLogger = yadamuService.getYadamu().LOGGER

function setSessionId(cookies,sessionState) {
  
  // movieTicketing.setSessionState(cookies,sessionState);

}

function initializeApplication(sessionState) {
  
  yadamuService.initialize(sessionState)
  sessionState.save();

}

function getRouter() {

  const router = express.Router();
  // const yadamuService = new YadamuService();
  
  /*
  router.use((req, res, next) => {
    setSessionId(req.cookies,req.session);
    initializeApplication(req.session);
    next();
  });
  */

  router.get('/about',about)
  
  // Stream exported content to the client

  router.route('/download/source/:sourceConnection/schema/:sourceSchema').get(exportStream);

  // Write exported content to a File located on the server in the directory. The file system location of the directory is obtained from the configuration file using the directory name specified in the URL.

  router.route('/export/source/:sourceConnection/schema/:sourceSchema/directory/:directory/file/:file').get(exportFile);
  
  // Import streamed content from the client

  router.route('/upload/target/:targetConnection/schema/:targetSchema').put(importStream)
  
  // Read content to import from a file located on the server. The file system location of the directory is obtained from the configuration file using the directory name specified in the URL.

  router.route('/import/directory/:directory/file/:file/target/:targetConnection/schema/:targetSchema').get(importFile);

  // Database to Database copy

  router.route('/copy/source/:sourceConnection/schema/:sourceSchema/target/:targetConnection/schema/:targetSchema').get(copy);
  
  // Upload Configuration Files
  
  router.route('/configuration').put(updateConfiguration)
 
  // Run Configured Jobs
  
  router.route('/jobs').get(executeJobs)
  router.route('/jobs/jobNumber/:jobNumber').get(executeJobs)
  router.route('/jobs/jobName/:jobName').get(executeJobs)
 
  return router;
}

async function exportStream(request,response,next) {
  try {
    yadamuLogger.info(['YADAMU-SVR','EXPORT-STREAM'],request.originalUrl)
    await yadamuService.exportStream(request,response)
  } catch (e) {
	next(e)
  }
}

async function exportFile(request,response,next) {
  try {
    yadamuLogger.info(['YADAMU-SVR','EXPORT-FILE'],request.originalUrl)
    await yadamuService.exportFile(request,response)
  } catch (e) {
	next(e)
  }
}

async function importStream(request,response,next) {
  try {
    yadamuLogger.info(['YADAMU-SVR','IMPORT-STREAM'],request.originalUrl)
    await yadamuService.importStream(request,response)
  } catch (e) {
	next(e)
  }
}

async function importFile(request,response,next) {
  try {
    yadamuLogger.info(['YADAMU-SVR','IMPORT-FILE'],request.originalUrl)
    await yadamuService.importFile(request,response)
  } catch (e) {
	next(e)
  }
}

async function copy(request,response,next) {
  try {
    yadamuLogger.info(['YADAMU-SVR','COPY'],request.originalUrl)
    await yadamuService.copy(request,response)
  } catch (e) {
	next(e)
  }
}

async function updateConfiguration(request,response,next) {
  try {
    yadamuLogger.info(['YADAMU-SVR','UPLOAD-CONFIGURATION'],request.originalUrl)
    await yadamuService.updateConfiguration(request,response);
  } catch (e) {
	next(e)
  }
}

async function executeJobs(request,response,next) {
  try {
    yadamuLogger.info(['YADAMU-SVR','EXECUTE-JOBS'],request.originalUrl)
    await yadamuService.executeJobs(request,response)
  } catch (e) {
	next(e)
  }
}

async function about(request,response,next) {
  try {
    yadamuLogger.info(['YADAMU-SVR','ABOUT'],request.originalUrl)
    await yadamuService.about(request,response)
  } catch (e) {
	next(e)
  }
}

function log(message) {
  yadamuLogger.info(['YADAMU-SVR'],message)
}

function handleException(err, req, res, next) {
  /*
  if (res.writableEnded) {
  if (!res.finished) {
    yadamuLogger.switchOutputStream(res);
    yadamuLogger.handleException(['YADAMU-SVR','EXCEPTIION'],err)
  }
  */
  yadamuLogger.switchOutputStream(process.stdout);
  yadamuLogger.handleException(['YADAMU-SVR','EXCEPTIION'],err)
  // res.status(500).send('An error has occurred, please contact support if the error persists');
  res.end();
}

export  {
  getRouter
, log
, handleException
}
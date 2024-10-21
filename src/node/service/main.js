
import path         from 'path'
import http         from 'http'
import express      from 'express'
import cookieParser from 'cookie-parser'
import session      from 'express-session'
import bodyParser   from 'body-parser'
import serveStatic  from 'serve-static'

import Routes       from './routes.js'

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

  router.get('/about',Routes.about)
  
  // Stream exported content to the client

  router.route('/download/source/:sourceConnection/schema/:sourceSchema').get(Routes.exportStream);

  // Write exported content to a File located on the server in the directory. The file system location of the directory is obtained from the configuration file using the directory name specified in the URL.

  router.route('/export/source/:sourceConnection/schema/:sourceSchema/directory/:directory/file/:file').get(Routes.exportFile);
  
  // Import streamed content from the client

  router.route('/upload/target/:targetConnection/schema/:targetSchema').put(Routes.importStream)
  
  // Read content to import from a file located on the server. The file system location of the directory is obtained from the configuration file using the directory name specified in the URL.

  router.route('/import/directory/:directory/file/:file/target/:targetConnection/schema/:targetSchema').get(Routes.importFile);

  // Database to Database copy

  router.route('/copy/source/:sourceConnection/schema/:sourceSchema/target/:targetConnection/schema/:targetSchema').get(Routes.copy);
  
  // Upload Configuration Files
  
  router.route('/configuration').put(Routes.updateConfiguration)
 
  // Run Configured Jobs
  
  router.route('/jobs').get(Routes.executeJobs)
  router.route('/jobs/jobNumber/:jobNumber').get(Routes.executeJobs)
  router.route('/jobs/jobName/:jobName').get(Routes.executeJobs)

  router.route('/batchOperation/batchName/:batchName').get(Routes.executeBatch)
 
  return router;
}

function initApp() {

  const app = express()
  const port = process.env.PORT || 3000;    
  
  // Do not use bodyParser due to restrictions on JSON payload size...
  // app.use(bodyParser.json()); // for parsing application/json
  
  app.use('/yadamu', getRouter());

  app.use((req, res, next) => {
    res.header("Cache-Control", "no-cache, no-store, must-revalidate");
    res.header("Pragma", "no-cache");
    res.header("Expires", 0);
    next();
  });
  
  app.use(Routes.handleException);
	
  
  app.listen(port,() => {
    Routes.log('Yadamu Server Listening on localhost:' + port);
  });
}

process.on('unhandledRejection', (reason, p) => {
  console.log("Unhandled Rejection:\nPromise:\n ", p, "\nReason:");
  console.log(reason);  
});

initApp();


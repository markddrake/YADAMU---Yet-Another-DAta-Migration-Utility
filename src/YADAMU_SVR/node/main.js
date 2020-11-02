"use strict";

const path = require('path');
const http = require('http');
const express = require('express');
const cookieParser = require('cookie-parser');
const session = require('express-session');
const bodyParser = require('body-parser');
const serveStatic = require('serve-static');

const router = require('./yadamuRouter.js');

const app  = express();

function writeLogEntry(module,comment) {
	
  const message = ( comment === undefined) ? module : module + ": " + comment
  console.log(new Date().toISOString() + ": index." + message);
}

function initApp() {

  const moduleId = 'initApp()';

  var port = process.env.PORT || 3000;
  
  var httpServer = http.Server(app);
  
  // Do not use bodyParser due to restrictions on JSON payload size...

  // app.use(bodyParser.json()); // for parsing application/json
  
  app.use(cookieParser());
  app.use(session({ secret: 'boo', resave: false, saveUninitialized: false}));

  app.use('/yadamu', router.getRouter());

  // app.use('/frameworks', serveStatic(__dirname + '/node_modules'));  
  // app.use('/frameworks', serveStatic(__dirname.substring(0, __dirname.lastIndexOf(path.sep))));

  // app.use('/', serveStatic(__dirname + '/public'));

  app.use((req, res, next) => {
    res.header("Cache-Control", "no-cache, no-store, must-revalidate");
    res.header("Pragma", "no-cache");
    res.header("Expires", 0);
    next();
  });


  app.use(handleError);
	
  
  httpServer.listen(port,() => {
    writeLogEntry(moduleId,'Listening on localhost:' + port);
  });
}

function handleError(err, req, res, next) {
  console.log('handleError\n',err);
  res.status(500).send({message: 'An error has occurred, please contact support if the error persists'});
  res.end();
}

process.on('unhandledRejection', (reason, p) => {
  console.log("Unhandled Rejection:\nPromise:\n ", p, "\nReason:");
  console.log(reason);  
});

initApp();


"use strict";

const express = require('express');
const cookieParser = require('cookie-parser');
const YadamuServer = require('./yadamuServer.js');
const YadamuLogger = require('../../YADAMU/common/yadamuLogger.js');

const yadamuServer = new YadamuServer();
const yadamuLogger = new YadamuLogger(process.stdout,{});

function setSessionId(cookies,sessionState) {
  
  // movieTicketing.setSessionState(cookies,sessionState);

}

function initializeApplication(sessionState) {
  
  yadamuServer.initialize(sessionState)
  sessionState.save();

}

function getRouter() {

  const router = express.Router();
  // const yadamuServer = new YadamuServer();
  
  /*
  router.use(function initalizeSession(req, res, next) {
    setSessionId(req.cookies,req.session);
    initializeApplication(req.session);
    next();
  });
  */

  // Client Receives Content

  router.get('/export/connection/:connection/schema/:schema',exportStream)

  // Client Provides Content

  router.put('/import/connection/:connection/schema/:schema',importStream)
  
  
  // File located on Server

  router.route('/export/connection/:connection/schema/:schema/directory/:directory/file/:file').get(exportFile);

  router.route('/import/directory/:directory/file/:file/connection/:connection/schema/:schema').get(importFile);


  // Database to Database copy

  router.route('/copy/from/:fromConnection/source:fromSchema/to/:toConnection/target/:toConnection').get(copy);
  
  // Configuration Files
  
  router.route('/connection').put(uploadConnectionDefinitions)
  
  router.route('/schema').put(uploadSchemaMappings)
  
  return router;
}

function getClassName() {
  return 'yadamuRouter.js';
}

async function exportStream(request,response,next) {
  console.log('exportStream');
  try {
    await yadamuServer.exportStream(request,response)
  } catch (e) {
	yadamuLogger.logException([`${getClassName()}.exportStream()`],'Exception',e)
	next(e);
  }
}

async function importStream(request,response,next) {
  console.log('importStream');
  try {
    await yadamuServer.importStream(request,response)
  } catch (e) {
	yadamuLogger.logException([`${getClassName()}.importStream()`],'Exception',e)
	next(e);
  }
}

async function exportFile(request,response,next) {
  console.log('exportFile');
  try {
    yadamuServer.exportFile(request,response)
  } catch (e) {
	yadamuLogger.logException([`${getClassName()}.exportFile()`],'Exception',e)
	next(e);
  }
}

async function importFile(request,response,next) {
  console.log('importFile');
  try {
    await yadamuServer.importFile(request,response)
  } catch (e) {
	yadamuLogger.logException([`${getClassName()}.importFile()`],'Exception',e)
	next(e);
  }
}

async function copy(request,response,next) {
  console.log('copy');
  try {
    await yadamuServer.copy(request,response)
  } catch (e) {
	yadamuLogger.logException([`${getClassName()}.copy()`],'Exception',e)
	next(e);
  }
}

async function uploadConnectionDefinitions(request,response,next) {
  console.log('uploadConnectionDefinitions');
  try {
    await yadamuServer.uploadConnectionDefinitions(request,response);
  } catch (e) {
	yadamuLogger.logException([`${getClassName()}.uploadConnectionDefinitions()`],'Exception',e)
	next(e);
  }
}

async function uploadSchemaMappings(request,response,next) {
  console.log('uploadSchemaMappings');
  try {
    await yadamuServer.uploadSchemaMappings(request,response)
  } catch (e) {
	yadamuLogger.logException([`${getClassName()}.uploadSchemaMappings()`],'Exception',e)
	next(e);
  }
}


async function about(request,response,next) {
  console.log('uploadSchemaMappings');
  try {
    await yadamuServer.about(request,response)
  } catch (e) {
	yadamuLogger.logException([`${getClassName()}.about()`],'Exception',e)
	next(e);
  }
}

module.exports.getRouter = getRouter
module.exports.exportStream = exportStream
module.exports.importStream = importStream
module.exports.exportFile = exportFile
module.exports.importFile = importFile
module.exports.copy = copy
module.exports.uploadSchemaMappings = uploadSchemaMappings
module.exports.uploadConnectionDefinitions = uploadConnectionDefinitions

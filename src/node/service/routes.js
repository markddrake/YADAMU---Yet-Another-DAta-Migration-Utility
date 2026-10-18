
import {
  performance
}                        from 'perf_hooks';

import Service           from './service.js'
import YadamuOutcome     from '../cli/yadamuOutcome.js'

class Routes {

  static #SERVICE = new Service()
  static get SERVICE()  { return Routes.#SERVICE }
		
  static get LOGGER()  { return Routes.SERVICE.yadamu.LOGGER }

  static async exportStream(request,response,next) {
    try {
      Routes.LOGGER.info(['YADAMU-SVR','EXPORT-STREAM'],request.originalUrl)
      await Routes.SERVICE.exportStream(request,response)
    } catch (e) {
	  next(e)
    }
  }

  static async exportFile(request,response,next) {
    try {
      Routes.LOGGER.info(['YADAMU-SVR','EXPORT-FILE'],request.originalUrl)
      await Routes.SERVICE.exportFile(request,response)
    } catch (e) {
  	  next(e)
    }
  }

  static async importStream(request,response,next) {
    try {
      Routes.LOGGER.info(['YADAMU-SVR','IMPORT-STREAM'],request.originalUrl)
      await Routes.SERVICE.importStream(request,response)
    } catch (e) {
	  next(e)
    }
  }

  static async importFile(request,response,next) {
    try {
      Routes.LOGGER.info(['YADAMU-SVR','IMPORT-FILE'],request.originalUrl)
      await Routes.SERVICE.importFile(request,response)
    } catch (e) {
      next(e)
    }
  }

  static async copy(request,response,next) {
    try {
      Routes.LOGGER.info(['YADAMU-SVR','COPY'],request.originalUrl)
      await Routes.SERVICE.copy(request,response)
    } catch (e) {
  	  next(e)
    }
  }

  static async updateConfiguration(request,response,next) {
    try {
      Routes.LOGGER.info(['YADAMU-SVR','UPLOAD-CONFIGURATION'],request.originalUrl)
      await Routes.SERVICE.updateConfiguration(request,response);
    } catch (e) {
      next(e)
    }
  }

  static async executeJobs(request,response,next) {
    try {
      Routes.LOGGER.info(['YADAMU-SVR','EXECUTE-JOBS'],request.originalUrl)
      await Routes.SERVICE.executeJobs(request,response)
    } catch (e) {
  	  next(e)
    }
  }

  static async executeBatch(request,response,next) {
    try {
      Routes.LOGGER.info(['YADAMU-SVR','EXECUTE-BATCH'],request.originalUrl)
      await Routes.SERVICE.executeBatch(request,response)
    } catch (e) {
  	  next(e)
    }
  }

  static async about(request,response,next) {
    try {
      Routes.LOGGER.info(['YADAMU-SVR','ABOUT'],request.originalUrl)
      await Routes.SERVICE.about(request,response)
    } catch (e) {
  	  next(e)
    }
  }

  static recordStartTime(request,response,next) {
	request.yadamuStartTime = performance.now()
	next()
  }

  static log(message) {
    Routes.LOGGER.info(['YADAMU-SVR'],message)
  }

  static handleException(err, req, res, next) {
	// Errors reach here either before a response has started streaming (invalid request: 4xx, internal error: 500) or, for internal errors, after.
	// Either way the client receives a RESULT reporting the failure. See service.js
	try {
	  if (YadamuOutcome.getHttpStatus(err) < 500) {
        Routes.LOGGER.error(['YADAMU-SVR','REQUEST',YadamuOutcome.getHttpStatus(err)],`${req.originalUrl}: ${err.message}`)
	  }
	  else {
        Routes.LOGGER.handleException(['YADAMU-SVR','EXCEPTION',req.originalUrl],err)
	  }
	  Routes.SERVICE.sendFailure(req,res,err)
	} catch (e) {
	  console.log(`${new Date().toISOString()} [ERROR][YADAMU-SVR][EXCEPTION]: Unable to report failure of "${req.originalUrl}" to the client:`,e)
	  if (!res.writableEnded && !res.destroyed) {
		// Abort rather than end the response so the client cannot mistake it for a successful one.
		res.destroy()
	  }
	}
  }

}

export {Routes as default}

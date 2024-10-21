
import Service           from './service.js'

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

  static log(message) {
    Routes.LOGGER.info(['YADAMU-SVR'],message)
  }

  static handleException(err, req, res, next) {
    /*
    if (res.writableEnded) {
    if (!res.finished) {
      Routes.LOGGER.switchOutputStream(res);
      Routes.LOGGER.handleException(['YADAMU-SVR','EXCEPTIION'],err)
    }
    */
    Routes.LOGGER.switchOutputStream(process.stdout);
    Routes.LOGGER.handleException(['YADAMU-SVR','EXCEPTIION'],err)
    // res.status(500).send('An error has occurred, please contact support if the error persists');
    res.end();
  }

}

export {Routes as default}

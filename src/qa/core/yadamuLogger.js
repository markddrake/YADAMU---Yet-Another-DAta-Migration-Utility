"use strict"

import _YadamuLogger from '../..//node/core/yadamuLogger.js';

class YadamuLogger extends _YadamuLogger {
      
  static get LOGGER_CLASS() { return YadamuLogger }

  constructor(outputStream,state,exceptionFolder,exceptionFilePrefix) {
   
    super(outputStream,state,exceptionFolder,exceptionFilePrefix)
  }
   
  qa(args,msg) {
    args.unshift('QA')
    return this.log(args,msg)
  }
  
  qaInfo(args,msg) {
    args.unshift('INFO')
    return this.qa(args,msg)
  }
  
  qaWarning(args,msg) {
    this.state.warningRaised = true;
    this.metrics.warnings++
    args.unshift('WARNING')
    return this.qa(args,msg)
  }

  qaError(args,msg) {
    this.state.errorRaised = true;
    this.metrics.errors++
    args.unshift('ERROR')
    return this.qa(args,msg)
  }

}

export { YadamuLogger as default}
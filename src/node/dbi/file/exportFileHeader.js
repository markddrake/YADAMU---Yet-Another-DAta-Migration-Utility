
import {
  Transform,
  PassThrough
}                          from 'stream'

import DBIConstants        from '../base/dbiConstants.js'

import JSONParser          from './jsonParser.js'

class exportFileHeader extends Transform {

  get SYSTEM_INFORMATION()   { return this._SYSTEM_INFORMATION }
  set SYSTEM_INFORMATION(v)  { this._SYSTEM_INFORMATION = v }
  
  get METADATA()             { return this._METADATA }
  set METADATA(v)            { this._METADATA = v}
  
  get DDL()                  { return this._DDL; }
  set DDL(v)                 { this._DDL = v  }

  get LOGGER()             { return this._LOGGER }
  set LOGGER(v)            { this._LOGGER = v }
  
  get DEBUGGER()           { return this._DEBUGGER }
  set DEBUGGER(v)          { this._DEBUGGER = v }

  constructor(source, importFilePath, yadamuLogger) {
    super({objectMode: true })
	this.LOGGER = yadamuLogger
	this.SYSTEM_INFORMATION = undefined
	this.METADATA = undefined
    this.DDL = []
	this.source = source;
	this.jsonParser = new JSONParser('DDL_ONLY',importFilePath,DBIConstants.PIPELINE_STATE,this.LOGGER)
    source.pipe(this.jsonParser).pipe(this)
  }

  async doTransform(obj) {
    switch (Object.keys(obj)[0]) {
      case 'systemInformation':
        this.SYSTEM_INFORMATION = obj.systemInformation
        break;
      case 'ddl':
        this.DDL = obj.ddl;
		break;
      case 'metadata':
		this.METADATA = obj.metadata
		break
      case 'table':
	  default:
		this.source.unpipe(this.jsonParser)
		this.destroy()
        break;
    }
  }

  _transform(obj,enc,callback) {
	 this.doTransform(obj).then(() => { callback() }).catch((e) => { callback(e) })
  }

}

export { exportFileHeader as default }
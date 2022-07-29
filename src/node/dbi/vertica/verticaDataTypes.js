
import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import VerticaConstants     from './verticaConstants.js'

class VerticaStorageOptions extends YadamuStorageOptions {

  get XML_TYPE()                            { return VerticaConstants.XML_STORAGE_OPTION }	
	
  set XML_TYPE(v)                           { YadamuDataTypes.redefineProperty(this,'XML_TYPE',v) }
    
  get JSON_TYPE()                           { return VerticaConstants.JSON_STORAGE_OPTION }	
	
  set JSON_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'JSON_TYPE',v) }
   	
}

class VerticaDataTypes extends YadamuDataTypes {
  
  get UNBOUNDED_TYPES() { 
    this._UNBOUNDED_TYPES = this._UNBOUNDED_TYPES || Object.freeze([
	  this.INTEGER_TYPE
	, this.FLOAT_TYPE
	, this.INTERVAL_YEAR_TO_MONTH_TYPE
	])
    return this._UNBOUNDED_TYPES;
  }

  get ONE_BYTE_TYPES() {
    this._ONE_BYTE_TYPES = this._ONE_BYTE_TYPES || Object.freeze([
	  this.BOOLEAN_TYPE,
	])
    return this._ONE_BYTE_TYPES;
  }

  get EIGHT_BYTE_TYPES() {
    this._EIGHT_BYTE_TYPES = this._EIGHT_BYTE_TYPES || Object.freeze([
      this.DATE_TYPE
	, this.TIME_TYPE
	, this.TIMESTAMP_TYPE
	, this.TIMESTAMP_TZ_TYPE
	, this.DATETIME_TYPE
	, this.INTERVAL_DTS_TYPE
	, this.INTERVAL_YTM_TYPE
	, this.INTEGER_TYPE
	, this.FLOAT_TYPE
	])
   return this._EIGHT_BYTE_TYPES;
  }
  
  get LOB_TYPES() {
     this._LOB_TYPES = this._LOB_TYPES || Object.freeze([this.CLOB_TYPE,this.BLOB_TYPE])
     return this._LOB_TYPES;
  }

  get DATABASE_KEY()      { return VerticaConstants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return VerticaConstants.DATABASE_VENDOR }

  get STORAGE_OPTIONS()   { return new VerticaStorageOptions() }
 
}
 
export { VerticaDataTypes as default }
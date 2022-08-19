import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import CockroachConstants    from './cockroachConstants.js'

class CockroachStorageOptions extends YadamuStorageOptions {

  get JSON_TYPE()                           { return CockroachConstants.JSON_STORAGE_OPTION }	

  set JSON_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'JSON_TYPE',v) }
  	
}

class CockroachDataTypes extends YadamuDataTypes {
	
  get UNBOUNDED_TYPES() { 
    this._UNBOUNDED_TYPES = this._UNBOUNDED_TYPES || Object.freeze([
      this.SMALLINT_TYPE, 
	  this.INTEGER_TYPE,
	  this.BIGINT_TYPE,
	  this.FLOAT_TYPE,
	  this.DOUBLE_TYPE,
	  this.BINARY_TYPE,
	  this.BLOB_TYPE,
	  this.CLOB_TYPE,
	  this.JSON_TYPE
	])
    return this._UNBOUNDED_TYPES;
  }
	
  isJSON(dataType) {
    return YadamuDataTypes.isJSON(dataType)
  }
  
  get DATABASE_KEY()      { return CockroachConstants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return CockroachConstants.DATABASE_VENDOR }

  get STORAGE_OPTIONS()   { return new CockroachStorageOptions() }

}
 
export { CockroachDataTypes as default }
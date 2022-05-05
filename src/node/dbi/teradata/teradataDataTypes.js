import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import TeradataConstants    from './teradataConstants.js'

class TeradataStorageOptions extends YadamuStorageOptions {

  get BOOLEAN_TYPE()                        { return OracleConstants.BOOLEAN_STORAGE_OPTION }	
	
  set BOOLEAN_TYPE(v)                       { YadamuDataTypes.redefineProperty(this,'BOOLEAN_TYPE',v) }
  
  get RAW1_IS_BOOLEAN()                     { return this.BOOLEAN_TYPE === 'RAW1' }
  

}

class TeradataDataTypes extends YadamuDataTypes {

 
  constructor() {
	super()
	this.sqlDataTypeNames = Object.assign(this,this.TYPE_CONFIGURATION.sqlDataTypeNames)
  }
  
  get UNBOUNDED_TYPES() { 
    this._UNBOUNDED_TYPES = this._UNBOUNDED_TYPES || Object.freeze([
	  this.sqlDataTypeNames[this.DATE_TYPE]     || this.DATE_TYPE
	, this.sqlDataTypeNames[this.TINYINT_TYPE]  || this.TINYINT_TYPE
	, this.sqlDataTypeNames[this.SMALLINT_TYPE] || this.SMALLINT_TYPE
	, this.sqlDataTypeNames[this.INTEGER_TYPE]  || this.INTEGER_TYPE
	, this.sqlDataTypeNames[this.BIGINT_TYPE]   || this.BIGINT_TYPE
	, this.sqlDataTypeNames[this.DOUBLE_TYPE]   || this.DOUBLE_TYPE
	, this.sqlDataTypeNames[this.JSON_TYPE]     || this.JSON_TYPE
    ])
    return this._UNBOUNDED_TYPES;
  }


  get DATABASE_KEY()      { return TeradataConstants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return TeradataConstants.DATABASE_VENDOR }
  
  get STORAGE_OPTIONS()   { return new TeradataStorageOptions() }
  
}

export { TeradataDataTypes as default }

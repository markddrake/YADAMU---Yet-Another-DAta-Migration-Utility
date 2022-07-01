
import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import DB2Constants         from './db2Constants.js'

class DB2StorageOptions extends YadamuStorageOptions {}

class DB2DataTypes extends YadamuDataTypes {
	
  get DATABASE_KEY()      { return DB2Constants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return DB2Constants.DATABASE_VENDOR }
  
  get STORAGE_OPTIONS()   { return new DB2StorageOptions() }

  get UNBOUNDED_TYPES() { 
    this._UNBOUNDED_TYPES = this._UNBOUNDED_TYPES || Object.freeze([
	  this.SMALLINT_TYPE
	, this.INTEGER_TYPE
	, this.BIGINT_TYPE
	, this.FLOAT_TYPE
	, this.DOUBLE_TYPE
	, this.TIME_TYPE
	, this.XML_TYPE
	, this.BOOLEAN_TYPE
	, this.DATE_TYPE
    ])
    return this._UNBOUNDED_TYPES;
  }

  get UCS2_TYPES() { 
    this._UCS2_TYPES = this._UCS2_TYPES || Object.freeze([
	  this.NCHAR_TYPE
	, this.NVARCHAR_TYPE
	, this.NCLOB_TYPE
    ])
    return this._UCS2_TYPES;
  }
  
}
 
export { DB2DataTypes as default }


import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import TeradataConstants    from './teradataConstants.js'

class TeradataStorageOptions extends YadamuStorageOptions {

  // teradataTypeMappings.json sets BYTEINT; TERADATA_BOOLEAN_STORAGE_OPTION overrides it (see TeradataDBI).
  get BOOLEAN_TYPE()                        { return 'BYTEINT' }

  set BOOLEAN_TYPE(v)                       { YadamuDataTypes.redefineProperty(this,'BOOLEAN_TYPE',v) }

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


  coalesceTypeMappings(typeList) {
    // Another database's interval type matches all of Teradata's INTERVAL types of the same kind. Keep the one that holds every value it can have.
    switch (true) {
      case typeList.includes(this.INTERVAL_TYPE):
        return this.INTERVAL_TYPE
      case typeList.includes(this.INTERVAL_DAY_TO_SECOND_TYPE):
        return this.INTERVAL_DAY_TO_SECOND_TYPE
      case typeList.includes(this.INTERVAL_YEAR_TO_MONTH_TYPE):
        return this.INTERVAL_YEAR_TO_MONTH_TYPE
      default:
        return super.coalesceTypeMappings(typeList)
    }
  }

  // A YEAR (MySQL's) is stored as an INTERVAL YEAR, but an INTERVAL YEAR is a duration: -1 or 5 is not a year. It is exported as an interval (P5Y).

  get EXPORT_TYPE_MAPPINGS() {
    return { [this.YEAR_TYPE] : 'INTERVAL_YEAR_TO_MONTH_TYPE' }
  }

  get DATABASE_KEY()      { return TeradataConstants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return TeradataConstants.DATABASE_VENDOR }
  
  get STORAGE_OPTIONS()   { return new TeradataStorageOptions() }
  
}

export { TeradataDataTypes as default }

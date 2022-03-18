
import YadamuDataTypes   from '../base/yadamuDataTypes.js'

class VerticaDataTypes extends YadamuDataTypes {
  
  static get UNBOUNDED_TYPES() { 
    this._UNBOUNDED_TYPES = this._UNBOUNDED_TYPES || Object.freeze([
	  this.INTEGER_TYPE,
	, this.FLOAT_TYPE
	])
    return this._UNBOUNDED_TYPES;
  }

  static get ONE_BYTE_TYPES() {
    this._ONE_BYTE_TYPES = this._ONE_BYTE_TYPES || Object.freeze([
	  this.BOOLEAN_TYPE,
	])
    return this._ONE_BYTE_TYPES;
  }

  static get EIGHT_BYTE_TYPES() {
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
  
  static get LOB_TYPES() {
     this._LOB_TYPES = this._LOB_TYPES || Object.freeze([this.CLOB_TYPE,this.BLOB_TYPE])
     return this._LOB_TYPES;
  }
  
}
 
export { VerticaDataTypes as default }
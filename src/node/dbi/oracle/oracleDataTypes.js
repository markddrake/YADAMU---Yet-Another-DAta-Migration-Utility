
import oracledb from 'oracledb';

import YadamuDataTypes   from '../base/yadamuDataTypes.js'

class OracleDataTypes extends YadamuDataTypes {
  
  static get LOB_TYPES()   { 
    this._LOB_TYPES = this._LOB_TYPES || Object.freeze([oracledb.CLOB,oracledb.BLOB])
    return this._LOB_TYPES
  }

  static get BOUNDED_TYPES() { 
    this._BOUNDED_TYPES = this._BOUNDED_TYPES || Object.freeze([this.CHAR_TYPE,this.NCHAR_TYPE,this.VARCHAR_TYPE,this.NVARCHAR_TYPE,this.BINARY_TYPE])
    return this._BOUNDED_TYPES;
  }

}
 
export { OracleDataTypes as default }
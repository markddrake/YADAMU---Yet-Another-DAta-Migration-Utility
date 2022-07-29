
import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import RedshiftConstants    from './redshiftConstants.js'

class RedshiftStorageOptions extends YadamuStorageOptions {}

class RedshiftDataTypes extends YadamuDataTypes {

  get UNBOUNDED_TYPES() { 
    this._UNBOUNDED_TYPES = this._UNBOUNDED_TYPES || Object.freeze([])
    return this._UNBOUNDED_TYPES;
  }

  get DATABASE_KEY()      { return RedshiftConstants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return RedshiftConstants.DATABASE_VENDOR }
  
  get STORAGE_OPTIONS()   { return new RedshiftStorageOptions() }

}
 
export {RedshiftDataTypes as default }
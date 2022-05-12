
import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import ExampleConstants     from './exampleConstants.js'

class ExampleStorageOptions extends YadamuStorageOptions {}

class ExampleDataTypes extends YadamuDataTypes {

  get DATABASE_KEY()      { return ExampleConstants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return ExampleConstants.DATABASE_VENDOR }
  
  get STORAGE_OPTIONS()   { return new ExampleStorageOptions() }	

  get UNBOUNDED_TYPES() { 
    this._UNBOUNDED_TYPES = this._UNBOUNDED_TYPES || Object.freeze([
    ])
    return this._UNBOUNDED_TYPES;
  }
  
}
 
export { ExampleDataTypes as default }
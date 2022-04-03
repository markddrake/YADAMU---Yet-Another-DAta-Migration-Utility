import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import SnowflakeConstants   from './snowflakeConstants.js'

class SnowflakeStorageOptions extends YadamuStorageOptions {

  get DATABASE_KEY()      { return SnowflakeConstants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return SnowflakeConstants.DATABASE_VENDOR }
  
  get STORAGE_OPTIONS()   { return new SnowflakeStorageOptions() }
  
}

class SnowflakeDataTypes extends YadamuDataTypes {}
 
export {SnowflakeDataTypes as default }
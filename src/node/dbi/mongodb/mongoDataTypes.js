import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import MongoConstants       from './mongoConstants.js'

class MongoStorageOptions extends YadamuStorageOptions {}

class MongoDataTypes extends YadamuDataTypes {

  get DATABASE_KEY()      { return MongoConstants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return MongoConstants.DATABASE_VENDOR }
	
  get STORAGE_OPTIONS()   { return new MongoStorageOptions() }
	
}

export { MongoDataTypes as default }

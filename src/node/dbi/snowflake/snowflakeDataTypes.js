
import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import SnowflakeConstants   from './snowflakeConstants.js'

class SnowflakeStorageOptions extends YadamuStorageOptions {

  get XML_TYPE()                            { return SnowflakeConstants.XML_STORAGE_OPTION }	
	
  set XML_TYPE(v)                           { YadamuDataTypes.redefineProperty(this,'XML_TYPE',v) }
    
  get JSON_TYPE()                           { return SnowflakeConstants.JSON_STORAGE_OPTION }	
	
  set JSON_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'JSON_TYPE',v) }
   	
  
}

class SnowflakeDataTypes extends YadamuDataTypes {

  get UNBOUNDED_TYPES() { 
    this._UNBOUNDED_TYPES = this._UNBOUNDED_TYPES || Object.freeze([this.XML_TYPE,this.JSON_TYPE,this.SNOWFLAKE_VARIANT_TYPE,this.GEOGRAPHY_TYPE,this.FLOAT_TYPE,this.BOOLEAN_TYPE,this.INTEGER_TYPE,this.CLOB_TYPE])
    return this._UNBOUNDED_TYPES;
  }

  get SPATIAL_TYPES() { 
    this._SPATIAL_TYPES = this._SPATIAL_TYPES || Object.freeze([,this.GEOGRAPHY_TYPE])
    return this._SPATIAL_TYPES;
  }

  get INTEGER_TYPES() { 
    this._INTEGER_TYPES = this._INTEGER_TYPES || Object.freeze([this.INTEGER_TYPE])
    return this._INTEGER_TYPES;
  }
  
  get STRONGLY_TYPED_VARIANTS() { 
    this._STRONGLY_TYPED_VARIANTS = this._STRONGLY_TYPED_VARIANTS || Object.freeze([this.XML_TYPE,this.JSON_TYPE])
    return this._STRONGLY_TYPED_VARIANTS;
  }
    
  get DATABASE_KEY()      { return SnowflakeConstants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return SnowflakeConstants.DATABASE_VENDOR }
  
  get STORAGE_OPTIONS()   { return new SnowflakeStorageOptions() }

}
 
export {SnowflakeDataTypes as default }
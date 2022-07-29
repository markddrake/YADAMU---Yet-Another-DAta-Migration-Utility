
import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import MariadbConstants     from './mariadbConstants.js'

class MariadbStorageOptions extends YadamuStorageOptions { 

  get BOOLEAN_TYPE()                        { return MariadbConstants.BOOLEAN_STORAGE_OPTION }	
	
  set BOOLEAN_TYPE(v)                       { YadamuDataTypes.redefineProperty(this,'BOOLEAN_TYPE',v) }
  
  get TINYINT1_IS_BOOLEAN()                 { return this.BOOLEAN_TYPE === 'tinyint(1)' }
  
  get BIT1_IS_BOOLEAN()                     { return this.BOOLEAN_TYPE === 'bit(1)' } 

  get SET_TYPE()                            { return MariadbConstants.SET_STORAGE_OPTION }	
	
  set SET_TYPE(v)                           { YadamuDataTypes.redefineProperty(this,'SET_TYPE',v) }
    
  get ENUM_TYPE()                           { return MariadbConstants.ENUM_STORAGE_OPTION }	
	
  set ENUM_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'ENUM_TYPE',v) }

  get XML_TYPE()                            { return MariadbConstants.XML_STORAGE_OPTION }	
	
  set XML_TYPE(v)                           { YadamuDataTypes.redefineProperty(this,'XML_TYPE',v) }
  
}

class MariadbDataTypes extends YadamuDataTypes {
	
  get UNBOUNDED_TYPES() { 
    this._UNBOUNDED_TYPES = this._UNBOUNDED_TYPES || Object.freeze([
      this.DATE_TYPE
    , this.MYSQL_TINYTEXT_TYPE
    , this.MYSQL_MEDIUMTEXT_TYPE
    , this.MYSQL_TEXT_TYPE
    , this.MYSQL_LONGTEXT_TYPE
    , this.MYSQL_TINYBLOB_TYPE
    , this.MYSQL_MEDIUMBLOB_TYPE
    , this.MYSQL_BLOB_TYPE
    , this.MYSQL_LONGBLOB_TYPE
	, this.FLOAT_TYPE
	, this.DOUBLE_TYPE
    , this.JSON_TYPE
    , this.MYSQL_SET_TYPE
    , this.MYSQL_ENUM_TYPE
	])
    return this._UNBOUNDED_TYPES;
  }
  
  get SPATIAL_TYPES() { 
    this._SPATIAL_TYPES = this._SPATIAL_TYPES || Object.freeze([
      this.GEOMETRY_TYPE
    , this.GEOGRAPHY_TYPE
    , this.POINT_TYPE
    , this.LINE_TYPE
    , this.POLYGON_TYPE
    , this.MULTI_POINT_TYPE
    , this.MULTI_LINE_TYPE
    , this.MULTI_POLYGON_TYPE
    , this.GEOMETRY_COLLECTION_TYPE
	])
    return this._SPATIAL_TYPES;
  }

  get INTEGER_TYPES() { 
    this._INTEGER_TYPES = this._INTEGER_TYPES || Object.freeze([
      this.TINYINT_TYPE
    , this.SMAILLINT_TYPE
    , this.MEDIUMINT_TYPE
    , this.INTEGER_TYPE
    , this.BIGINT_TYPE
	])
    return this._INTEGER_TYPES;
  }

  get DATABASE_KEY()      { return MariadbConstants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return MariadbConstants.DATABASE_VENDOR }
  
  get STORAGE_OPTIONS()   { return new MariadbStorageOptions() }
  
}

export { MariadbDataTypes as default }

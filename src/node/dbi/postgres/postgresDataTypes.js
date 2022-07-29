
import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import PostgresConstants    from './postgresConstants.js'

class PostgresStorageOptions extends YadamuStorageOptions {

  get JSON_TYPE()                           { return PostgresConstants.JSON_STORAGE_OPTION }	

  set JSON_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'JSON_TYPE',v) }
  	
}

class PostgresDataTypes extends YadamuDataTypes {
	
  get POSTGRES_JSON_TYPES() {
    return this._POSTGRES_JSON_TYPES || (() => {
	  this._POSTGRES_JSON_TYPES = Object.freeze([
        this.PGSQL_RANGE_INT4_TYPE                                         
      , this.PGSQL_RANGE_INT8_TYPE                                         
      , this.PGSQL_RANGE_NUM_TYPE                                          
      , this.PGSQL_RANGE_TIMESTAMP_TYPE                                    
      , this.PGSQL_RANGE_TIMESTAMP_TZ_TYPE                                 
      , this.PGSQL_RANGE_DATE_TYPE                                         
      , this.PGSQL_TIMESTAMP_VECTOR                                        
      , this.PGSQL_ACLITEM                                                 
      , this.PGSQL_REFCURSOR                               
      , this.PGSQL_TEXTSEACH_VECTOR_TYPE                                   
      , this.PGSQL_TEXTSEACH_QUERY_TYPE                                  
	  , this.JSON_TYPE                                                     
      , this.PGSQL_BINARY_JSON_TYPE                                        
      ])
	  return this._POSTGRES_JSON_TYPES
	})()
  }

  isJSON(dataType) {
    return this.POSTGRES_JSON_TYPES.includes(dataType.toLowerCase());
  }
   
  coalesceTypeMappings(typeList) {
    
    switch (true) {
      case typeList.includes(this.SPATIAL_TYPE):
        return this.SPATIAL_TYPE
      case typeList.includes(this.GEOMETRY_TYPE):
        return this.GEOMETRY_TYPE
      case typeList.includes(this.GEOGRAPHY_TYPE):
        return this.GEOGRAPHY_TYPE
      case typeList.includes(this.JSON_TYPE):
        return this.JSON_TYPE
      case typeList.includes(this.POLYGON_TYPE):
        return this.POLYGON_TYPE
      case typeList.includes(this.LINE_TYPE):
        return this.LINE_TYPE
      case typeList.includes(this.CLOB_TYPE):
        return this.CLOB_TYPE
      case typeList.includes(this.VARCHAR_TYPE):
        return this.VARCHAR_TYPE
      case typeList.includes(this.CHAR_TYPE):
        return this.CHAR_TYPE
      case typeList.includes(this.BLOB_TYPE):
        return this.BLOB_TYPE
      case typeList.includes(this.VARBINARY_TYPE):
        return this.VARBINARY_TYPE
      case typeList.includes(this.BINARY_TYPE):
        return this.BINARY_TYPE
      case typeList.includes(this.BIGINT_TYPE):
        return this.BIGINT_TYPE     
      case typeList.includes(this.INTEGER_TYPE):
        return this.INTEGER_TYPE        
      case typeList.includes(this.MEDIUMINT_TYPE):
        return this.MEDIUMINT_TYPE      
      case typeList.includes(this.SMALLINT_TYPE):
        return this.SMALLINT_TYPE       
      case typeList.includes(this.DOUBLE_TYPE):
        return this.DOUBLE_TYPE
      case typeList.includes(this.NUMERIC_TYPE):
        return this.NUMERIC_TYPE
      case typeList.includes(this.TIMESTAMP_TZ_TYPE):
        return this.TIMESTAMP_TZ_TYPE
      case typeList.includes(this.TIMESTAMP_TYPE):
        return this.TIMESTAMP_TYPE
      case typeList.includes(this.TIME_TZ_TYPE):
        return this.TIME_TZ_TYPE
      case typeList.includes(this.DATETIME_TYPE):
        return this.DATETIME_TYPE
      case typeList.includes(this.PGSQL_OID_TYPE):
	    return this.PGSQL_OID_TYPE
	  default:
        console.log(this.constructor.name,'Type List Reduction failed for ',typeList)
    }
  }
  
  get DATABASE_KEY()      { return PostgresConstants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return PostgresConstants.DATABASE_VENDOR }

  get STORAGE_OPTIONS()   { return new PostgresStorageOptions() }

}
 
export { PostgresDataTypes as default }
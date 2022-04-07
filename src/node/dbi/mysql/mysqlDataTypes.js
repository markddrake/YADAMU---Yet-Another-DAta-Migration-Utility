
import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import MySQLConstants       from './mysqlConstants.js'

class MySQLStorageOptions extends YadamuStorageOptions {
  
  get BOOLEAN_TYPE()                        { return MySQLConstants.BOOLEAN_STORAGE_OPTION }	
	
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

class MySQLDataTypes extends YadamuDataTypes {

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
	  case typeList.includes(this.MYSQL_SET_TYPE):
	    return this.MYSQL_SET_TYPE		
	  case typeList.includes(this.MYSQL_ENUM_TYPE):
	    return this.MYSQL_ENUM_TYPE		
	  default:
	    console.log(this.name,'Type List Reduction failed for ',typeList)
	}
  }

  get DATABASE_KEY()      { return MySQLConstants.DATABASE_KEY }
 
  get DATABASE_VENDOR()   { return MySQLConstants.DATABASE_VENDOR }   
  
  get STORAGE_OPTIONS()   { return new MySQLStorageOptions() }

}
 
export { MySQLDataTypes as default }
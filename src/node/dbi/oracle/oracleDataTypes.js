import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import OracleConstants      from './oracleConstants.js'

class OracleStorageOptions extends YadamuStorageOptions {
	 
  get XML_TYPE()                            { return OracleConstants.XML_STORAGE_OPTION }	

  set XML_TYPE(v)                           { YadamuDataTypes.redefineProperty(this,'XML_TYPE',v) }
  
  get JSON_TYPE()                           { return OracleConstants.JSON_STORAGE_OPTION }	

  set JSON_TYPE(v)                          { YadamuDataTypes.redefineProperty(this,'JSON_TYPE',v) }
  
  get BOOLEAN_TYPE()                        { return OracleConstants.BOOLEAN_STORAGE_OPTION }	
	
  set BOOLEAN_TYPE(v)                       { YadamuDataTypes.redefineProperty(this,'BOOLEAN_TYPE',v) }
  
  get RAW1_IS_BOOLEAN()                     { return this.BOOLEAN_TYPE === 'RAW1' }
  
  get ORACLE_OBJECT_TYPE()                  { return OracleConstants.OBJECT_STORAGE_OPTION }	
	
  set ORACLE_OBJECT_TYPE(v)                 { YadamuDataTypes.redefineProperty(this,'ORACLE_OBJECT_TYPE',v) }
  
}

class OracleDataTypes extends YadamuDataTypes {

  get LOB_TYPES()   { 
    this._LOB_TYPES = this._LOB_TYPES || Object.freeze([this.CLOB_TYPE,this.BLOB_TYPE,this.NCLOB_TYPE])
    return this._LOB_TYPES
  }
 
  get BOUNDED_TYPES() { 
    this._BOUNDED_TYPES = this._BOUNDED_TYPES || Object.freeze([this.CHAR_TYPE,this.NCHAR_TYPE,this.VARCHAR_TYPE,this.NVARCHAR_TYPE,this.BINARY_TYPE])
    return this._BOUNDED_TYPES;
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
      case typeList.includes(this.CLOB_TYPE):
	    return this.CLOB_TYPE
      case typeList.includes(this.NCLOB_TYPE):
	    return this.NCLOB_TYPE
      case typeList.includes(this.VARCHAR_TYPE):
	    return this.VARCHAR_TYPE
      case typeList.includes(this.NVARCHAR_TYPE):
	    return this.NVARCHAR_TYPE
      case typeList.includes(this.CHAR_TYPE):
	    return this.CHAR_TYPE
      case typeList.includes(this.NCHAR_TYPE):
	    return this.NCHAR_TYPE
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
	  default:
	    const dataTypeDefinitions = YadamuDataTypes.decomposeDataTypes(typeList)
		// Check for common case of NUMBER mapped to different sized integers: return largest
		if (dataTypeDefinitions.every((dataTypeDefinition) => { return ((dataTypeDefinition.type === 'NUMBER') && (dataTypeDefinition.scale === 0)) })) {
		  return `NUMBER(${Math.max(...dataTypeDefinitions.map((dataTypeDefinition) => { return dataTypeDefinition.length }))},0)`
		}
        console.log(this.name,'Type List Reduction failed for ',typeList)
	}
  } 

  get DATABASE_KEY()      { return OracleConstants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return OracleConstants.DATABASE_VENDOR }
  
  get STORAGE_OPTIONS()   { return new OracleStorageOptions() }
  
}
 
export { OracleDataTypes as default }
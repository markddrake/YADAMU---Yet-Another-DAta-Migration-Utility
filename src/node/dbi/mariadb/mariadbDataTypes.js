
import YadamuDataTypes   from '../base/yadamuDataTypes.js'

class MariadbDataTypes extends YadamuDataTypes {

  static get UNBOUNDED_TYPES() { 
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
  
  static get SPATIAL_TYPES() { 
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

  static get INTEGER_TYPES() { 
    this._INTEGER_TYPES = this._INTEGER_TYPES || Object.freeze([
      this.TINYINT_TYPE
    , this.SMAILLINT_TYPE
    , this.MEDIUMINT_TYPE
    , this.INTEGER_TYPE
    , this.BIGINT_TYPE
	])
    return this._INTEGER_TYPES;
  }
  
}

export { MariadbDataTypes as default }

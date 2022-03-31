
import YadamuDataTypes   from '../base/yadamuDataTypes.js'

class PostgresDataTypes extends YadamuDataTypes {
	
  static get POSTGRES_JSON_TYPES() {
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

  static isJSON(dataType) {
    return this.POSTGRES_JSON_TYPES.includes(dataType.toLowerCase());
  }
}
 
export { PostgresDataTypes as default }
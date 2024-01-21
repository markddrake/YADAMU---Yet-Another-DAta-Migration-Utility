
import {
  setTimeout 
}                       from "timers/promises"

import VerticaDBI       from '../../../node/dbi/vertica/verticaDBI.js';
import {VerticaError}   from '../../../node/dbi/vertica/verticaException.js'
import VerticaConstants from '../../../node/dbi/vertica/verticaConstants.js';

import Yadamu           from '../../core/yadamu.js';
import YadamuQALibrary  from '../../lib/yadamuQALibrary.js'


class VerticaQA extends YadamuQALibrary.qaMixin(VerticaDBI) {
    
    static #_DBI_PARAMETERS
    
    static get DBI_PARAMETERS()  { 
       this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,VerticaConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[VerticaConstants.DATABASE_KEY] || {},{RDBMS: VerticaConstants.DATABASE_KEY}))
       return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return VerticaQA.DBI_PARAMETERS
    }   
    
    constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters);
    }

    async recreateSchema() {
      try {
        const dropSchema = `drop schema if exists "${this.parameters.TO_USER}" cascade`;
        await this.executeSQL(dropSchema);      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }        
        else {
          this.yadamu.LOGGER.handleException([this.DATABASE_VENDOR,'RECREATE DATABASE',this.parameters.TO_USER],e);
        }
      }
      await this.createSchema(this.parameters.TO_USER);    
    }      
	
    buildColumnLists(schemaMetadata,rules) {
		
	  const compareInfo = {}
	  schemaMetadata.forEach((tableMetadata) => {
        compareInfo[tableMetadata.TABLE_NAME] = tableMetadata.DATA_TYPE_ARRAY.map((dataType,idx) => {
		  const columnName = tableMetadata.COLUMN_NAME_ARRAY[idx]
		  switch (dataType) {
			case (this.DATA_TYPES.XML_TYPE):
			  return rules.XML_COMPARISON_RULE === 'STRIP_XML_DECLARATION' ? `YADAMU.STRIP_XML_DECLARATION("${columnName}")` : `"${columnName}"`
		    case this.DATA_TYPES.JSON_TYPE:
              return `case when "${columnName}" is NULL then NULL when SUBSTR("${columnName}",1,1) = '{' then MAPTOSTRING(MAPJSONEXTRACTOR("${columnName}")) when SUBSTR("${columnName}",1,1) = '[' then MAPTOSTRING(mapDelimitedExtractor(substr("${columnName}",2,length("${columnName}")-2) using parameters delimiter=',')) else "${columnName}" end`
            case this.DATA_TYPES.CHAR_TYPE :
            case this.DATA_TYPES.VARCHAR_TYPE :
            case this.DATA_TYPES.CLOB_TYPE :
		      return rules.EMPTY_STRING_IS_NULL ? `case when "${columnName}" = '' then NULL else "${columnName}" end` : `"${columnName}"`
		    case this.DATA_TYPES.DOUBLE_TYPE :
		      const columnClause = rules.DOUBLE_PRECISION !== null ? `round("${columnName}",${rules.DOUBLE_PRECISION})` : `"${columnName}"`
			  return rules.INFINITY_IS_NULL ? `case when "${columnName}" = 'Inf' then NULL when "${columnName}" = '-Inf' then NULL when "${columnName}" <> "${columnName}" then NULL else ${columnClause} end` : columnClause
		    case this.DATA_TYPES.NUMERIC_TYPE :
		    case this.DATA_TYPES.DECIMAL_TYPE:
              return ((rules.NUMERIC_SCALE !== null) && (rules.NUMERIC_SCALE < tableMetadata.SIZE_CONSTRAINT_ARRAY[idx][0])) ? `round("${columnName}",${rules.NUMERIC_SCALE})` : `"${columnName}"`
  		    case this.DATA_TYPES.GEOGRAPHY_TYPE :
            case this.DATA_TYPES.GEOMETRY_TYPE :
		      return `case when YADAMU.invalidGeoHash("${columnName}") then ST_AsText("${columnName}") else ${rules.SPATIAL_PRECISION < 20 ? `ST_GeoHash("${columnName}" USING PARAMETERS NUMCHARS=${rules.SPATIAL_PRECISION})` : `ST_GeoHash("${columnName}")`} end`
            default:
              return `"${columnName}"`
          }
	    }).join(',')
      })
	  return compareInfo
    }
 
    classFactory(yadamu) {
      return new VerticaQA(yadamu,this,this.connectionParameters,this.parameters)
    }
	
    async scheduleTermination(pid,workerId) {
      let stack
	  const operation = `select pg_terminate_backend(${pid})`
	  const tags = this.getTerminationTags(workerId,pid)
	  this.LOGGER.qa(tags,`Termination Scheduled.`);
      setTimeout(this.yadamu.KILL_DELAY,pid,{ref : false}).then(async (pid) => {
        if (this.pool !== undefined && this.pool.end) {
          stack = new Error().stack
          this.LOGGER.log(tags,`Killing connection.`);
          const conn = await this.getConnectionFromPool();
		  const res = await conn.query(operation);
          await conn.release()
        }
        else {
          this.LOGGER.log(tags,`Unable to Kill Connection: Connection Pool no longer available.`);
        }
      }).catch((e) => {
		const cause = this.createDatabaseError(this.DRIVER_ID,e,stack,operation)
        this.LOGGER.handleException(tags,cause)
      })
    }
  
}

export { VerticaQA as default }

"use strict" 

import {
  setTimeout 
}                         from "timers/promises"

import CockroachError     from '../../../node/dbi/postgres/postgresException.js'

import CockroachDBI       from '../../../node/dbi/cockroach/cockroachDBI.js';
import CockroachConstants from '../../../node/dbi/cockroach/cockroachConstants.js';

import Yadamu             from '../../core/yadamu.js';
import YadamuQALibrary    from '../../lib/yadamuQALibrary.js'


class CockroachQA extends YadamuQALibrary.qaMixin(CockroachDBI) {
    
    static get SQL_SCHEMA_TABLE_NAMES()    { return _SQL_SCHEMA_TABLE_NAMES }
    static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }

	static #_DBI_PARAMETERS
	
	static get DBI_PARAMETERS()  { 
	   this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,CockroachConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[CockroachConstants.DATABASE_KEY] || {},{RDBMS: CockroachConstants.DATABASE_KEY}))
	   return this.#_DBI_PARAMETERS
    }
   
    get DBI_PARAMETERS() {
      return CockroachQA.DBI_PARAMETERS
    }	
		
    constructor(yadamu,manager,connectionSettings,parameters) {
       super(yadamu,manager,connectionSettings,parameters);
    }

 	async recreateSchema() {
      try {
        const dropSchema = `drop schema if exists "${this.parameters.TO_USER}" cascade`;
        const results = await this.executeSQL(dropSchema);      
      } catch (e) {
        if (e.errorNum && (e.errorNum === 1918)) {
        }
        else {
          throw e;
        }
      }
      await this.createSchema(this.parameters.TO_USER);    
    }      

async getRowCounts(target) {
		

	  let results = await this.executeSQL(`select table_name from information_schema.tables where table_schema = '${this.CURRENT_SCHEMA}'`)
	  results = results.rows.map((result) => { return Object.values(result)})
	  if (results.length > 0 ) {
	    const sqlCountRows = results.map((row) => { return `select cast('${this.CURRENT_SCHEMA}' as VARCHAR(128)), cast('${row[0]}' as VARCHAR(128)), count(*) from "${this.CURRENT_SCHEMA}"."${row[0]}"`}).join('\nunion all \n')
	    results = await this.executeSQL(sqlCountRows)
	    results = results.rows.map((result) => { return Object.values(result)})
	  }
	  return results
    }    


    async compareSchemas(source,target,rules) {
  
	  
      const report = {
        successful : []
       ,failed     : []
      }

      const compareRules = this.yadamu.getCompareRules(rules)	  

      /*
      await this.closeConnection()	  
	  this.connection = await this.getConnectionFromPool()
	  await this.beginTransaction()
      let results = await this.executeSQL(`set transaction isolation level serializable read only deferrable`);
      await this.configureConnection()	  
	  */
	  
      await this.executeSQL(YugabyteQA.SQL_COMPARE_SCHEMAS,[source.schema,target.schema,compareRules])      

         
      const successful = await this.executeSQL(YugabyteQA.SQL_SUCCESS)            
      report.successful = successful.rows
      
      const failed = await this.executeSQL(YugabyteQA.SQL_FAILED)
      report.failed = failed.rows

	  await this.rollbackTransaction()

      return report
    }
	
	getSpatialClause(columnName,spatialPrecision) {
		
	  if (spatialPrecision === 18) {
		return `ST_ASEWKB("${columnName}")`
	  }
	  else {		
	    return `COALESCE(
         ST_ASEWKB((
		   select concat('POINT(',round(ST_X("${columnName}"::geometry)::numeric,${spatialPrecision})::text,' ',round(ST_Y("${columnName}"::geometry)::numeric,${spatialPrecision})::text,')')::geometry
		     from (
			   select "${columnName}"::geometry "${columnName}"
			 ) as "POINT"
			where ST_GeometryType("${columnName}"::geometry) = 'ST_Point'
         )),
         ST_ASEWKB(( 
		   select concat('MULTIPOLYGON(',string_agg("POLYGON",','),')')::geometry
             from (
               select 1 as "X", concat('(',string_agg("RING",','),')') as "POLYGON"
			     from (
                   select level1, level2, concat('(',string_agg(concat(
				     case 
					   when (point ->> 0)::numeric = -180 then
					     '180'
					   else
					     round((point ->> 0)::numeric,${spatialPrecision})::text
					 end
					 ,' ',
					 case
					   when (point ->> 1)::numeric = -180 then
					     '180'
					   else
					     round((point ->> 1)::numeric,${spatialPrecision})::text
					 end
                     ),','),')') as "RING" 
	                 from JSONB_ARRAY_ELEMENTS(ST_ASGEOJSON("${columnName}")::JSONB -> 'coordinates') WITH ORDINALITY AS t1 (polygon, level1), 
		                  JSONB_ARRAY_ELEMENTS(polygon) WITH ORDINALITY AS t2 (ring,  level2),
		                  JSONB_ARRAY_ELEMENTS(ring) WITH ORDINALITY AS t3 (point, level3)
                    group by  level1, level2
                 ) as "RINGS"
		        group by level1
			 ) as "POLYGONS"
            where ST_GeometryType("${columnName}"::geometry) = 'ST_MultiPolygon' 
            group by "X"
	     )),      
         ST_ASEWKB((
		   select concat('POLYGON(',string_agg("RING",','),')')::geometry
            from (
              select 1 as "X", level1, concat('(',string_agg(concat(round((point ->> 0)::numeric,${spatialPrecision})::text,' ',round((point ->> 1)::numeric,${spatialPrecision})::text),','),')') as "RING"
	            from JSONB_ARRAY_ELEMENTS(ST_ASGEOJSON("${columnName}")::JSONB -> 'coordinates')  WITH ORDINALITY AS t4 (ring, level1), 
		             JSONB_ARRAY_ELEMENTS(ring)  WITH ORDINALITY AS t5 (point, level2)
                group by  level1
            ) as "RINGS"
            where ST_GeometryType("${columnName}"::geometry) = 'ST_Polygon'
            group by "X"
	     )),
		 ST_ASEWKB("${columnName}"::geometry)
	   ) "${columnName}"`
	  }
	}

	buildColumnLists(schemaInfo,rules) {
	

	  return schemaInfo.map((tableInfo) => {
		  return {
			TABLE_NAME   : tableInfo.TABLE_NAME
		  , COLUMN_LIST  : tableInfo.COLUMN_NAME_ARRAY.map((columnName,idx) => { 
		     switch (tableInfo.DATA_TYPE_ARRAY[idx]) {
			   case this.DATA_TYPES.JSON_TYPE:
			     return `to_jsonb("${columnName}")::text`
			   case this.DATA_TYPES.CLOB_TYPE:
			   case this.DATA_TYPES.NCLOB_TYPE:
			   case this.DATA_TYPES.VARCHAR_TYPE:
			     return rules.EMPTY_STRING_IS_NULL ? `case when "${columnName}" = '' then NULL else "${columnName}" end` : `"${columnName}"`
               case this.DATA_TYPES.FLOAT_TYPE:
			   case this.DATA_TYPES.DOUBLE_TYPE:
			     return rules.INFINITY_IS_NULL ? `case when "${columnName}" in ('Infinity','-Infinity','NaN') then NULL else "${columnName}" end` : rules.DOUBLE_PRECISION && rules.DOUBLE_PRECISION < 18 ? `round("${columnName}"::numeric,${rules.DOUBLE_PRECISION})` : `"${columnName}"`
               case this.DATA_TYPES.NUMERIC_TYPE:
			   case this.DATA_TYPES.DECIMAL_TYPE:
			     switch (true) {
                   case (((rules.NUMERIC_SCALE !== null) && (tableInfo.SIZE_CONSTRAINT_ARRAY[idx].length === 0)) || (rules.NUMERIC_SCALE && rules.NUMERIC_SCALE  < tableInfo.SIZE_CONSTRAINT_ARRAY[idx][0])):
   					 return `round("${columnName}",${rules.NUMERIC_SCALE})`
				   default:
  				     return (tableInfo.SIZE_CONSTRAINT_ARRAY[idx].length === 0) ? `CASE WHEN "${columnName}" = 0 then 0 WHEN trim("${columnName}"::text, '0')::numeric = "${columnName}" THEN trim("${columnName}"::text, '0')::numeric ELSE "${columnName}" END` : `"${columnName}"`
				 }
               case this.DATA_TYPES.SPATIAL_TYPE:
               case this.DATA_TYPES.GEOMETRY_TYPE:
               case this.DATA_TYPES.GEOGRAPHY_TYPE:			   
			      return this.getSpatialClause(columnName,rules.SPATIAL_PRECISION)
			      // return rules.SPATIAL_PRECISION === 18 ? `ST_AsEWKB("${columnName}")` : `ST_AsEWKT("${columnName}",${rules.SPATIAL_PRECISION})`
               default: 
				 return `"${columnName}"`
			  }
		   }).join(',')
		  }
	  })
    }  
	
	  
    async compareSchemas(source,target,rules) {

      const report = {
        successful : []
       ,failed     : []
      }
	  
	  this.parameters.FROM_USER = source.schema;
      const schemaInfo = (await this.getSchemaMetadata()).filter((tableInfo) => {
         return ((this.TABLE_FILTER.length === 0) || (this.TABLE_FILTER.includes(tableInfo.TABLE_NAME)))
	   })
	  
      let columnLists = this.buildColumnLists(schemaInfo,rules)

      const timeout_period = 30 * 60 * 1000;
      let results = await this.executeSQL(`set statement_timeout=${timeout_period}`);
	  
	  const compareResults = [] 
      for (const table of columnLists) {
        const sqlStatement =
`select 
  '${table.TABLE_NAME}' "TABLE_NAME",
  (select count(*) from "${source.schema}"."${table.TABLE_NAME}") SOURCE_ROWS,
  (select count(*) from "${target.schema}"."${table.TABLE_NAME}") TARGET_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from "${source.schema}"."${table.TABLE_NAME}" except select ${table.COLUMN_LIST} from "${target.schema}"."${table.TABLE_NAME}") T1) EXTRA_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from "${target.schema}"."${table.TABLE_NAME}" except select ${table.COLUMN_LIST} from "${source.schema}"."${table.TABLE_NAME}") T2) MISSING_ROWS`
  
        try {
          let results = await this.executeSQL(sqlStatement);
  		  results = Object.values(results.rows[0])
          compareResults.push(results)
		} catch (e) {
		  this.yadamuLogger.handleException([this.DATABASE_VENDOR,'QA','COMPARE_SCHEMA'],e)  
		  compareResults.push([table[0],-1,-1,-1,-1,e.message])
		}
	  }
	 
	 compareResults.forEach((results,idx) => {
        const compareResult =  results
        if ((parseInt(compareResult[1]) === parseInt(compareResult[2])) && (parseInt(compareResult[3]) === 0) && (parseInt(compareResult[4])  === 0)) {
          report.successful.push(new Array(source.schema,target.schema,compareResult[0],compareResult[2]))
        }
        else {
          report.failed.push(new Array(source.schema,target.schema,...compareResult,''))
        }
      })
     
      return report
    }  
   
   classFactory(yadamu) {
      return new CockroachQA(yadamu,this,this.connectionParameters,this.parameters)
    }
	
	async scheduleTermination(pid,workerId) {
	  let stack
	  const operation = `select pg_terminate_backend(${pid})`
	  const tags = this.getTerminationTags(workerId,pid)
	  this.yadamuLogger.qa(tags,`Termination Scheduled.`);
	  setTimeout(this.yadamu.KILL_DELAY,pid,{ref : false}).then(async (pid) => {
        if (this.pool !== undefined && this.pool.end) {
	      stack = new Error().stack
		  this.yadamuLogger.log(tags,`Killing connection.`);
	      const conn = await this.getConnectionFromPool();
		  const res = await conn.query(operation);
		  await conn.release()
		}
		else {
		  this.yadamuLogger.log(tags,`Unable to Kill Connection: Connection Pool no longer available.`);
		}
      }).catch((e) => {
        this.yadamu.LOGGER.handleException(tags,new CockroachError(this.DRIVER_ID,e,stack,operation));
      })
	}

}

export { CockroachQA as default }

const _SQL_SUCCESS =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT
   from SCHEMA_COMPARE_RESULTS 
  where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
    and MISSING_ROWS = 0
    and EXTRA_ROWS = 0
    and SQLERRM is NULL
 order by TABLE_NAME`;

const _SQL_FAILED = 
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS,  SQLERRM
   from SCHEMA_COMPARE_RESULTS 
  where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
     or MISSING_ROWS <> 0
      or EXTRA_ROWS <> 0
    or SQLERRM is NOT NULL
  order by TABLE_NAME`;
  
const _SQL_SCHEMA_TABLE_NAMES = `select relname from pg_stat_user_tables where schemaname = $1`;

// const _SQL_SCHEMA_TABLE_ROWS  = `select schemaname, relname, n_live_tup from pg_stat_user_tables where schemaname = $1`;

const _SQL_SCHEMA_TABLE_ROWS = 
`with ROW_COUNTS as (
select table_schema, table_name,  query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
  from information_schema.tables
 where table_schema = $1 
)
select table_schema, table_name, (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
  from ROW_COUNTS`

const _SQL_COMPARE_SCHEMAS    = `call COMPARE_SCHEMA($1,$2,$3)`
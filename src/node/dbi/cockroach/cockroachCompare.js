
import YadamuCompare   from '../base/yadamuCompare.js'

class CockroachCompare extends YadamuCompare {

    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

	async getRowCounts(target) {
	
	  let results = await this.dbi.executeSQL(`select table_name from information_schema.tables where table_schema = '${target}'`)
	  results = results.rows.map((result) => { return Object.values(result)})
	  if (results.length > 0 ) {
	    const sqlCountRows = results.map((row) => { return `select cast('${target}' as VARCHAR(128)), cast('${row[0]}' as VARCHAR(128)), count(*) from "${target}"."${row[0]}"`}).join('\nunion all \n')
	    results = await this.dbi.executeSQL(sqlCountRows)
		results = results.rows.map((result) => { return Object.values(result)})
	  }
	  return results
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
			   case this.dbi.DATA_TYPES.JSON_TYPE:
			     return `to_jsonb("${columnName}")::text`
			   case this.dbi.DATA_TYPES.CLOB_TYPE:
			   case this.dbi.DATA_TYPES.NCLOB_TYPE:
			   case this.dbi.DATA_TYPES.VARCHAR_TYPE:
			     return rules.EMPTY_STRING_IS_NULL ? `case when "${columnName}" = '' then NULL else "${columnName}" end` : `"${columnName}"`
               case this.dbi.DATA_TYPES.FLOAT_TYPE:
			   case this.dbi.DATA_TYPES.DOUBLE_TYPE:
			     return rules.INFINITY_IS_NULL ? `case when "${columnName}" in ('Infinity','-Infinity','NaN') then NULL else "${columnName}" end` : rules.DOUBLE_PRECISION && rules.DOUBLE_PRECISION < 18 ? `round("${columnName}"::numeric,${rules.DOUBLE_PRECISION})` : `"${columnName}"`
               case this.dbi.DATA_TYPES.NUMERIC_TYPE:
			   case this.dbi.DATA_TYPES.DECIMAL_TYPE:
			     switch (true) {
                   case (((rules.NUMERIC_SCALE !== null) && (tableInfo.SIZE_CONSTRAINT_ARRAY[idx].length === 0)) || (rules.NUMERIC_SCALE && rules.NUMERIC_SCALE  < tableInfo.SIZE_CONSTRAINT_ARRAY[idx][0])):
   					 return `round("${columnName}",${rules.NUMERIC_SCALE})`
				   default:
  				     return (tableInfo.SIZE_CONSTRAINT_ARRAY[idx].length === 0) ? `CASE WHEN "${columnName}" = 0 then 0 WHEN trim("${columnName}"::text, '0')::numeric = "${columnName}" THEN trim("${columnName}"::text, '0')::numeric ELSE "${columnName}" END` : `"${columnName}"`
				 }
               case this.dbi.DATA_TYPES.SPATIAL_TYPE:
               case this.dbi.DATA_TYPES.GEOMETRY_TYPE:
               case this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:			   
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
	  
	  this.dbi.parameters.FROM_USER = source;
      const schemaInfo = (await this.dbi.getSchemaMetadata()).filter((tableInfo) => {
         return ((this.dbi.TABLE_FILTER.length === 0) || (this.dbi.TABLE_FILTER.includes(tableInfo.TABLE_NAME)))
	   })
	  
      let columnLists = this.buildColumnLists(schemaInfo,rules)

      const timeout_period = 30 * 60 * 1000;
      let results = await this.dbi.executeSQL(`set statement_timeout=${timeout_period}`);
	  
	  const compareResults = [] 
      for (const table of columnLists) {
        const sqlStatement =
`select 
  '${table.TABLE_NAME}' "TABLE_NAME",
  (select count(*) from "${source}"."${table.TABLE_NAME}") SOURCE_ROWS,
  (select count(*) from "${target}"."${table.TABLE_NAME}") TARGET_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from "${source}"."${table.TABLE_NAME}" except select ${table.COLUMN_LIST} from "${target}"."${table.TABLE_NAME}") T1) EXTRA_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from "${target}"."${table.TABLE_NAME}" except select ${table.COLUMN_LIST} from "${source}"."${table.TABLE_NAME}") T2) MISSING_ROWS`
  
        try {
          let results = await this.dbi.executeSQL(sqlStatement);
  		  results = Object.values(results.rows[0])
          compareResults.push(results)
		} catch (e) {
		  // this.yadamuLogger.handleException([this.dbi.DATABASE_VENDOR,'QA','COMPARE_SCHEMA'],e)  
		  compareResults.push([table[0],-1,-1,-1,-1,e.message])
		}
	  }
	 
	 compareResults.forEach((results,idx) => {
        const compareResult =  results
        if ((parseInt(compareResult[1]) === parseInt(compareResult[2])) && (parseInt(compareResult[3]) === 0) && (parseInt(compareResult[4])  === 0)) {
          report.successful.push(new Array(source,target,compareResult[0],compareResult[2]))
        }
        else {
          report.failed.push(new Array(source,target,...compareResult,''))
        }
      })
     
      return report
    }  

}
export { CockroachCompare as default }

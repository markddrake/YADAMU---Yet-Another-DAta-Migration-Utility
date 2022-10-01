
import YadamuCompare   from '../base/yadamuCompare.js'

class VerticaCompare extends YadamuCompare {    static get SQL_SCHEMA_TABLE_NAMES()    { return _SQL_SCHEMA_TABLE_NAMES }

    // static get SQL_SCHEMA_TABLE_ROWS()     { return _SQL_SCHEMA_TABLE_ROWS }
    static get SQL_COMPARE_SCHEMAS()       { return _SQL_COMPARE_SCHEMAS }
    static get SQL_SUCCESS()               { return _SQL_SUCCESS }
    static get SQL_FAILED()                { return _SQL_FAILED }

    static SQL_SCHEMA_TABLE_ROWS(schema) { return `with num_rows as (
    select schema_name,    
           anchor_table_name as table_name,
           sum(total_row_count) as rows
      from v_monitor.storage_containers sc
      join v_catalog.projections p
           on sc.projection_id = p.projection_id
           and p.is_super_projection = true
    where schema_name = '${schema}'
     group by schema_name,
              table_name,
              sc.projection_id
),
tables_with_rows as (
  select schema_name, table_name, max(rows) as rows
    from num_rows
   group by schema_name,
         table_name
)
select t.table_schema, t.table_name, case when rows is null then 0 else rows end
  from tables_with_rows twr
       right outer join v_catalog.tables t
                on t.table_schema = twr.schema_name
               and t.table_name = twr.table_name 
  where t.table_schema = '${schema}'

`
    }
   
    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

    async getRowCounts(target) {
      const results = await this.dbi.executeSQL(VerticaCompare.SQL_SCHEMA_TABLE_ROWS(target));
      return results.rows
    }    

    buildColumnLists(schemaMetadata,rules) {
		
	  const compareInfo = {}
	  schemaMetadata.forEach((tableMetadata) => {
        compareInfo[tableMetadata.TABLE_NAME] = tableMetadata.DATA_TYPE_ARRAY.map((dataType,idx) => {
		  const columnName = tableMetadata.COLUMN_NAME_ARRAY[idx]
		  switch (dataType) {
			case (this.dbi.DATA_TYPES.XML_TYPE):
			  return rules.XML_COMPARISON_RULE === 'STRIP_XML_DECLARATION' ? `YADAMU.STRIP_XML_DECLARATION("${columnName}")` : `"${columnName}"`
		    case this.dbi.DATA_TYPES.JSON_TYPE:
              return `case when "${columnName}" is NULL then NULL when SUBSTR("${columnName}",1,1) = '{' then MAPTOSTRING(MAPJSONEXTRACTOR("${columnName}")) when SUBSTR("${columnName}",1,1) = '[' then MAPTOSTRING(mapDelimitedExtractor(substr("${columnName}",2,length("${columnName}")-2) using parameters delimiter=',')) else "${columnName}" end`
            case this.dbi.DATA_TYPES.CHAR_TYPE :
            case this.dbi.DATA_TYPES.VARCHAR_TYPE :
            case this.dbi.DATA_TYPES.CLOB_TYPE :
		      return rules.EMPTY_STRING_IS_NULL ? `case when "${columnName}" = '' then NULL else "${columnName}" end` : `"${columnName}"`
		    case this.dbi.DATA_TYPES.DOUBLE_TYPE :
		      const columnClause = rules.DOUBLE_PRECISION !== null ? `round("${columnName}",${rules.DOUBLE_PRECISION})` : `"${columnName}"`
			  return rules.INFINITY_IS_NULL ? `case when "${columnName}" = 'Inf' then NULL when "${columnName}" = '-Inf' then NULL when "${columnName}" <> "${columnName}" then NULL else ${columnClause} end` : columnClause
		    case this.dbi.DATA_TYPES.NUMERIC_TYPE :
		    case this.dbi.DATA_TYPES.DECIMAL_TYPE:
              return ((rules.NUMERIC_SCALE !== null) && (rules.NUMERIC_SCALE < tableMetadata.SIZE_CONSTRAINT_ARRAY[idx][0])) ? `round("${columnName}",${rules.NUMERIC_SCALE})` : `"${columnName}"`
  		    case this.dbi.DATA_TYPES.GEOGRAPHY_TYPE :
            case this.dbi.DATA_TYPES.GEOMETRY_TYPE :
		      return `case when YADAMU.invalidGeoHash("${columnName}") then ST_AsText("${columnName}") else ${rules.SPATIAL_PRECISION < 20 ? `ST_GeoHash("${columnName}" USING PARAMETERS NUMCHARS=${rules.SPATIAL_PRECISION})` : `ST_GeoHash("${columnName}")`} end`
            default:
              return `"${columnName}"`
          }
	    }).join(',')
      })
	  return compareInfo
    }

    async compareSchemas(source,target,rules) {
		
      const report = {
        successful : []
       ,failed     : []
      }
	  
	  const results = await this.dbi.executeSQL(this.dbi.StatementLibrary.SQL_SCHEMA_INFORMATION(source))
	  let compareInfo = this.dbi.buildSchemaInfo(results.rows)
	 	  
      const compareOperations = this.buildColumnLists(compareInfo,rules) 
	  
      const compareResults = await Promise.all(Object.keys(compareOperations).map(async (TABLE_NAME) => {
        const sqlStatement =
`select 
  '${TABLE_NAME}' "TABLE_NAME",
  (select count(*) from "${source}"."${TABLE_NAME}") SOURCE_ROWS,
  (select count(*) from "${target}"."${TABLE_NAME}") TARGET_ROWS,
  (select count(*) from (select ${compareOperations[TABLE_NAME]} from "${source}"."${TABLE_NAME}" except select ${compareOperations[TABLE_NAME]} from "${target}"."${TABLE_NAME}") T1) EXTRA_ROWS,
  (select count(*) from (select ${compareOperations[TABLE_NAME]} from "${target}"."${TABLE_NAME}" except select ${compareOperations[TABLE_NAME]} from "${source}"."${TABLE_NAME}") T2) MISSING_ROWS`;
        return this.dbi.executeSQL(sqlStatement);
     }))
	 
	 
     compareResults.forEach((results,idx) => {
        const compareResult =  results.rows[0]
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

export { VerticaCompare as default }

const _SQL_SUCCESS =
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, TARGET_ROW_COUNT
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT = TARGET_ROW_COUNT
   and MISSING_ROWS = 0
   and EXTRA_ROWS = 0
   and NOTES is NULL
order by TABLE_NAME`;

const _SQL_FAILED = 
`select SOURCE_SCHEMA, TARGET_SCHEMA, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, MISSING_ROWS, EXTRA_ROWS, NOTES
  from SCHEMA_COMPARE_RESULTS 
 where SOURCE_ROW_COUNT <> TARGET_ROW_COUNT
    or MISSING_ROWS <> 0
    or EXTRA_ROWS <> 0
    or NOTES is NOT NULL
 order by TABLE_NAME`;

const _SQL_SCHEMA_TABLE_ROWS = `select TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = ?`;

const _SQL_COMPARE_SCHEMAS =  `CALL COMPARE_SCHEMAS(?,?,?);`;

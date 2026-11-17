
import YadamuCompare   from '../base/yadamuCompare.js'

class DB2Compare extends YadamuCompare {

    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

	async getRowCounts(target) {

	  let results = await this.dbi.executeSQL(`select TABNAME from SYSCAT.TABLES where TABSCHEMA = '${target}'`)
	  results = results.map((result) => { return Object.values(result)})
	  if (results.length > 0 ) {
	    const sqlCountRows = results.map((row) => { return `select cast('${target}' as VARCHAR(128)), cast('${row[0]}' as VARCHAR(128)), count(*) from "${target}"."${row[0]}"`}).join('\nunion all \n')
	    results = await this.dbi.executeSQL(sqlCountRows)
	    results = results.map((result) => { return Object.values(result)})
	  }
	  return results
    }    


	columnExpression(columnName,dataType,sizeConstraint,rules,qualifier) {

      const column = `${qualifier}"${columnName}"`
	  switch (dataType) {
	    case this.dbi.DATA_TYPES.XML_TYPE:
		  return `HASH(xmlserialize(${column} as BLOB(2G) EXCLUDING XMLDECLARATION),2)`
		case this.dbi.DATA_TYPES.JSON_TYPE:
		case this.dbi.DATA_TYPES.BLOB_TYPE:
		  return `HASH(${column},2)`
		case this.dbi.DATA_TYPES.CLOB_TYPE:
		case this.dbi.DATA_TYPES.NCLOB_TYPE:
		  return rules.EMPTY_STRING_IS_NULL ? `HASH(case when length(${column}) = 0 then NULL else ${column} end,2)` : `HASH(${column},2)`
		case this.dbi.DATA_TYPES.VARCHAR_TYPE:
		case this.dbi.DATA_TYPES.NVARCHAR_TYPE:
		  return rules.EMPTY_STRING_IS_NULL ? `case when ${column} = '' then NULL else ${column} end` : column
		case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
		  return rules.TIMESTAMP_PRECISION < sizeConstraint[0] ? `substr(to_char(${column},'YYYY-MM-DD HH24:MI:SS.FF12'),1,${20 + rules.TIMESTAMP_PRECISION})` : column
		default:
		  return column
	  }
	}

	buildColumnLists(schemaInfo,rules) {

	  return schemaInfo.map((tableInfo) => {
		const columnNames = JSON.parse(tableInfo.COLUMN_NAME_ARRAY)
		const dataTypes = JSON.parse(tableInfo.DATA_TYPE_ARRAY)
		const sizeConstraints = JSON.parse(tableInfo.SIZE_CONSTRAINT_ARRAY)
		const extendedTypes = tableInfo.EXTENDED_TYPE_ARRAY ? JSON.parse(tableInfo.EXTENDED_TYPE_ARRAY) : []
		const expressions = (qualifier) => { return columnNames.map((columnName,idx) => { return this.columnExpression(columnName,dataTypes[idx],sizeConstraints[idx],rules,qualifier) })}
		return {
		  TABLE_NAME   : tableInfo.TABLE_NAME
		, COLUMN_LIST  : expressions('').map((expression,idx) => { return `${expression} "${columnNames[idx]}"` }).join(',')
		, COLUMN_NAMES : columnNames
		, DATA_TYPES   : dataTypes
		, EXPRESSIONS  : expressions
		  // BSON columns (JSON_TYPE), and character or binary columns with an IS_JSON check
		, JSON_COLUMNS : columnNames.map((columnName,idx) => { return ((dataTypes[idx] === this.dbi.DATA_TYPES.JSON_TYPE) || (extendedTypes[idx] === 'JSON')) ? idx : -1 }).filter((idx) => { return idx > -1 })
		}
	  })
    }

	/*
	**
	** Db2 SQL cannot sort the keys of a JSON object, so pass 1 compares JSON as a hash. When ORDERED_JSON applies and a table with a JSON column still
	** differs, pass 2 fetches only the rows pass 1 counted, matched to the EXCEPT result column by column (IS NOT DISTINCT FROM, so NULLs match),
	** with each JSON column as text, and YADAMU recounts them with the keys sorted.
	**
	*/

	async fetchDifferingRows(table,from,to) {

	  const exceptList = table.EXPRESSIONS('').map((expression,idx) => { return `${expression} "C${idx}"` }).join(',')
	  const rowExpressions = table.EXPRESSIONS('S.')
	  const selectList = rowExpressions.map((expression,idx) => {
		const column = `S."${table.COLUMN_NAMES[idx]}"`
		// Every column is aliased: the driver names an unaliased expression by number, and Object.values() would put it first
		return `${table.JSON_COLUMNS.includes(idx) ? (table.DATA_TYPES[idx] === this.dbi.DATA_TYPES.JSON_TYPE ? `SYSTOOLS.BSON2JSON(${column})` : column) : expression} "C${idx}"`
	  }).join(',')
	  const match = rowExpressions.map((expression,idx) => { return `D."C${idx}" is not distinct from ${expression}` }).join(' and ')
	  const sqlStatement =
`with D as (select ${exceptList} from "${from}"."${table.TABLE_NAME}" except select ${exceptList} from "${to}"."${table.TABLE_NAME}")
select ${selectList} from "${from}"."${table.TABLE_NAME}" S where exists (select 1 from D where ${match})`
	  const results = await this.dbi.executeSQL(sqlStatement)
	  return results.map((row) => { return table.COLUMN_NAMES.map((columnName,idx) => { return row[`C${idx}`] }) })
	}

	async applyOrderedJSON(table,source,target,compareResult) {
	  try {
		const extraRows = await this.fetchDifferingRows(table,source,target)
		const missingRows = await this.fetchDifferingRows(table,target,source)
		const recount = this.recountOrderedJSON(extraRows,missingRows,table.JSON_COLUMNS)
		compareResult[3] = recount.extraRows
		compareResult[4] = recount.missingRows
	  } catch (e) {
		compareResult[5] = `Ordered JSON recount failed: ${e.message}`
	  }
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
      
	  const compareResults = [] 
      for (const table of columnLists) {
        const sqlStatement =
`select 
  '${table.TABLE_NAME}' "TABLE_NAME",
  (select count(*) from "${source}"."${table.TABLE_NAME}") SOURCE_ROWS,
  (select count(*) from "${target}"."${table.TABLE_NAME}") TARGET_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from "${source}"."${table.TABLE_NAME}" except select ${table.COLUMN_LIST} from "${target}"."${table.TABLE_NAME}") T1) EXTRA_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from "${target}"."${table.TABLE_NAME}" except select ${table.COLUMN_LIST} from "${source}"."${table.TABLE_NAME}") T2) MISSING_ROWS
  from sysibm.sysdummy1`;
  
        let results
        try {
          results = await this.dbi.executeSQL(sqlStatement);
		  results = Object.values(results[0])
		  const differs = (parseInt(results[3]) > 0) || (parseInt(results[4]) > 0)
		  if (differs && (rules.ORDERED_JSON === true) && (table.JSON_COLUMNS.length > 0)) {
		    await this.applyOrderedJSON(table,source,target,results)
		  }
          compareResults.push(results)
		} catch (e) {
		  // this.LOGGER.handleException([this.DATABASE_VENDOR,'QA','COMPARE_SCHEMA'],e)  
		  compareResults.push([table.TABLE_NAME,-1,-1,-1,-1,e.message])
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
export { DB2Compare as default }

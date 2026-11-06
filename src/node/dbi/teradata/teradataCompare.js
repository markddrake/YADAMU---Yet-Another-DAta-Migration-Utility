import YadamuCompare   from '../base/yadamuCompare.js'

class TeradataCompare extends YadamuCompare {

    /*
    **
    ** Teradata cannot apply a set operator to a LOB, JSON, XML or ST_GEOMETRY column, and HASHROW does not accept LOBs. Teradata 17.10 has no MD5 or SHA function,
    ** so each of these values is compared as its length plus a HASHROW of its first 2 and last chunks. HASHROW ignores case and trailing spaces, so a character
    ** chunk is hashed as its CHAR2HEXINT representation. A difference inside a value longer than 3 chunks that leaves its length unchanged is not detected.
    **
    ** Character columns are NOT CASESPECIFIC by default, which makes EXCEPT ignore case, so they are compared as CASESPECIFIC.
    **
    ** Teradata stores JSON text as it was written, and some databases reformat it or reorder its keys (ORDERED_JSON). A table with a JSON column is
    ** compared here instead: each JSON value is parsed and serialized again, with its keys sorted when ORDERED_JSON applies. When INFINITY_IS_NULL
    ** applies, a table whose FLOAT columns hold Infinity or NaN is also compared here: any test for them inside EXCEPT can raise "Numeric processor
    ** operand error".
    **
    */

    static get CHAR_CHUNK_SIZE()           { return 4000 }
    static get BYTE_CHUNK_SIZE()           { return 32000 }

    constructor(dbi,configuration) {
	  super(dbi,configuration)
	}

    async getRowCounts(target) {

	  let results = await this.dbi.executeSQL(`select TRIM(TABLENAME) from DBC.TABLESV where DATABASENAME = '${target}' and TABLEKIND in ('T','O')`)
	  if (results.length > 0) {
	    const sqlCountRows = results.map((row) => { return `select cast('${target}' as VARCHAR(128)), cast('${row[0]}' as VARCHAR(128)), count(*) from "${target}"."${row[0]}"`}).join('\nunion all \n')
	    results = await this.dbi.executeSQL(sqlCountRows)
	  }
	  return results
	}

	hashCharacterLOB(expression,rules) {
	  const chunkSize = TeradataCompare.CHAR_CHUNK_SIZE
	  const chunk = (start) => { return `CHAR2HEXINT(CAST(SUBSTR(${expression},${start},${chunkSize}) AS VARCHAR(${chunkSize})))` }
	  const length = rules.emptyStringIsNull ? `NULLIF(CHARACTER_LENGTH(${expression}),0)` : `CHARACTER_LENGTH(${expression})`
	  return [length,`HASHROW(${chunk(1)},${chunk(chunkSize + 1)},${chunk(`GREATEST(CHARACTER_LENGTH(${expression}) - ${chunkSize - 1},1)`)})`]
	}

	hashBinaryLOB(expression) {
	  const chunkSize = TeradataCompare.BYTE_CHUNK_SIZE
	  const chunk = (start) => { return `CAST(SUBSTR(${expression},${start},${chunkSize}) AS VARBYTE(${chunkSize}))` }
	  return [`BYTES(${expression})`,`HASHROW(${chunk(1)},${chunk(chunkSize + 1)},${chunk(`GREATEST(BYTES(${expression}) - ${chunkSize - 1},1)`)})`]
	}

	truncateTemporal(columnName,dataType,precision,rules) {
	  // CAST to a lower fractional precision raises "DateTime field overflow", so the text form is truncated. The time zone offset follows the fraction.
	  const timeLength = (p) => { return 8 + (p > 0 ? p + 1 : 0) }
	  const dateLength = 11
	  const text = `CAST("${columnName}" AS VARCHAR(64))`
	  switch (dataType) {
		case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
		  return `SUBSTR(${text},1,${dateLength + timeLength(rules.timestampPrecision)})`
		case this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE:
		  return `SUBSTR(${text},1,${dateLength + timeLength(rules.timestampPrecision)}) || SUBSTR(${text},${dateLength + timeLength(precision) + 1})`
		case this.dbi.DATA_TYPES.TIME_TYPE:
		  return `SUBSTR(${text},1,${timeLength(rules.timestampPrecision)})`
		case this.dbi.DATA_TYPES.TIME_TZ_TYPE:
		  return `SUBSTR(${text},1,${timeLength(rules.timestampPrecision)}) || SUBSTR(${text},${timeLength(precision) + 1})`
	  }
	}

	buildColumnLists(schemaInfo,rules) {

	  // XML, JSON and ST_GEOMETRY values are converted to a CLOB once, in a derived table: XMLSERIALIZE evaluated for each chunk runs out of memory.
	  const roundSpatial = (rules.spatialPrecision >= 0) && (rules.spatialPrecision < 18)
	  return schemaInfo.map((tableInfo) => {
		const dataTypes = JSON.parse(tableInfo.DATA_TYPE_ARRAY)
		const sizeConstraints = JSON.parse(tableInfo.SIZE_CONSTRAINT_ARRAY)
		const columnNames = JSON.parse(tableInfo.COLUMN_NAME_ARRAY)
		const isSpatial = (dataType) => { return (dataType === this.dbi.DATA_TYPES.SPATIAL_TYPE) || (dataType === 'ST_GEOMETRY') }
		// A LATIN CLOB or JSON column is translated to UNICODE, so that its CHAR2HEXINT hash matches the same text in a UNICODE column
		const sourceList = (latinColumns = new Set()) => { return columnNames.map((columnName,idx) => {
		  switch (true) {
		    case latinColumns.has(columnName) && (dataTypes[idx] === this.dbi.DATA_TYPES.JSON_TYPE):
			  return `TRANSLATE(CAST("${columnName}" AS CLOB) USING LATIN_TO_UNICODE) "${columnName}"`
		    case latinColumns.has(columnName):
			  return `TRANSLATE("${columnName}" USING LATIN_TO_UNICODE) "${columnName}"`
		    case (dataTypes[idx] === this.dbi.DATA_TYPES.XML_TYPE):
			  return `XMLSERIALIZE(CONTENT "${columnName}" AS CLOB CHARACTER SET UNICODE) "${columnName}"`
		    case (dataTypes[idx] === this.dbi.DATA_TYPES.JSON_TYPE):
			  return `CAST("${columnName}" AS CLOB) "${columnName}"`
		    case isSpatial(dataTypes[idx]):
			  return `"${columnName}".ST_AsText() "${columnName}"`
		    default:
			  return `"${columnName}"`
		  }
		}).join(',') }
		const expressions = (clientSide) => {
		  return columnNames.flatMap((columnName,idx) => {
		    const dataType = dataTypes[idx]
		    switch (true) {
			  case (dataType === this.dbi.DATA_TYPES.CHAR_TYPE):
			  case (dataType === this.dbi.DATA_TYPES.VARCHAR_TYPE):
			    return rules.emptyStringIsNull ? `NULLIF("${columnName}",'') (CASESPECIFIC)` : `"${columnName}" (CASESPECIFIC)`
			  case (dataType === this.dbi.DATA_TYPES.JSON_TYPE) && clientSide:
			    // Marked so that its position in the select list can be found
			    return [{json: `"${columnName}"`}]
			  case isSpatial(dataType) && clientSide && roundSpatial:
			    return [{spatial: `"${columnName}"`}]
			  case (dataType === this.dbi.DATA_TYPES.CLOB_TYPE):
			  case (dataType === this.dbi.DATA_TYPES.JSON_TYPE):
			  case (dataType === this.dbi.DATA_TYPES.XML_TYPE):
			  case isSpatial(dataType):
			    return this.hashCharacterLOB(`"${columnName}"`,rules)
			  case (dataType === this.dbi.DATA_TYPES.NUMERIC_TYPE):
			  case (dataType === this.dbi.DATA_TYPES.DECIMAL_TYPE):
			    // An unbounded NUMBER, or a scale above NUMERIC_SCALE, is rounded to NUMERIC_SCALE
			    return ((rules.numericScale !== null) && ((sizeConstraints[idx].length < 2) || (sizeConstraints[idx][1] > rules.numericScale))) ? `ROUND("${columnName}",${rules.numericScale})` : `"${columnName}"`
			  case (dataType === this.dbi.DATA_TYPES.BLOB_TYPE):
			    return this.hashBinaryLOB(`"${columnName}"`)
			  case (dataType === this.dbi.DATA_TYPES.TIMESTAMP_TYPE):
			  case (dataType === this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE):
			  case (dataType === this.dbi.DATA_TYPES.TIME_TYPE):
			  case (dataType === this.dbi.DATA_TYPES.TIME_TZ_TYPE):
			    return ((rules.timestampPrecision !== null) && (rules.timestampPrecision < sizeConstraints[idx][0])) ? this.truncateTemporal(columnName,dataType,sizeConstraints[idx][0],rules) : `"${columnName}"`
			  default:
			    return `"${columnName}"`
		    }
		  })
		}
		// Every expression in a derived table needs a name
		const columnList = (expressions) => { return expressions.map((expression,idx) => { return `${expression.json || expression.spatial || expression} "C${idx+1}"` }).join(',') }
		const clientExpressions = expressions(true)
		return {
		  TABLE_NAME         : tableInfo.TABLE_NAME
		, SOURCE_LIST        : sourceList
		, COLUMN_LIST        : columnList(expressions(false))
		, CLIENT_COMPARE     : dataTypes.includes(this.dbi.DATA_TYPES.JSON_TYPE) || (roundSpatial && dataTypes.some(isSpatial))
		, FLOAT_COLUMNS      : rules.infinityIsNull ? columnNames.filter((columnName,idx) => { return dataTypes[idx] === this.dbi.DATA_TYPES.DOUBLE_TYPE }) : []
		, CLIENT_COLUMN_LIST : columnList(clientExpressions)
		, JSON_INDEXES       : clientExpressions.flatMap((expression,idx) => { return expression.json ? [idx] : [] })
		, SPATIAL_INDEXES    : clientExpressions.flatMap((expression,idx) => { return expression.spatial ? [idx] : [] })
		}
	  })
    }

	roundCoordinates(wkt,spatialPrecision) {
	  // SPATIAL_PRECISION is the number of decimal places a coordinate keeps
	  return wkt.replace(/-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?/g,(coordinate) => { return String(Number(Number(coordinate).toFixed(spatialPrecision))) })
	}

	canonicalJSON(text,orderedJSON) {
	  const sortKeys = (value) => {
	    return Array.isArray(value) ? value.map(sortKeys) : ((value !== null) && (typeof value === 'object')) ? Object.fromEntries(Object.keys(value).sort().map((key) => { return [key,sortKeys(value[key])] })) : value
	  }
	  try {
	    const value = JSON.parse(text)
	    return JSON.stringify(orderedJSON ? sortKeys(value) : value)
	  } catch (e) {
	    return text
	  }
	}

	async hasNonFiniteValues(schema,table) {
	  // A plain query can test for Infinity and NaN. Only EXCEPT fails on them. The largest finite FLOAT, 1.7976931348623157E308, is not Infinity (as a literal it raises "Precision error in FLOAT type constant").
	  const predicate = table.FLOAT_COLUMNS.map((columnName) => { return `"${columnName}" > CAST('1.7976931348623157E308' AS FLOAT) OR "${columnName}" < CAST('-1.7976931348623157E308' AS FLOAT) OR "${columnName}" <> "${columnName}"` }).join(' OR ')
	  const results = await this.dbi.executeSQL(`select count(*) from "${schema}"."${table.TABLE_NAME}" where ${predicate}`)
	  return parseInt(Object.values(results[0])[0]) > 0
	}

	matchSpatialRows(table,result,rowKey,tolerance) {

	  // Rounding a coordinate that lies on a rounding boundary, such as a binary fraction like 11.4998046875, can go either way. Rows left unmatched
	  // after rounding are matched when their other values and the shape of each geometry are equal, and every coordinate is within the tolerance.
	  const coordinatePattern = /-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?/g
	  const describe = (row) => {
		const values = Object.values(row)
		const shape = values.map((value,idx) => { return (table.SPATIAL_INDEXES.includes(idx) && (typeof value === 'string')) ? value.replace(coordinatePattern,'#') : null })
		const coordinates = values.flatMap((value,idx) => { return (table.SPATIAL_INDEXES.includes(idx) && (typeof value === 'string')) ? value.match(coordinatePattern).map(Number) : [] })
		const others = values.map((value,idx) => { return table.SPATIAL_INDEXES.includes(idx) ? shape[idx] : value })
		return {key: rowKey(others,0), coordinates, row}
	  }
	  const candidates = new Map()
	  result.target.map(describe).forEach((target) => { candidates.has(target.key) ? candidates.get(target.key).push(target) : candidates.set(target.key,[target]) })
	  const unmatchedSource = []
	  result.source.map(describe).forEach((source) => {
		const targets = candidates.get(source.key) || []
		const idx = targets.findIndex((target) => { return source.coordinates.every((coordinate,i) => { return Math.abs(coordinate - target.coordinates[i]) <= tolerance }) })
		idx < 0 ? unmatchedSource.push(source.row) : targets.splice(idx,1)
	  })
	  return {source: unmatchedSource, target: Array.from(candidates.values()).flat().map((target) => { return target.row })}
	}

	async getLatinColumns(schema) {
	  // The LATIN CLOB and JSON columns of each table
	  const latinColumns = new Map()
	  const results = await this.dbi.executeSQL(`select TRIM(TableName), TRIM(ColumnName) from DBC.ColumnsV where DatabaseName = '${schema}' and CharType = 1 and ColumnType in ('CO','JN')`)
	  results.forEach(([tableName,columnName]) => { latinColumns.has(tableName) ? latinColumns.get(tableName).add(columnName) : latinColumns.set(tableName,new Set([columnName])) })
	  return latinColumns
	}

	async compareRowsClientSide(source,target,table,rules) {

	  // Each row becomes a string: the JSON columns are canonicalized, spatial coordinates are rounded to SPATIAL_PRECISION decimal places, Infinity and
	  // NaN become NULL when INFINITY_IS_NULL applies, and the other values (lengths, hashes and values) are serialized as they are.
	  const rowKey = (row,spatialPrecision) => {
		return JSON.stringify(Object.values(row).map((value,idx) => {
		  switch (true) {
		    case table.JSON_INDEXES.includes(idx) && (typeof value === 'string'):
			  return this.canonicalJSON(value,rules.orderedJSON)
			case table.SPATIAL_INDEXES.includes(idx) && (typeof value === 'string'):
			  return this.roundCoordinates(value,spatialPrecision)
			case (typeof value === 'number') && !isFinite(value):
			  return rules.infinityIsNull ? null : value.toString()
			case (value instanceof Uint8Array):
			  return Buffer.from(value).toString('hex')
			case (typeof value === 'bigint'):
			  return value.toString()
			default:
			  return value
		  }
		}))
	  }

	  // Returns the rows of each side that have no match in the other
	  const unmatched = (sourceRows,targetRows,spatialPrecision) => {
		const keys = new Map()
		sourceRows.forEach((row) => { const key = rowKey(row,spatialPrecision); keys.has(key) ? keys.get(key).push({side: 1, row}) : keys.set(key,[{side: 1, row}]) })
		targetRows.forEach((row) => { const key = rowKey(row,spatialPrecision); keys.has(key) ? keys.get(key).push({side: -1, row}) : keys.set(key,[{side: -1, row}]) })
		const result = {source: [], target: []}
		keys.forEach((entries) => {
		  const balance = entries.reduce((total,entry) => { return total + entry.side },0)
		  const side = balance > 0 ? 1 : -1
		  entries.filter((entry) => { return entry.side === side }).slice(0,Math.abs(balance)).forEach((entry) => { (side > 0 ? result.source : result.target).push(entry.row) })
		})
		return result
	  }

	  const sql = (schema,latinColumns) => { return `select ${table.CLIENT_COLUMN_LIST} from (select ${table.SOURCE_LIST(latinColumns.get(table.TABLE_NAME))} from "${schema}"."${table.TABLE_NAME}") T0` }
	  const sourceRows = await this.dbi.executeSQL(sql(source,rules.latinColumns.source))
	  const targetRows = await this.dbi.executeSQL(sql(target,rules.latinColumns.target))
	  let result = unmatched(sourceRows,targetRows,rules.spatialPrecision)
	  if ((table.SPATIAL_INDEXES.length > 0) && (result.source.length > 0) && (result.target.length > 0)) {
		result = this.matchSpatialRows(table,result,rowKey,10 ** -rules.spatialPrecision)
	  }
	  return [table.TABLE_NAME,sourceRows.length,targetRows.length,result.source.length,result.target.length]
	}

    async compareSchemas(source,target,rules) {

      const report = {
        successful : []
       ,failed     : []
      }

	  // A DDL_ONLY operation copies no data, so there is nothing to compare.
	  if (this.configuration.parameters?.MODE === 'DDL_ONLY') {
	    return report
	  }

	  this.dbi.parameters.FROM_USER = source;
      const schemaInfo = (await this.dbi.getSchemaMetadata()).filter((tableInfo) => {
         return ((this.dbi.TABLE_FILTER.length === 0) || (this.dbi.TABLE_FILTER.includes(tableInfo.TABLE_NAME)))
	   })

	  const compareRules = this.formatCompareRules(rules)
	  compareRules.timestampPrecision = rules.TIMESTAMP_PRECISION ?? null
	  compareRules.latinColumns = { source: await this.getLatinColumns(source), target: await this.getLatinColumns(target) }
	  const columnLists = this.buildColumnLists(schemaInfo,compareRules)

	  const compareResults = []
      for (const table of columnLists) {
        const sourceList = table.SOURCE_LIST(compareRules.latinColumns.source.get(table.TABLE_NAME))
        const targetList = table.SOURCE_LIST(compareRules.latinColumns.target.get(table.TABLE_NAME))
        const sqlStatement =
`select
  '${table.TABLE_NAME}' "TABLE_NAME",
  (select count(*) from "${source}"."${table.TABLE_NAME}") SOURCE_ROWS,
  (select count(*) from "${target}"."${table.TABLE_NAME}") TARGET_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from (select ${sourceList} from "${source}"."${table.TABLE_NAME}") S0 except all select ${table.COLUMN_LIST} from (select ${targetList} from "${target}"."${table.TABLE_NAME}") T0) T1) EXTRA_ROWS,
  (select count(*) from (select ${table.COLUMN_LIST} from (select ${targetList} from "${target}"."${table.TABLE_NAME}") T0 except all select ${table.COLUMN_LIST} from (select ${sourceList} from "${source}"."${table.TABLE_NAME}") S0) T2) MISSING_ROWS`;

        let results
        try {
		  if (table.CLIENT_COMPARE || ((table.FLOAT_COLUMNS.length > 0) && (await this.hasNonFiniteValues(source,table) || await this.hasNonFiniteValues(target,table)))) {
		    compareResults.push(await this.compareRowsClientSide(source,target,table,compareRules))
		    continue
		  }
          results = await this.dbi.executeSQL(sqlStatement);
		  results = Object.values(results[0])
          compareResults.push(results)
		} catch (e) {
		  try {
		    // EXCEPT over many XML values can exhaust Teradata's XML memory (Error 9134). Comparing them here only needs one row at a time.
		    if (!e.message.includes('[Error 9134]')) throw e
		    compareResults.push(await this.compareRowsClientSide(source,target,table,compareRules))
		  } catch (e) {
		    compareResults.push([table.TABLE_NAME,-1,-1,-1,-1,e.message])
		  }
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
export { TeradataCompare as default }

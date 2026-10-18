import YadamuCompare   from '../base/yadamuCompare.js'

import SqliteDataTypes from './sqliteDataTypes.js'

/*
**
** SQLite has no stored procedures, so the comparison that other drivers delegate to a COMPARE_SCHEMAS
** procedure installed in the database is generated here as one SQL statement per table and run by the
** client. Both schemas must be visible to one connection, which the schema per file form of the
** connection ({ "directory" : ... }) provides by attaching each schema's file under its own name. A
** single file connection keeps every table in main, so a source and its clone cannot coexist in it.
**
** Each row is compared through a canonical form of every column, computed in SQL on both sides, and the
** two sets are compared with EXCEPT, which is how the other drivers count missing and extra rows. The
** canonical forms absorb differences in representation that a round trip through another database
** introduces without changing a value:
**
**   numeric text   (NUMERIC_TEXT etc.)   trailing fractional zeros: 24000.00 and 24000 are one value.
**   temporal text  (TIMESTAMP_TEXT etc.) separator, zone suffix and fractional zeros, normalized with
**                                        strftime() and truncated to the TIMESTAMP_PRECISION rule.
**   JSON and GeoJSON                     key order and number format: the document is reduced to its
**                                        sorted json_tree() paths, numbers rounded to DOUBLE_PRECISION
**                                        (JSON) or SPATIAL_PRECISION (GEOMETRY) significant digits.
**   REAL                                 rounded to DOUBLE_PRECISION significant digits, when set.
**   other text                           '' is NULL when EMPTY_STRING_IS_NULL is set.
**
** Columns are classified by their declared type, which is how SqliteDBI records what a TEXT value holds
** (see SqliteDataTypes, SERIALIZED STORAGE).
**
*/

class SqliteCompare extends YadamuCompare {

  constructor(dbi,configuration) {
    super(dbi,configuration)
  }

  static #TEMPORAL_TYPES = ['DATE','DATETIME','TIMESTAMP','DATE_TEXT','DATETIME_TEXT','TIMESTAMP_TEXT','TIMESTAMP_TZ_TEXT','TIMESTAMP_LTZ_TEXT','MSSQL_DATETIME_TEXT','MSSQL_SMALLDATETIME_TEXT','TIME_TEXT','TIME_TZ_TEXT']
  static #NUMERIC_TYPES  = ['NUMERIC_TEXT','DECIMAL_TEXT','NUMBER_TEXT','UNBOUNDED_NUMERIC_TEXT','MSSQL_MONEY_TEXT','MSSQL_SMALLMONEY_TEXT','MONGO_DECIMAL128_TEXT','IBMDB2_DECFLOAT_TEXT','IBMDB2_DECFLOAT16_TEXT','IBMDB2_DECFLOAT34_TEXT']
  // An Oracle BFILE is carried as a JSON locator ({"alias":...,"path":...}) and a MySQL SET as a JSON array, both of which their databases reformat.
  static #JSON_TYPES     = ['JSON','JSON_TEXT','PGSQL_BINARY_JSON_TEXT','MONGO_OBJECT_TEXT','MONGO_ARRAY_TEXT','SNOWFLAKE_VARIANT_TEXT','ORACLE_BFILE_TEXT','MYSQL_SET_TEXT']
  static #SPATIAL_TYPES  = ['GEOMETRY']
  static #XML_TYPES      = ['XML','XML_TEXT']

  // A double carries at most 17 significant digits, so a precision rule of 17 or more is no rule at all.

  static #MAX_DOUBLE_DIGITS = 17

  static #JSON_DIGITS = 15

  quote(identifier) {
    return `"${identifier.replace(/"/g,'""')}"`
  }

  async attachedSchemas() {
    const databases = await this.dbi.executeSQL(`select name from pragma_database_list()`)
    return databases.map((database) => database.name)
  }

  async listTables(schema) {
    const tables = await this.dbi.executeSQL(this.dbi.STATEMENT_LIBRARY_CLASS.SQL_LIST_USER_TABLES(schema))
    return tables.map((table) => table.TABLE_NAME).sort()
  }

  async listColumns(schema,tableName) {
    return await this.dbi.executeSQL(`select name, type from pragma_table_info(?,?) order by cid`,[tableName,schema])
  }

  async getRowCounts(target) {
    const schema = this.dbi.MULTI_SCHEMA ? target : this.dbi.SCHEMA_NAME
    const tables = await this.listTables(schema)
    const rowCounts = []
    for (const tableName of tables) {
      const results = await this.dbi.executeSQL(`select count(*) "ROW_COUNT" from ${this.dbi.qualifiedTableName(tableName,schema)}`)
      rowCounts.push([target,tableName,results[0].ROW_COUNT])
    }
    return rowCounts
  }

  significantDigits(precision) {
    return ((precision === null) || (precision === undefined) || (Math.abs(precision) >= SqliteCompare.#MAX_DOUBLE_DIGITS)) ? null : Math.abs(precision)
  }

  /*
  **
  ** %.17g is exact for a double and, unlike quote(), renders 1 and 1.0 alike.
  **
  ** A precision rule rounds twice: first to NOISE_DIGITS, then to the rule. Rounding once puts two
  ** values that differ only by floating point noise on opposite sides of a tie: 37.231640625 is a
  ** double, 37.23164062499998 is the same coordinate after a round trip, and to 10 digits they
  ** round to ...62 and ...63. Rounding to 13 digits first makes both 37.231640625.
  **
  */

  static #NOISE_DIGITS = 13

  canonicalNumber(atom,digits) {
    switch (true) {
      case (digits === null):
        return `printf('%.${SqliteCompare.#MAX_DOUBLE_DIGITS}g',${atom})`
      case (digits < SqliteCompare.#NOISE_DIGITS):
        return `printf('%.${digits}g',cast(printf('%.${SqliteCompare.#NOISE_DIGITS}g',${atom}) as real))`
      default:
        return `printf('%.${digits}g',${atom})`
    }
  }

  canonicalJSON(column,digits) {
    // Containers are kept, so that {} and [] remain distinguishable from each other and from NULL.
    return `case when json_valid(${column}) then (select group_concat(j.fullkey || ':' || case when j.type in ('integer','real') then 'number=' || ${this.canonicalNumber('j.atom',digits)} when j.atom is null then j.type else j.type || '=' || quote(j.atom) end,'|' order by j.fullkey) from json_tree(${column}) j) else ${column} end`
  }

  canonicalTimestamp(column,precision) {
    const seconds = `strftime('%Y-%m-%dT%H:%M:%S',${column})`
    const fraction = `rtrim(substr(${column},instr(${column},'.') + 1,${precision}),'0Z+-:')`
    return `case when typeof(${column}) = 'text' and ${seconds} is not null then ${seconds} || case when instr(${column},'.') > 0 and length(${fraction}) > 0 then '.' || ${fraction} else '' end else ${column} end`
  }

  /*
  **
  ** SQL Server serializes an apostrophe in content as itself rather than &apos;, and writes its
  ** attributes in double quotes, so the two forms are the same document. STRIP_XML_DECLARATION is the
  ** rule the other drivers apply when the target drops the <?xml ...?> declaration.
  **
  */

  canonicalXML(column,rule) {
    // A target that drops the declaration keeps the line break after it and drops the one after the root element.
    const unescaped = `replace(${column},'&apos;','''')`
    return rule === 'STRIP_XML_DECLARATION'
         ? `trim(case when ${unescaped} like '<?xml%' then substr(${unescaped},instr(${unescaped},'?>') + 2) else ${unescaped} end,char(9,10,13,32))`
         : unescaped
  }

  /*
  **
  ** An interval is held as an ISO 8601 duration, and databases differ in whether they render the zero
  ** components: P0Y3M and P3M are one interval. Each pattern is anchored on the designator before
  ** it, so P10Y and Y10M are never mistaken for a zero.
  **
  */

  static #ZERO_DURATION_COMPONENTS = [['P0Y','P'],['P0M','P'],['Y0M','Y'],['P0D','P'],['Y0D','Y'],['M0D','M'],['T0H','T'],['H0M','H'],['T0M','T'],['T0S','T'],['H0S','H'],['M0S','M']]

  canonicalDuration(column) {
    const stripped = SqliteCompare.#ZERO_DURATION_COMPONENTS.reduce((expression,[zero,designator]) => `replace(${expression},'${zero}','${designator}')`,column)
    return `case when typeof(${column}) = 'text' and ${column} like 'P%' then rtrim(${stripped},'T') else ${column} end`
  }

  canonicalDecimal(column) {
    const trimmed = `rtrim(rtrim(${column},'0'),'.')`
    return `case when typeof(${column}) = 'text' and instr(${column},'.') > 0 and instr(lower(${column}),'e') = 0 then case when ${trimmed} like '-.%' then '-0' || substr(${trimmed},2) when ${trimmed} like '.%' then '0' || ${trimmed} else ${trimmed} end else ${column} end`
  }

  canonicalColumn(columnName,declaredType,rules) {

    const column = this.quote(columnName)
    const dataType = SqliteDataTypes.decomposeDataType(declaredType || '')
    const typeName = (dataType.type || '').toUpperCase()

    switch (true) {
      case SqliteCompare.#SPATIAL_TYPES.includes(typeName):
        return this.canonicalJSON(column,this.significantDigits(rules.spatialPrecision))
      case SqliteCompare.#JSON_TYPES.includes(typeName):
        return this.canonicalJSON(column,this.significantDigits(rules.doublePrecision) || SqliteCompare.#JSON_DIGITS)
      case SqliteCompare.#TEMPORAL_TYPES.includes(typeName):
        return this.canonicalTimestamp(column,rules.timestampPrecision)
      case SqliteCompare.#NUMERIC_TYPES.includes(typeName):
        return this.canonicalDecimal(column)
      case SqliteCompare.#XML_TYPES.includes(typeName):
        return this.canonicalXML(column,rules.xmlRule)
      case typeName.startsWith('DURATION_'):
        return this.canonicalDuration(column)
      case (typeName === 'UUID_TEXT'):
        // SQL Server renders a uniqueidentifier in upper case, Postgres a uuid in lower case.
        return `lower(${column})`
    }

    switch (this.dbi.DATA_TYPES.storageClass(typeName)) {
      case 'REAL':
        const digits = this.significantDigits(rules.doublePrecision)
        return digits === null ? column : `case when typeof(${column}) = 'real' then printf('%.${digits}g',${column}) else ${column} end`
      case 'TEXT':
        return rules.emptyStringIsNull ? `nullif(${column},'')` : column
      default:
        return column
    }

  }

  async compareTable(source,target,tableName,rules) {

    const sourceColumns = await this.listColumns(source,tableName)
    const targetColumns = await this.listColumns(target,tableName)
    const targetColumnNames = targetColumns.map((column) => column.name)
    const missingColumns = sourceColumns.filter((column) => !targetColumnNames.includes(column.name)).map((column) => column.name)

    const counts = await this.dbi.executeSQL(`select (select count(*) from ${this.quote(source)}.${this.quote(tableName)}) "SOURCE_ROWS", (select count(*) from ${this.quote(target)}.${this.quote(tableName)}) "TARGET_ROWS"`)
    const sourceRows = counts[0].SOURCE_ROWS
    const targetRows = counts[0].TARGET_ROWS

    if (missingColumns.length > 0) {
      return [source,target,tableName,sourceRows,targetRows,-1,-1,`Target is missing column(s): ${missingColumns.join(', ')}.`]
    }

    const selectList = sourceColumns.map((column) => `${this.canonicalColumn(column.name,column.type,rules)} ${this.quote(column.name)}`).join(', ')
    const sourceRowSet = `select ${selectList} from ${this.quote(source)}.${this.quote(tableName)}`
    const targetRowSet = `select ${selectList} from ${this.quote(target)}.${this.quote(tableName)}`

    const results = await this.dbi.executeSQL(`select (select count(*) from (${sourceRowSet} except ${targetRowSet})) "MISSING_ROWS", (select count(*) from (${targetRowSet} except ${sourceRowSet})) "EXTRA_ROWS"`)
    return [source,target,tableName,sourceRows,targetRows,results[0].MISSING_ROWS,results[0].EXTRA_ROWS,null]

  }

  async compareSchemas(source,target,rules) {

    const report = {
      successful : []
    , failed     : []
    }

    if (!this.dbi.MULTI_SCHEMA) {
      throw new Error(`${this.dbi.DATABASE_VENDOR} schemas "${source}" and "${target}" cannot be compared through a single file connection, which holds every table in main. Use a connection with "directory" so that each schema is a separate database file.`)
    }

    // A DDL_ONLY operation copies no data, so there are no rows to compare.
    if (this.configuration.parameters?.MODE === 'DDL_ONLY') {
      return report
    }

    const compareRules = this.formatCompareRules(rules)

    const attached = await this.attachedSchemas()
    const attachments = [source,target].filter((schema,idx,schemas) => !attached.includes(schema) && (schemas.indexOf(schema) === idx))
    for (const schema of attachments) {
      await this.dbi.attachSchema(schema)
    }

    try {
      const sourceTables = await this.listTables(source)
      const targetTables = await this.listTables(target)

      for (const tableName of sourceTables) {
        if (!targetTables.includes(tableName)) {
          const counts = await this.dbi.executeSQL(`select count(*) "ROW_COUNT" from ${this.quote(source)}.${this.quote(tableName)}`)
          report.failed.push([source,target,tableName,counts[0].ROW_COUNT,-1,-1,-1,'Missing Target'])
          continue
        }
        const result = await this.compareTable(source,target,tableName,compareRules)
        if ((result[3] === result[4]) && (result[5] === 0) && (result[6] === 0) && (result[7] === null)) {
          report.successful.push([source,target,tableName,result[4]])
        }
        else {
          report.failed.push(result)
        }
      }

      for (const tableName of targetTables.filter((tableName) => !sourceTables.includes(tableName))) {
        const counts = await this.dbi.executeSQL(`select count(*) "ROW_COUNT" from ${this.quote(target)}.${this.quote(tableName)}`)
        report.failed.push([source,target,tableName,-1,counts[0].ROW_COUNT,-1,-1,'Missing Source'])
      }
    } finally {
      for (const schema of attachments) {
        await this.dbi.executeSQL(`DETACH DATABASE ${this.quote(schema)}`)
      }
    }

    return report
  }
}

export { SqliteCompare as default }

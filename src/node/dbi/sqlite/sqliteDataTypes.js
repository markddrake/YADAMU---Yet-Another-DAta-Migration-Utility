import {
  YadamuDataTypes
, YadamuStorageOptions
}                           from '../base/yadamuDataTypes.js'

import SqliteConstants     from './sqliteConstants.js'

class SqliteStorageOptions extends YadamuStorageOptions {}

class SqliteDataTypes extends YadamuDataTypes {

  get DATABASE_KEY()      { return SqliteConstants.DATABASE_KEY }

  get DATABASE_VENDOR()   { return SqliteConstants.DATABASE_VENDOR }
  
  get STORAGE_OPTIONS()   { return new SqliteStorageOptions() }

  /*
  **
  ** SERIALIZED STORAGE
  **
  ** SQLite has five storage classes - NULL, INTEGER, REAL, TEXT, BLOB - and no exact decimal type
  ** at any version. NUMERIC is an AFFINITY, not a type: a NUMERIC column converts its value to
  ** INTEGER or REAL, so it cannot hold an exact decimal either. Text is the only faithful home,
  ** which is also what SQLite's own decimal extension operates on.
  **
  ** Types that have been serialized for that reason carry a name ending _TEXT - NUMERIC_TEXT,
  ** DECIMAL_TEXT, UNBOUNDED_NUMERIC_TEXT. The name is what allows the transformation to reverse:
  ** reading such a column back, the target selects a numeric type rather than a character type, so
  ** an Oracle NUMBER that arrived here as text leaves as a NUMBER. Declaring them REAL instead
  ** loses that, and is not merely lossy - MSSQL binds decimals as VarChar and REJECTS a number,
  ** and Postgres money goes out of range. See docs/Driver-Contracts.md section 4a.
  **
  ** The _TEXT suffix is forced by affinity, not chosen for style: see storageClass() below.
  **
  */

  /*
  **
  ** SQLite decides how to store a value from the DECLARED TYPE NAME, by substring, and the tests
  ** are ORDERED: INT beats CHAR/CLOB/TEXT, which beat BLOB, which beat REAL/FLOA/DOUB. Anything
  ** matching none of them gets NUMERIC affinity. This is the single definition of that rule.
  **
  */

  storageClass(declaredType) {

    const type = (declaredType || '').toUpperCase()

    if (type.includes('INT'))                                                     return 'INTEGER'
    if (type.includes('CHAR') || type.includes('CLOB') || type.includes('TEXT'))   return 'TEXT'
    if (type.includes('BLOB') || (type === ''))                                   return 'BLOB'
    if (type.includes('REAL') || type.includes('FLOA') || type.includes('DOUB'))   return 'REAL'
    return 'NUMERIC'

  }

  /*
  **
  ** Several canonical types can share one source type - Postgres reports both NUMERIC_TYPE and
  ** DECIMAL_TYPE as "numeric", because the base aliases DECIMAL_TYPE to NUMERIC_TYPE - and the
  ** statement generator then asks the target to reduce the candidates to one.
  **
  ** The base implementation walks an ordered list of concrete vendor types (CLOB, VARCHAR, CHAR,
  ** JSON ...). None of SQLite's names match it, so it falls through to a numeric type: NUMERIC and
  ** DECIMAL both became REAL, which is a double, silently rounding 12356789.123456789 to
  ** 12356789.12345679.
  **
  ** For SQLite the declared name is metadata and the affinity is the storage decision, so
  ** candidates that share an affinity are interchangeable and any of them is correct. Only a
  ** genuine disagreement about affinity needs the base ordering.
  **
  */

  /*
  **
  ** A source's JSON type is also how most vendors store the Postgres line and circle types, which
  ** have no equivalent of their own (PGSQL_LINE_EQ_TYPE and PGSQL_CIRCLE_TYPE map to "json" in
  ** MySQL, "JSON" in Oracle, and so on). SQLite maps both of those to GEOMETRY, so the candidates
  ** for every such JSON type disagree on affinity, and the base ordering, which puts SPATIAL_TYPE
  ** first, declared every JSON column GEOMETRY. The GEOMETRY transformer then parsed each JSON
  ** value as WKB or WKT ("Expected geometry type" on every row of a MySQL json column). A column
  ** of the source's JSON type holds JSON, so JSON wins whenever it is a candidate.
  **
  */

  coalesceTypeMappings(typeList) {

    const affinities = new Set(typeList.map((dataType) => this.storageClass(dataType)))

    if (affinities.size === 1) {
      return typeList[0]
    }

    if (typeList.includes(this.JSON_TYPE)) {
      return this.JSON_TYPE
    }

    return super.coalesceTypeMappings(typeList)

  }

  get UNBOUNDED_TYPES() { 
    this._UNBOUNDED_TYPES = this._UNBOUNDED_TYPES || Object.freeze([
      'TEXT'
    , 'BLOB'
    , 'GEOMETRY'
    ])
    return this._UNBOUNDED_TYPES;
  }
  
}
 
export { SqliteDataTypes as default }

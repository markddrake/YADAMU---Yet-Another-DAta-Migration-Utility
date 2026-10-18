import YadamuLibrary            from '../../lib/yadamuLibrary.js'
import YadamuSpatialLibrary     from '../../lib/yadamuSpatialLibrary.js'

import {
  RejectedColumnValue
}                               from '../../core/yadamuException.js'

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuOutputManager      from '../base/yadamuOutputManager.js'

/*
**
** Transformations are applied to each row, on a column by column basis, prior to the row being cached. They
** convert values supplied by the source into a format that can be bound directly by the target driver.
**
** sqliteTypeMappings.json maps all source data types to one of the four SQLite storage classes (TEXT, INTEGER,
** REAL, BLOB). As a result the mapped target data type does not identify the nature of the incoming data:
** character, temporal, JSON, XML and spatial data are all mapped to TEXT, while boolean and integer data are
** both mapped to INTEGER.
**
** Transformations are therefore driven by the JavaScript type of the value supplied, which mirrors the way
** SQLite itself assigns a storage class to each individual value:
**
**   strings  -> bound as TEXT
**   numbers  -> bound as INTEGER / REAL
**   booleans -> bound as INTEGER (1/0)
**   Buffers  -> bound as BLOB
**   Dates    -> bound as REAL (epoch milliseconds) : converted to ISO 8601 TEXT
**   objects  -> bound as '[object Object]'        : converted to JSON TEXT
**   null     -> bound as NULL
**
** Columns whose values are always supplied in a format the driver can bind (BLOB columns, and all columns when
** copying between SQLite databases) use the default transformer, which simply returns the value supplied.
**
*/

class SqliteOutputManager extends YadamuOutputManager {

  constructor(dbi,tableName,pipelineState,status,yadamuLogger) {
    super(dbi,tableName,pipelineState,status,yadamuLogger)
  }

  generateTransformations(dataTypes) {

    // Set up the Transformation functions to be applied to the incoming rows.
    //
    // Returning null for a column selects the default transformer, which simply returns the value supplied.

	return dataTypes.map((dataType) => {

      const dataTypeDefinition = YadamuDataTypes.decomposeDataType(dataType);

      /*
      ** GEOMETRY is matched by name because it needs recoding to GeoJSON, not merely binding as
      ** text. Everything else is dispatched on affinity.
      */

      const declaredType = dataTypeDefinition.type.toUpperCase()

      switch (declaredType === 'GEOMETRY' ? 'GEOMETRY' : this.dbi.DATA_TYPES.storageClass(declaredType)) {
        case 'GEOMETRY':
          // Spatial columns are declared GEOMETRY rather than TEXT so that the declared type still
          // identifies them as spatial when SQLite is the source. TEXT erases that, and the target then
          // falls back to its own default format and parses the wrong thing.
          //
          // SqliteDBI pins SPATIAL_FORMAT to GeoJSON, so whatever the source produced has to be recoded
          // to GeoJSON on the way in: declaring a format without storing it is what produced
          // "ORA-40441: JSON syntax error" when Oracle handed over WKB buffers.
          //
          // GEOMETRY matches none of SQLite's affinity substrings (INT/CHAR/CLOB/TEXT/BLOB/REAL/
          // FLOA/DOUB) so the column takes NUMERIC affinity. That is permissive rather than lossy:
          // a GeoJSON document cannot be losslessly converted to a number, so it is stored as TEXT,
          // and a Buffer would be stored as a BLOB byte for byte.
          switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
            case 'GeoJSON':
              return (col,idx) => {
                return typeof col === 'object' ? JSON.stringify(col) : col
              }
            default: {
              /*
              ** A source can hand over more than one representation for its spatial columns.
              ** Postgres emits WKB buffers for the types with an OGC equivalent and a GeoJSON
              ** Feature carrying a shape discriminator for circle and line, which have none - so
              ** the declared inbound format is not true of every column.
              **
              ** That mix is forced, not a fallback. The built in geometric types are a separate
              ** type family from PostGIS geometries, so PostGIS being installed does not help:
              **
              **   ST_AsGeoJSON(ST_GeomFromText('POINT(1 2)'))   works
              **   ST_AsGeoJSON(point(1,2))                      function st_asgeojson(point) does not exist
              **   point(1,2)::geometry                          casts, so GeoJSON is reachable
              **   box('((0,0),(1,1))')::geometry                cannot cast type box to geometry
              **   circle('<(0,0),1>')::geometry                 cannot cast type circle to geometry
              **
              ** verified against PostGIS 3.6. There is no version at which box or circle render as
              ** GeoJSON, which is why forcing SPATIAL_FORMAT=GeoJSON fails in the reader with a
              ** syntax error rather than producing a uniform format. Dispatch on the value:
              **
              **   Buffer  WKB, recode with wkx
              **   object  already GeoJSON, serialize it
              **   string  already serialized, or the declared format, which wkx handles
              */
              const toGeoJSON = YadamuSpatialLibrary.toGeoJSON(this.dbi.INBOUND_SPATIAL_FORMAT)
              return (col,idx) => {
                if (col === null) return null
                if (Buffer.isBuffer(col)) return JSON.stringify(YadamuSpatialLibrary.bufferToGeoJSON(col))
                if (typeof col === 'object') return JSON.stringify(col)
                if ((toGeoJSON === null) || (toGeoJSON === undefined)) return col
                const geometry = toGeoJSON(col)
                return typeof geometry === 'object' ? JSON.stringify(geometry) : geometry
              }
            }
          }
        case 'NUMERIC':
        case 'TEXT':
          // Character, temporal, JSON, XML and spatial data are stored as TEXT.
          //
          // Dates are converted to ISO 8601 strings, objects (JSON documents / GeoJSON objects) are serialized
          // to JSON, and binary values (e.g. WKB spatial data) are rendered as hex strings. String values,
          // including WKT, ISO 8601 timestamps and XML documents, are passed through unchanged.
          return (col,idx) => {
            if (typeof col === 'string')       return col
            if (col instanceof Date)           return col.toISOString()
            if (Buffer.isBuffer(col))          return col.toString('hex')
            if (typeof col === 'object')       return JSON.stringify(col)
            return col
	      }
        case 'INTEGER':
          // Boolean and integer data are both stored as INTEGER. Boolean values must be converted to 1/0: the
          // SQLite driver binds JS booleans as integers, but numeric columns can also receive booleans when
          // copying from data sources that support a dedicated boolean data type. Note that BOOLEAN_TYPE and
          // INTEGER_TYPE are both mapped to INTEGER, so the transformation is applied to the value only when it
          // is actually a boolean. Numeric values are passed through unchanged.
          return (col,idx) => {
            return typeof col === 'boolean' ? YadamuLibrary.booleanToInt(col) : col
          }
        case 'REAL':
          // SQLite stores Infinity and -Infinity as REAL values, but has no representation for NaN: it stores NaN as NULL, silently.
          // Honour INFINITY_MANAGEMENT for NaN explicitly, as the MSSQL, MySQL, MariaDB and DB2 drivers do, so that
          // REJECT is available and NULLIFY is reported rather than losing the value without a word.
          switch (this.dbi.INFINITY_MANAGEMENT) {
            case "REJECT":
              return (col,idx) => {
                if (Number.isNaN(col)) {
                  throw new RejectedColumnValue(this.tableInfo.columnNames[idx],col)
                }
                return typeof col === 'boolean' ? YadamuLibrary.booleanToInt(col) : col
              }
            case "NULLIFY":
              return (col,idx) => {
                if (Number.isNaN(col)) {
                  this.LOGGER.warning([this.dbi.DATABASE_VENDOR,this.tableName],`Column "${this.tableInfo.columnNames[idx]}" contains unsupported value "${col}". Column nullified.`)
                  return null
                }
                return typeof col === 'boolean' ? YadamuLibrary.booleanToInt(col) : col
              }
            default:
              return (col,idx) => {
                return typeof col === 'boolean' ? YadamuLibrary.booleanToInt(col) : col
              }
          }
        case 'BLOB':
        default:
          // Binary data arrives as a Buffer and is bound as a BLOB.
          return null
      }
    })
  }

}

export { SqliteOutputManager as default }
"use strict";

const YadamuLibrary = require('../../common/yadamuLibrary.js');

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, spatialFormat, batchSize, commitSize) {
    
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.spatialFormat = spatialFormat
  }
  

  async generateStatementCache () {    
  
    const sqlStatement = `select GENERATE_SQL($1,$2,$3)`
    const results = await this.dbi.executeSQL(sqlStatement,[{metadata : this.metadata}, this.targetSchema, this.spatialFormat])
    let statementCache = results.rows[0][0]
    if (statementCache === null) {
      statementCache = {}
    }
    else {
      const tables = Object.keys(this.metadata); 
      const ddlStatements = tables.map((table,idx) => {
        const tableMetadata = this.metadata[table];
        const tableName = tableMetadata.tableName;
        const tableInfo = statementCache[tableName];

        tableInfo.columnNames = tableMetadata.columnNames
        const dataTypes = YadamuLibrary.decomposeDataTypes(tableInfo.targetDataTypes)

        const maxBatchSize        = Math.trunc(45000 / tableInfo.targetDataTypes.length);
        tableInfo._BATCH_SIZE     = this.dbi.BATCH_SIZE > maxBatchSize ? maxBatchSize : this.dbi.BATCH_SIZE
        tableInfo._COMMIT_COUNT   = this.dbi.COMMIT_COUNT
        tableInfo._SPATIAL_FORMAT = this.spatialFormat
        tableInfo.insertMode      = 'Batch';
        
        tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('select ')-1) + '\nvalues ';        
        tableInfo.insertOperators = dataTypes.map((dataType) => {
		  switch (dataType.type) {
            case "geography":
            case "geometry":
			case 'point':
			case 'path':
			case 'polygon':
              switch (this.spatialFormat) {
                case "WKB":
                  return `ST_GeomFromWKB($%)::${dataType.type}`
                  break;
                case "EWKB":
                  return `ST_GeomFromEWKB($%)::${dataType.type}`
                  break;
                case "WKT":
                  return `ST_GeomFromText($%)::${dataType.type}`
                  break;
                case "EWKT":
                  return `ST_GeomFromEWKT($%)::${dataType.type}`
                  break;
                case "GeoJSON":
                  return `ST_GeomFromGeoJSON($%)::${dataType.type}`
                  break;
				case "Native":
                  return '$%';		
                default:
                  return `ST_GeomFromWKB($%)::${dataType.type}`
              }
			case 'circle':
			case 'box':
              switch (this.spatialFormat) {
                case "WKB":
                  return `${dataType.type}(ST_GeomFromWKB($%)::polygon)`
                  break;
                case "EWKB":
                  return `${dataType.type}(ST_GeomFromEWKB($%)::polygon)`
                  break;
                case "WKT":
                  return `${dataType.type}(ST_GeomFromText($%)::polygon)`
                  break;
                case "EWKT":
                  return `${dataType.type}(ST_GeomFromEWKT($%)::polygon)`
                  break;
                case "GeoJSON":
                  return `${dataType.type}(ST_GeomFromGeoJSON($%)::polygon)`
                  break;
				case "Native":
                  return '$%';		
                default:
                  return `${dataType.type}(ST_GeomFromWKB($%)::polygon)`
              }
			case 'lseg':
    		  const conversionPrefix = `(select lseg(point(p[1]),point(p[2])) from (select string_to_array(ltrim(rtrim((`;
              const conversionSuffix = `)::path)::varchar,')]'),'[('),'),(') p) as foo)`
              switch (this.spatialFormat) {
                case "WKB":
                  return `${conversionPrefix}(ST_GeomFromWKB($%)${conversionSuffix}`
                  break;
                case "EWKB":
                  return `${conversionPrefix}(ST_GeomFromEWKB($%)${conversionSuffix}`
                  break;
                case "WKT":
                  return `${conversionPrefix}(ST_GeomFromText($%)${conversionSuffix}`
                  break;
                case "EWKT":
                  return `${conversionPrefix}(ST_GeomFromEWKT($%)${conversionSuffix}`
                  break;
                case "GeoJSON":
                  return `${conversionPrefix}(ST_GeomFromGeoJSON($%)${conversionSuffix}`
                  break;
				case "Native":
                  return '$%';		
                default:
                  return `${conversionPrefix}(ST_GeomFromWKB($%)${conversionSuffix}`
              }
			   
            default:
              return '$%';
          }            
        })
        return tableInfo.ddl
      });
    }
    return statementCache;
  }
}

module.exports = StatementGenerator;
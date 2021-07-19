"use strict";

const path = require('path');
const crypto = require('crypto');
const YadamuLibrary = require('../../common/yadamuLibrary.js');

class StatementGenerator {

  constructor(dbi, targetSchema, metadata, yadamuLogger) {  
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;
  }
  

  async generateStatementCache () {    
    
    const sqlStatement = `select GENERATE_SQL($1,$2,$3)`
	
	const options = {
	  spatialFormat    : this.dbi.INBOUND_SPATIAL_FORMAT
	, jsonDataType     : this.dbi.JSON_DATA_TYPE
	}
	
    const results = await this.dbi.executeSQL(sqlStatement,[{metadata : this.metadata}, this.targetSchema, options])
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
        tableInfo._SPATIAL_FORMAT = this.dbi.INBOUND_SPATIAL_FORMAT
        tableInfo.insertMode      = 'Batch';
        
        tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('select ')-1) + '\nvalues ';    

        tableInfo.insertOperators = dataTypes.map((dataType,idx) => {
		  switch (dataType.type) {
            case "geography":
            case "geometry":
			case 'point':
			case 'path':
			case 'polygon':
              switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
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
                  return `$%::${dataType.type}`
              }
			case 'circle':
			  switch (this.dbi.INBOUND_CIRCLE_FORMAT) {
			    case "CIRCLE":
				  return `YADAMU_AsCircle($%)`
				default:
                  switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
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
			          return `$%::${dataType.type}`
                  }
		      }
			case 'box':
              switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
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
			      return `$%::${dataType.type}`
              }
			case 'lseg':
              switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
                case "WKB":
                  return `YADAMU_AsLSeg(ST_GeomFromWKB($%)::path)`
                  break;
                case "EWKB":
                  return `YADAMU_AsLSeg(ST_GeomFromEWKB($%)::path)`
                  break;
                case "WKT":
                  return `YADAMU_AsLSeg(ST_GeomFromText($%)::path)`
                  break;
                case "EWKT":
                  return `YADAMU_AsLSeg(ST_GeomFromEWKT($%)::path}`
                  break;
                case "GeoJSON":
                  return `YADAMU_AsLSeg(ST_GeomFromGeoJSON($%)::path)`
                  break;
				case "Native":
                  return '$%';		
                default:
                  return `$%::lseg`
              }
			case 'line':
			  return `YADAMU_AsLine($%)`
			case 'int4range':
			case 'int8range':
			case 'numrange':
			case 'tsrange':
			case 'tstzrange':
			case 'daterange':
			  return `YADAMU_AsRange($%)::${dataType.type}`
			case 'tsvector':
			  return `YADAMU_AsTsVector($%)`
			case 'bit':
			  switch (dataTypes[idx].typeQualifier) {
				 case null:
		           const length = this.metadata[tableName].sizeConstraints[idx]
			       return length ? `rpad($%,${length},'0')::bit(${length})` : `$%`
			  }
              return '$%';
            default:
              return '$%';
          }            
        })
		

        if (tableMetadata.dataFile) {
		  const columnDefinitions = []
          const copyOperators = dataTypes.map((dataType,idx) => {
		    switch (dataType.type) {
    	      case 'bytea':
		        columnDefinitions.push(`"${tableInfo.columnNames[idx]}" text`)
		        return `decode("${tableInfo.columnNames[idx]}",'hex')`   			  
		      case 'character':
		      case 'character varying':
		      case 'char':
		      case 'nchar':
		      case 'bpchar':
		        columnDefinitions.push(`"${tableInfo.columnNames[idx]}" text`)
		        return `"${tableInfo.columnNames[idx]}"`
			  case 'time':
		        columnDefinitions.push(`"${tableInfo.columnNames[idx]}" text`)
		        return `case when position('1970-01-01T' in "${tableInfo.columnNames[idx]}") = 1 then substring("${tableInfo.columnNames[idx]}",12)::time else "${tableInfo.columnNames[idx]}"::time end`
				
		    }		  
		    columnDefinitions.push(`"${tableInfo.columnNames[idx]}" ${tableInfo.targetDataTypes[idx]}`)
		    return `"${tableInfo.columnNames[idx]}"`
		  })

          const externalTableName = `"${this.targetSchema}"."YXT-${crypto.randomBytes(16).toString("hex").toUpperCase()}"`;
		  tableInfo.copy = {
		    ddl          : `create foreign table ${externalTableName} (${columnDefinitions.join(",")}) SERVER "${this.dbi.COPY_SERVER_NAME}" options (format 'csv', filename '${tableMetadata.dataFile.split(path.sep).join(path.posix.sep)}')`
          , dml          : `insert into "${this.targetSchema}"."${tableName}" select ${copyOperators.join(",")} from ${externalTableName}`
	      , drop         : `drop foreign table ${externalTableName}`
	      }
        } 
	    return tableInfo.ddl
      });
	  
    }
    return statementCache;
  }
}

module.exports = StatementGenerator;
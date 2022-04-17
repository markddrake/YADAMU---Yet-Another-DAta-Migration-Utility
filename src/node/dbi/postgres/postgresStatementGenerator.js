
import path                     from 'path';
import crypto                   from 'crypto';

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

class PostgresStatementGenerator extends YadamuStatementGenerator {

  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {  
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }
  
  async generateStatementCache () {    
    
	await this.init()
		
    const sqlStatement = `select GENERATE_STATEMENTS($1,$2,$3,$4)`
	
	const options = {
	  spatialFormat        : this.dbi.INBOUND_SPATIAL_FORMAT
	, jsonStorageOption    : this.dbi.DATA_TYPES.storageOptions.JSON_TYPE
	}
	
	const vendorTypeMappings = Array.from(this.TYPE_MAPPINGS.entries())
	
	// Passing vendorTypeMappings as Native Javascript Array causes Parsing errors...

    const results = await this.dbi.executeSQL(sqlStatement,[{metadata : this.metadata}, JSON.stringify(vendorTypeMappings), this.targetSchema, options])
    let statementCache = results.rows[0][0]

    // await this.debugStatementGenerator(options,statementCache)
	
	 
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
		const dataTypeDefinitions = YadamuDataTypes.decomposeDataTypes(tableInfo.targetDataTypes)

        const maxBatchSize        = Math.trunc(45000 / tableInfo.targetDataTypes.length);
        tableInfo._BATCH_SIZE     = this.dbi.BATCH_SIZE > maxBatchSize ? maxBatchSize : this.dbi.BATCH_SIZE
        tableInfo._SPATIAL_FORMAT = this.dbi.INBOUND_SPATIAL_FORMAT
        tableInfo.insertMode      = 'Batch';
        
        tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('select ')-1) + '\nvalues ';    
        tableInfo.sizeConstraints = tableMetadata.sizeConstraints
		
        tableInfo.insertOperators = dataTypeDefinitions.map((dataTypeDefinition,idx) => {
		  switch (dataTypeDefinition.type) {
            case this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:
            case this.dbi.DATA_TYPES.GEOMETRY_TYPE:
			case this.dbi.DATA_TYPES.POINT_TYPE:
			case this.dbi.DATA_TYPES.PATH_TYPE:
			case this.dbi.DATA_TYPES.POLYGON_TYPE:
              switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
                case "WKB":
                  return `ST_GeomFromWKB($%)::${dataTypeDefinition.type}`
                  break;
                case "EWKB":
                  return `ST_GeomFromEWKB($%)::${dataTypeDefinition.type}`
                  break;
                case "WKT":
                  return `ST_GeomFromText($%)::${dataTypeDefinition.type}`
                  break;
                case "EWKT":
                  return `ST_GeomFromEWKT($%)::${dataTypeDefinition.type}`
                  break;
                case "GeoJSON":
                  return `ST_GeomFromGeoJSON($%)::${dataTypeDefinition.type}`
                  break;
				case "Native":
                  return '$%';		
                default:
                  return `$%::${dataTypeDefinition.type}`
              }
			case this.dbi.DATA_TYPES.PGSQL_CIRCLE_TYPE:
			  switch (this.dbi.INBOUND_CIRCLE_FORMAT) {
			    case "CIRCLE":
				  return `YADAMU_AsCircle($%)`
				default:
                  switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
                    case "WKB":
                      return `${dataTypeDefinition.type}(ST_GeomFromWKB($%)::polygon)`
                      break;
                    case "EWKB":
                      return `${dataTypeDefinition.type}(ST_GeomFromEWKB($%)::polygon)`
                      break;
                    case "WKT":
                      return `${dataTypeDefinition.type}(ST_GeomFromText($%)::polygon)`
                      break;
                    case "EWKT":
                      return `${dataTypeDefinition.type}(ST_GeomFromEWKT($%)::polygon)`
                      break;
                    case "GeoJSON":
                      return `${dataTypeDefinition.type}(ST_GeomFromGeoJSON($%)::polygon)`
                      break;
				    case "Native":
                      return '$%';		
                    default:
			          return `$%::${dataTypeDefinition.type}`
                  }
		      }
			case this.dbi.DATA_TYPES.BOX_TYPE:
              switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
                case "WKB":
                  return `${dataTypeDefinition.type}(ST_GeomFromWKB($%)::polygon)`
                  break;
                case "EWKB":
                  return `${dataTypeDefinition.type}(ST_GeomFromEWKB($%)::polygon)`
                  break;
                case "WKT":
                  return `${dataTypeDefinition.type}(ST_GeomFromText($%)::polygon)`
                  break;
                case "EWKT":
                  return `${dataTypeDefinition.type}(ST_GeomFromEWKT($%)::polygon)`
                  break;
                case "GeoJSON":
                  return `${dataTypeDefinition.type}(ST_GeomFromGeoJSON($%)::polygon)`
                  break;
				case "Native":
                  return '$%';		
                default:
			      return `$%::${dataTypeDefinition.type}`
              }
			case this.dbi.DATA_TYPES.LINE_TYPE:
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
                  return `YADAMU_AsLSeg(ST_GeomFromEWKT($%)::path)`
                  break;
                case "GeoJSON":
                  return `YADAMU_AsLSeg(ST_GeomFromGeoJSON($%)::path)`
                  break;
				case "Native":
                  return '$%';		
                default:
                  return `$%::lseg`
              }
			case this.dbi.DATA_TYPES.PGSQL_LINE_EQ_TYPE:
			  return `YADAMU_AsLine($%)`
			case this.dbi.DATA_TYPES.PGSQL_RANGE_INT4_TYPE:
			case this.dbi.DATA_TYPES.PGSQL_RANGE_INT8_TYPE:
			case this.dbi.DATA_TYPES.PGSQL_RANGE_NUM_TYPE:
			case this.dbi.DATA_TYPES.PGSQL_RANGE_TIMESTAMP_TYPE:
			case this.dbi.DATA_TYPES.PGSQL_RANGE_TIMESTAMP_TZ_TYPE:
			case this.dbi.DATA_TYPES.PGSQL_RANGE_DATE_TYPE:
			  return `YADAMU_AsRange($%)::${dataTypeDefinition.type}`
			case this.dbi.DATA_TYPES.PGSQL_TEXTSEACH_VECTOR_TYPE:
			  return `YADAMU_AsTsVector($%)`
			case this.dbi.DATA_TYPES.BIT_STRING_TYPE:
			  switch (dataTypeDefinitions[idx].typeQualifier) {
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
          const copyOperators = dataTypeDefinitions.map((dataTypeDefinition,idx) => {
		    switch (dataTypeDefinition.type) {
    	      case this.dbi.DATA_TYPES.BINARY_TYPE:
		        columnDefinitions.push(`"${tableInfo.columnNames[idx]}" text`)
		        return `decode("${tableInfo.columnNames[idx]}",'hex')`   			  
		      case this.dbi.DATA_TYPES.CHAR_TYPE:
		      case this.dbi.DATA_TYPES.VARCHAR_TYPE:
		      case this.dbi.DATA_TYPES.PGSQL_SINGLE_CHAR_TYPE:
		      case this.dbi.DATA_TYPES.PGSQL_BPCHAR_TYPE:
		        columnDefinitions.push(`"${tableInfo.columnNames[idx]}" text`)
		        return `"${tableInfo.columnNames[idx]}"`
			  case this.dbi.DATA_TYPES.TIME_TYPE:
		        columnDefinitions.push(`"${tableInfo.columnNames[idx]}" text`)
		        return `case when position('1970-01-01T' in "${tableInfo.columnNames[idx]}") = 1 then substring("${tableInfo.columnNames[idx]}",12)::time else "${tableInfo.columnNames[idx]}"::time end`
				
		    }		  
		    columnDefinitions.push(`"${tableInfo.columnNames[idx]}" ${tableInfo.targetdataTypeDefinitions[idx]}`)
		    return `"${tableInfo.columnNames[idx]}"`
		  })

          // Partitioned Tables need one entry per partition 

          if (tableMetadata.hasOwnProperty('partitionCount')) {
	  	    tableInfo.copy = tableMetadata.dataFile.map((filename,idx) => {
              const externalTableName = `"${this.targetSchema}"."YXT-${crypto.randomBytes(16).toString("hex").toUpperCase()}"`;
			  return  {
  		        ddl             : `create foreign table ${externalTableName} (${columnDefinitions.join(",")}) SERVER "${this.dbi.COPY_SERVER_NAME}" options (format 'csv', filename '${filename.split(path.sep).join(path.posix.sep)}')`
              , dml             : `insert into "${this.targetSchema}"."${tableName}" select ${copyOperators.join(",")} from ${externalTableName}`
	          , drop            : `drop foreign table ${externalTableName}`
			  , partitionCount  : tableMetadata.partitionCount
			  , partitionID     : idx+1
	          }
			})
		  }
		  else {
			const externalTableName = `"${this.targetSchema}"."YXT-${crypto.randomBytes(16).toString("hex").toUpperCase()}"`;
	    	tableInfo.copy = {
		      ddl          : `create foreign table ${externalTableName} (${columnDefinitions.join(",")}) SERVER "${this.dbi.COPY_SERVER_NAME}" options (format 'csv', filename '${tableMetadata.dataFile.split(path.sep).join(path.posix.sep)}')`
            , dml          : `insert into "${this.targetSchema}"."${tableName}" select ${copyOperators.join(",")} from ${externalTableName}`
	        , drop         : `drop foreign table ${externalTableName}`
	        }
		  }

        } 
	    return tableInfo.ddl
      });
	  
    }
    return statementCache;
  }
}

export { PostgresStatementGenerator as default }
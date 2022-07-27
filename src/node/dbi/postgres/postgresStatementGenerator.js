
import path                     from 'path';
import crypto                   from 'crypto';

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

class PostgreStatementGenerator extends YadamuStatementGenerator {

  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) { 
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }
  
  async generateStatementCache () {    

	await this.init()
		
    const sqlStatement = `select YADAMU.GENERATE_STATEMENTS($1,$2,$3,$4)`
	
	const options = {
	  spatialFormat        : this.SPATIAL_FORMAT
	, jsonStorageOption    : this.dbi.DATA_TYPES.storageOptions.JSON_TYPE
	}
	
	const vendorTypeMappings = Array.from(this.TYPE_MAPPINGS.entries())
	
	// Passing vendorTypeMappings as Native Javascript Array causes Parsing errors...

    const results = await this.dbi.executeSQL(sqlStatement,[{metadata : this.metadata}, JSON.stringify(vendorTypeMappings), this.targetSchema, options])
    let statementCache = results.rows[0][0]

    // this.debugStatementGenerator(options,statementCache)
 
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
        
		tableInfo.insertMode      = 'Batch';
        tableInfo._BATCH_SIZE     = this.dbi.BATCH_SIZE > maxBatchSize ? maxBatchSize : this.dbi.BATCH_SIZE
        tableInfo._SPATIAL_FORMAT = this.getSpatialFormat(tableMetadata)
        
        tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('select ')-1) + '\nvalues ';    
        tableInfo.sizeConstraints = tableMetadata.sizeConstraints
		
        tableInfo.insertOperators = dataTypeDefinitions.map((dataTypeDefinition,idx) => {
		  switch (dataTypeDefinition.type) {
            case this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:
            case this.dbi.DATA_TYPES.GEOMETRY_TYPE:
			case this.dbi.DATA_TYPES.POINT_TYPE:
			case this.dbi.DATA_TYPES.PATH_TYPE:
			case this.dbi.DATA_TYPES.POLYGON_TYPE:
			  if (this.dbi.POSTGIS_INSTALLED) {
                switch (this.SPATIAL_FORMAT) {
                  case "WKB":
                    return `ST_GeomFromWKB($%)::${dataTypeDefinition.type}`
                  case "EWKB":
                    return `ST_GeomFromEWKB($%)::${dataTypeDefinition.type}`
                  case "WKT":
                    return `ST_GeomFromText($%)::${dataTypeDefinition.type}`
                  case "EWKT":
                    return `ST_GeomFromEWKT($%)::${dataTypeDefinition.type}`
                  case "GeoJSON":
                    return `ST_GeomFromGeoJSON($%)::${dataTypeDefinition.type}`
                  default:
                    return `$%::${dataTypeDefinition.type}`
				}  
			  }
			  else {
		        switch (dataTypeDefinition.type) {
			      case this.dbi.DATA_TYPES.POINT_TYPE:
				    return `YADAMU.AS_POINT($%)`
			      case this.dbi.DATA_TYPES.PATH_TYPE:
				    return `YADAMU.AS_PATH($%::jsonb)`
			      case this.dbi.DATA_TYPES.POLYGON_TYPE:
				    return `YADAMU.AS_POLYGON($%)`
				}	  
			  }
			  return '$%';
			case this.dbi.DATA_TYPES.PGSQL_CIRCLE_TYPE:
			  switch (this.dbi.INBOUND_CIRCLE_FORMAT) {
			    case "CIRCLE":
				  return `YADAMU.AS_CIRCLE($%)`
				default:
    			  if (this.dbi.POSTGIS_INSTALLED) {
                    switch (this.SPATIAL_FORMAT) {
                      case "WKB":
                        return `${dataTypeDefinition.type}(ST_GeomFromWKB($%)::polygon)`
                      case "EWKB":
                        return `${dataTypeDefinition.type}(ST_GeomFromEWKB($%)::polygon)`
                      case "WKT":
                        return `${dataTypeDefinition.type}(ST_GeomFromText($%)::polygon)`
                      case "EWKT":
                        return `${dataTypeDefinition.type}(ST_GeomFromEWKT($%)::polygon)`
                      case "GeoJSON":
                        return `${dataTypeDefinition.type}(ST_GeomFromGeoJSON($%)::polygon)`
  				      case "Native":
                        return '$%';		
                      default:
			            return `$%::${dataTypeDefinition.type}`
                    }
				 }
		      }
			  return '$%';		  
			case this.dbi.DATA_TYPES.BOX_TYPE:
  			  if (this.dbi.POSTGIS_INSTALLED) {
                switch (this.SPATIAL_FORMAT) {
                  case "WKB":
                    return `${dataTypeDefinition.type}(ST_GeomFromWKB($%)::polygon)`
                  case "EWKB":
                    return `${dataTypeDefinition.type}(ST_GeomFromEWKB($%)::polygon)`
                  case "WKT":
                    return `${dataTypeDefinition.type}(ST_GeomFromText($%)::polygon)`
                  case "EWKT":
                    return `${dataTypeDefinition.type}(ST_GeomFromEWKT($%)::polygon)`
                  case "GeoJSON":
                    return `${dataTypeDefinition.type}(ST_GeomFromGeoJSON($%)::polygon)`
                  default:
			        return `$%::${dataTypeDefinition.type}`
                }
			  }
			  else {
				return `YADAMU.AS_BOX($%)`
			  }
			  return '$%';		  
			case this.dbi.DATA_TYPES.LINE_TYPE:
  			  if (this.dbi.POSTGIS_INSTALLED) {
                switch (this.SPATIAL_FORMAT) {
                  case "WKB":
                    return `YADAMU.AS_LINE_SEGMENT(ST_GeomFromWKB($%)::path)`
                  case "EWKB":
                    return `YADAMU.AS_LINE_SEGMENT(ST_GeomFromEWKB($%)::path)`
                  case "WKT":
                    return `YADAMU.AS_LINE_SEGMENT(ST_GeomFromText($%)::path)`
                  case "EWKT":
                    return `YADAMU.AS_LINE_SEGMENT(ST_GeomFromEWKT($%)::path)`
                  case "GeoJSON":
                    return `YADAMU.AS_LINE_SEGMENT(ST_GeomFromGeoJSON($%)::path)`
                  default:
                    return `$%::lseg`
                }
			  }
			  else {						   
				return `YADAMU.AS_LINE_SEGMENT($%::jsonb)`
			  }
			case this.dbi.DATA_TYPES.PGSQL_LINE_EQ_TYPE:
			  return `YADAMU.AS_LINE_EQ($%)`
			case this.dbi.DATA_TYPES.PGSQL_RANGE_INT4_TYPE:
			case this.dbi.DATA_TYPES.PGSQL_RANGE_INT8_TYPE:
			case this.dbi.DATA_TYPES.PGSQL_RANGE_NUM_TYPE:
			case this.dbi.DATA_TYPES.PGSQL_RANGE_TIMESTAMP_TYPE:
			case this.dbi.DATA_TYPES.PGSQL_RANGE_TIMESTAMP_TZ_TYPE:
			case this.dbi.DATA_TYPES.PGSQL_RANGE_DATE_TYPE:
			  return `YADAMU.AS_RANGE($%)::${dataTypeDefinition.type}`
			case this.dbi.DATA_TYPES.PGSQL_TEXTSEACH_VECTOR_TYPE:
			  return `YADAMU.AS_TS_VECTOR($%)`
			case this.dbi.DATA_TYPES.BIT_STRING_TYPE:
			  switch (dataTypeDefinitions[idx].typeQualifier) {
				 case null:
		           const length = this.metadata[tableName].sizeConstraints[idx][0]
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
		        return `case when length("${tableInfo.columnNames[idx]}") > 16 then substring("${tableInfo.columnNames[idx]}",12,15)::time when  length("${tableInfo.columnNames[idx]}") > 15 then substring("${tableInfo.columnNames[idx]}",1,15)::time else "${tableInfo.columnNames[idx]}"::time end`				
			  case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
		        columnDefinitions.push(`"${tableInfo.columnNames[idx]}" text`)
		        return `case when length("${tableInfo.columnNames[idx]}") > 26 then substring("${tableInfo.columnNames[idx]}",1,26)::timestamp without time zone  else "${tableInfo.columnNames[idx]}"::timestamp without time zone end`				
			  default:	
		    }		  
		    columnDefinitions.push(`"${tableInfo.columnNames[idx]}" ${tableInfo.targetDataTypes[idx]}`)
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

export { PostgreStatementGenerator as default }
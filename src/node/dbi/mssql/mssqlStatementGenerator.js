
import path                     from 'path';

import sql                      from 'mssql';

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

class MsSQLStatementGenerator extends YadamuStatementGenerator {
  
  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {  
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }
    
  bulkSupported(dataTypes) {
    
	for (const dataType of dataTypes) {
      switch (dataType.type.toLowerCase()) {
        case this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:
          // TypeError: parameter.type.validate is not a function
          return false;
	    case this.dbi.DATA_TYPES.GEOMETRY_TYPE:
          // TypeError: parameter.type.validate is not a function
          return false;
	    case this.dbi.DATA_TYPES.XML_TYPE:1
          // Unsupported Data Type for BCP
          return false;		
        /*
 	    ** Upload images as VarBinary(MAX). Convert data to Buffer. This enables bulk upload and avoids Collation issues...
	    **
		
        case this.dbi.DATA_TYPES.image':
           return false;
		  
	    **
        */
      }
    }
    return true
   
  }
  
  getMetadata() {
	return JSON.stringify({metadata : this.metadata})
  }

  getSourceTypeMappings() {
	 JSON.stringify(vendorTypeMappings)
  }
  
  async generateStatementCache () {

    await this.init()
    
	const vendorTypeMappings = Array.from(this.TYPE_MAPPINGS.entries())
	
    const args = { 
	        inputs: [{
              name: 'METADATA',        type: sql.NVARCHAR, value: this.getMetadata()
			},{
              name: 'TYPE_MAPPINGS',   type: sql.NVARCHAR, value: this.getSourceTypeMappings()
			},{
			  name: 'TARGET_DATABASE', type: sql.VARCHAR,  value: this.targetSchema
			},{
              name: 'SPATIAL_FORMAT',  type: sql.NVARCHAR, value: this.dbi.INBOUND_SPATIAL_FORMAT
	        },{ 
			  name: 'DB_COLLATION',    type: sql.NVARCHAR, value: this.dbi.DB_COLLATION
			}]
	      }
    
	// console.log(args)

    let results = await this.dbi.execute('master.dbo.sp_GENERATE_STATEMENTS',args,'SQL_STATEMENTS')
    results = results.output[Object.keys(results.output)[0]]
    const statementCache = JSON.parse(results)
	
    // this.debugStatementGenerator(null,statementCache)

	const tables = Object.keys(this.metadata); 
    const ddlStatements = tables.map((table,idx) => {
      const tableMetadata = this.metadata[table];
      const tableName = tableMetadata.tableName;
      statementCache[tableName] = statementCache[tableName]
      const tableInfo = statementCache[tableName]; 

      tableInfo.columnNames = tableMetadata.columnNames
      // msssql requires type and length information when generating a prepared statement.
      const dataTypeDefinitions  = YadamuDataTypes.decomposeDataTypes(tableInfo.targetDataTypes)
      
      tableInfo._BATCH_SIZE     = this.dbi.BATCH_SIZE
      tableInfo._SPATIAL_FORMAT = this.dbi.INBOUND_SPATIAL_FORMAT
	  
      // Create table before attempting to Prepare Statement..
      tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + "\nVALUES (";
      dataTypeDefinitions.forEach((dataTypeDefinition,idx) => {
        switch(dataTypeDefinition.type) {
          case this.dbi.DATA_TYPES.IMAGE_TYPE:
		    // Upload images as VarBinary(MAX). Convert data to Buffer. This enables BCP Operations and avoids Collation issues...
            tableInfo.dml = tableInfo.dml + 'convert(image,@C' + idx + ')' + ','
            break;
          case this.dbi.DATA_TYPES.XML_TYPE:
            tableInfo.dml = tableInfo.dml + 'convert(XML,@C' + idx + ',1)' + ','
            break;
          case this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:
            switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
               case "WKT":
               case "EWKT":
                 tableInfo.dml = tableInfo.dml + 'geography::STGeomFromText(@C' + idx + ',4326)' + ','
                 break
               case "WKB":
               case "EWKB":
                 tableInfo.dml = tableInfo.dml + 'geography::STGeomFromWKB(@C' + idx + ',4326)' + ','
                 break
			   case "GeoJSON":
                 tableInfo.dml = tableInfo.dml + 'geography::STGeomFromText(@C' + idx + ',4326)' + ','
                 break
               default:
                 tableInfo.dml = tableInfo.dml + 'geography::STGeomFromWKB(@C' + idx + ',4326)' + ','
            }    
            break;          
          case this.dbi.DATA_TYPES.GEOMETRY_TYPE:
            switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
               case "WKT":
               case "EWKT":
                 tableInfo.dml = tableInfo.dml + 'geometry::STGeomFromText(@C' + idx + ',4326)' + ','
                 break
               case "WKB":
               case "EWKB":
                 tableInfo.dml = tableInfo.dml + 'geometry::STGeomFromWKB(@C' + idx + ',4326)' + ','
                 break
			   case "GeoJSON":
                 tableInfo.dml = tableInfo.dml + 'geometry::STGeomFromText(@C' + idx + ',4326)' + ','
                 break			   
               default:
                 tableInfo.dml = tableInfo.dml + 'geometry::STGeomFromWKB(@C' + idx + ',4326)' + ','
            }      
            break;            
    	  case this.dbi.DATA_TYPES.NUMERIC_TYPE:
		  case this.dbi.DATA_TYPES.DECIMAL_TYPE:
		    if ((dataTypeDefinition.length || 18) > 15) {
              tableInfo.dml = tableInfo.dml + 'cast(@C' + idx+ ` as ${tableInfo.targetDataTypes[idx]}),`
			  break;
		    }
            tableInfo.dml = tableInfo.dml + '@C' + idx+ ','
			break;
		  case this.dbi.DATA_TYPES.BIGINT_TYPE:
		    tableInfo.dml = tableInfo.dml + 'cast(@C' + idx+ ` as ${tableInfo.targetDataTypes[idx]}),`
			break;
          default: 
             tableInfo.dml = tableInfo.dml + '@C' + idx+ ','
        }
      })
	  
      tableInfo.dml = tableInfo.dml.slice(0,-1) + ")";
 	  tableInfo.dataTypeDefinitions = dataTypeDefinitions
	  tableInfo.insertMode = this.bulkSupported(dataTypeDefinitions) ? 'BCP' : 'Iterative'

	  try {
	
		if (tableMetadata.dataFile) {
		  const loadColumnNames = []
		  const setOperations = []
          const copyOperators = dataTypeDefinitions.map((dataTypeDefinition,idx) => {
			const psuedoColumnName = `@YADAMU_${String(idx+1).padStart(3,"0")}`
   	        loadColumnNames.push(psuedoColumnName);
		    setOperations.push(`"${tableInfo.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, ${psuedoColumnName})`)
			switch (dataTypeDefinition.type.toLowerCase()) {
              case this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:
              case this.dbi.DATA_TYPES.GEOMETRY_TYPE:
			    let spatialFunction
                switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
                  case "WKB":
                  case "EWKB":
                    spatialFunction = `ST_GeomFromWKB(UNHEX(${psuedoColumnName}))`;
                    break;
                  case "WKT":
                  case "EWRT":
                    spatialFunction = `ST_GeomFromText(${psuedoColumnName})`;
                    break;
                  case "GeoJSON":
                    spatialFunction = `ST_GeomFromGeoJSON(${psuedoColumnName})`;
                    break;
                  default:
                    return `ST_GeomFromWKB(UNHEX(${psuedoColumnName}))`;
				}
                setOperations[idx] = `"${tableInfo.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, ${spatialFunction})`
			    break
              case this.dbi.DATA_TYPES.BINARY_TYPE:                              
              case this.dbi.DATA_TYPES.VARBINARY_TYPE:                              
              case this.dbi.DATA_TYPES.BLOB_TYPE:                                 
                setOperations[idx] = `"${tableInfo.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, UNHEX(${psuedoColumnName}))`
			    break;
			  case this.dbi.DATA_TYPES.TIME_TYPE:
                setOperations[idx] = `"${tableInfo.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(INSTR(${psuedoColumnName},'.') > 0,str_to_date(${psuedoColumnName},'%Y-%m-%dT%T.%f'),str_to_date(${psuedoColumnName},'%Y-%m-%dT%T')))`
			    break;
			  case this.dbi.DATA_TYPES.DATETIME_TYPE:
			  case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
                setOperations[idx] = `"${tableInfo.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(INSTR(${psuedoColumnName},'.') > 0,str_to_date(${psuedoColumnName},'%Y-%m-%dT%T.%f'),str_to_date(${psuedoColumnName},'%Y-%m-%dT%T')))`
			    break;
  			  case this.dbi.DATA_TYPES.TINYINT_TYPE:    
                switch (true) {
                  case ((dataTypeDefinition.length === 1) && this.dbi.TREAT_TINYINT1_AS_BOOLEAN):
                     setOperations[idx] = `"${tableInfo.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(${psuedoColumnName} = 'true',1,0))`
				     break;
				}
                break;				 
	          default:
			}
		  })

		  tableInfo.copy = {
	        dml         : `bulk insert "${this.targetSchema}"."${tableName}" from '${tableMetadata.dataFile.split(path.sep).join(path.posix.sep)}' WITH ( MAXERRORS = ${this.dbi.TABLE_MAX_ERRORS}, KEEPNULLS, FORMAT = 'CSV', FIELDTERMINATOR = ',', ROWTERMINATOR = '\n')`
	      }
        }  
        return tableInfo.ddl;
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}`],e)
        this.yadamuLogger.writeDirect(`${tableInfo.ddl}`)
      } 
    });
    return statementCache;
  }
}

export { MsSQLStatementGenerator as default }

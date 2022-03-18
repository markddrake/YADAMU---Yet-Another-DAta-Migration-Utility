
import path                     from 'path';

import YadamuLibrary            from '../../lib/yadamuLibrary.js';

import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

class MySQLStatementGenerator extends YadamuStatementGenerator {
  
  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {  
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }
 
  async generateStatementCache() {    
  
    const typeMappings = {
	  spatialFormat    : this.dbi.INBOUND_SPATIAL_FORMAT
	, circleFormat     : this.dbi.INBOUND_CIRCLE_FORMAT
	}

    const sqlStatement = `SET @RESULTS = '{}'; CALL GENERATE_STATEMENTS(?,?,?,@RESULTS); SELECT @RESULTS "INSERT_INFORMATION"`;                       
    let results = await this.dbi.executeSQL(sqlStatement,[JSON.stringify({metadata : this.metadata}),this.targetSchema, JSON.stringify(typeMappings)]);
	
    results = results.pop();
    let statementCache = JSON.parse(results[0].INSERT_INFORMATION)
	
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
		
		tableInfo.sourceDataTypes = tableMetadata.source?.dataTypes || []
        const dataTypes = YadamuLibrary.decomposeDataTypes(tableInfo.targetDataTypes)
		
        tableInfo._BATCH_SIZE     = this.dbi.BATCH_SIZE
        tableInfo._SPATIAL_FORMAT = this.dbi.INBOUND_SPATIAL_FORMAT
        tableInfo.insertMode      = 'Batch';
        /*
        **
        ** Avoid use of Iterative Mode where possible due to significant performance impact.
        **
        */
        const setOperators = dataTypes.map((dataType,idx) => {
	      if (this.dbi.DB_VERSION < '8.0.19' || false) {
            switch (dataType.type) {
              case 'geometry':
			  case 'point':
			  case 'lseg':
			  case 'linestring':
			  case 'box':
			  case 'path':
			  case 'polygon':
			  case 'multipoint':
			  case 'multilinestring':
			  case 'multipolygon':
			  case 'geomcollection':
			  case 'geometrycollection':
                tableInfo.insertMode = 'Iterative'; 
                switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
                  case "WKB":
                  case "EWKB":
                    return ' "' + tableInfo.columnNames[idx] + '"' + " = ST_GeomFromWKB(?)";
                    break;
                  case "WKT":
                  case "EWRT":
                    return ' "' + tableInfo.columnNames[idx] + '"' + " = ST_GeomFromText(?)";
                    break;
                  case "GeoJSON":
                    return ' "' + tableInfo.columnNames[idx] + '"' + " = ST_GeomFromGeoJSON(?)";
                    break;
                  default:
                    return ' "' + tableInfo.columnNames[idx] + '"' + " = ST_GeomFromWKB(?)";
                }              
			  case 'bit':
                this.tableInfo.insertMode = 'Iterative';
      	        return 'conv(?,2,10)+0';
              default:
                return ' "' + tableInfo.columnNames[idx] + '" = ?'
            }
          }
          else {
            switch (dataType.type) {
              case 'geometry':
			  case 'point':
			  
			  case 'lseg':
			  case 'linestring':
			  case 'box':
			  case 'path':
			  case 'polygon':
			  case 'multipoint':
			  case 'multilinestring':
			  case 'multipolygon':
			  case 'geomcollection':
			  case 'geometrycollection':
                tableInfo.insertMode = 'Rows';  
                switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
                  case "WKB":
                  case "EWKB":
                    return 'ST_GeomFromWKB(?)';
                    break;
                  case "WKT":
                  case "EWRT":
                    return 'ST_GeomFromText(?)';
                    break;
                  case "GeoJSON":
                    return 'ST_GeomFromGeoJSON(?)';
                    break;
                  default:
                    return 'ST_GeomFromWKB(?)';
                }              
			  case 'bit':
                tableInfo.insertMode = 'Rows';  
      	        return `conv(rpad(?,${dataType.length},'0'),2,10)+0`;
              default:
                return '?'
            }
          }
        }) 

        

        tableInfo.rowConstructor = `(${setOperators.join(',')})`
        switch (tableInfo.insertMode) {
          case 'Batch':
            tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + `  values `;
            break;
          case 'Rows':
            tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + `  values `;
            break;
          case 'Iterative':
            tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf('(')) + ` set ` + setOperators.join(',');
            break;
        }
       
        if (tableMetadata.dataFile) {
		  const loadColumnNames = []
		  const setOperations = []
          const copyOperators = dataTypes.map((dataType,idx) => {
			const psuedoColumnName = `@YADAMU_${String(idx+1).padStart(3,"0")}`
   	        loadColumnNames.push(psuedoColumnName);
		    setOperations.push(`"${tableInfo.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, ${psuedoColumnName})`)
			switch (dataType.type.toLowerCase()) {
			  case 'point':
              case 'linestring':
              case 'polygon':
              case 'geometry':
              case 'multipoint':
              case 'multilinestring':
              case 'multipolygon':
              case 'geometry':                             
              case 'geometrycollection':
              case 'geomcollection':
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
              case 'binary':                              
              case 'varbinary':                              
              case 'blob':                                 
              case 'tinyblob':                             
              case 'mediumblob':                           
              case 'longblob':                             
                setOperations[idx] = `"${tableInfo.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, UNHEX(${psuedoColumnName}))`
			    break;
			  case 'time':
                setOperations[idx] = `"${tableInfo.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(INSTR(${psuedoColumnName},'.') > 0,str_to_date(${psuedoColumnName},'%Y-%m-%dT%T.%f'),str_to_date(${psuedoColumnName},'%Y-%m-%dT%T')))`
			    break;
			  case 'datetime':
			  case 'timestamp':
                setOperations[idx] = `"${tableInfo.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(INSTR(${psuedoColumnName},'.') > 0,str_to_date(${psuedoColumnName},'%Y-%m-%dT%T.%f'),str_to_date(${psuedoColumnName},'%Y-%m-%dT%T')))`
			    break;
  			  case 'tinyint':    
                switch (true) {
                  case ((dataType.length === 1) && this.dbi.TREAT_TINYINT1_AS_BOOLEAN):
                     setOperations[idx] = `"${tableInfo.columnNames[idx]}" = IF(CHAR_LENGTH(${psuedoColumnName}) = 0, NULL, IF(${psuedoColumnName} = 'true',1,0))`
				     break;
				}
                break;				 
            /*
              case 'smallint':
              case 'mediumint':
              case 'integer':
              case 'bigint':
              case 'decimal':                                           
              case 'float':                                           
              case 'double':                                           
              case 'bit':
			  case 'date':
              case 'year':                            
              case 'char':                              
              case 'varchar':                              
              case 'text':                                 
              case 'tinytext':
			  case 'mediumtext':                           
              case 'longtext':                             
              case 'set':                                  
              case 'enum':                                 
              case 'json':                                 
              case 'xml':                                  
              
			*/
	          default:
			}
		  })
		  
		  // Partitioned Tables need one entry per partition 
		  
          if (tableMetadata.hasOwnProperty('partitionCount')) {
	  	    tableInfo.copy = tableMetadata.dataFile.map((filename,idx) => {
			  return  {
	            dml             : `load data infile '${filename.split(path.sep).join(path.posix.sep)}' into table "${this.targetSchema}"."${tableName}" character set UTF8 fields terminated by ',' optionally enclosed by '"' ESCAPED BY '"' lines terminated by '\n' (${loadColumnNames.join(",")}) SET ${setOperations.join(",")}`
			  , partitionCount  : tableMetadata.partitionCount
			  , partitionID     : idx+1
	          }
			})
		  }
		  else {
	    	tableInfo.copy = {
	           dml         : `load data infile '${tableMetadata.dataFile.split(path.sep).join(path.posix.sep)}' into table "${this.targetSchema}"."${tableName}" character set UTF8 fields terminated by ',' optionally enclosed by '"' ESCAPED BY '"' lines terminated by '\n' (${loadColumnNames.join(",")}) SET ${setOperations.join(",")}`
	        }
		  }
        }       
        return tableInfo.ddl;
      });
    }
	return statementCache;
  }
 
}

export { MySQLStatementGenerator as default }
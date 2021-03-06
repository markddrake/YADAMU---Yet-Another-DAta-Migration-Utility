"use strict";

const YadamuLibrary = require('../../common/yadamuLibrary.js');

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, yadamuLogger) {    
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;
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

        const dataTypes = YadamuLibrary.decomposeDataTypes(tableInfo.targetDataTypes)
		
        tableInfo._BATCH_SIZE     = this.dbi.BATCH_SIZE
        tableInfo._COMMIT_COUNT   = this.dbi.COMMIT_COUNT
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
        return tableInfo.ddl;
      });
    }
    return statementCache;
  }
 
}

module.exports = StatementGenerator;
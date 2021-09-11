"use strict";

const sql = require('mssql');
const path = require('path');

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata, yadamuLogger) {
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;
  }
  
  bulkSupported(dataTypes) {
    
	for (const dataType of dataTypes) {
      switch (dataType.type.toLowerCase()) {
        case 'geography':
          // TypeError: parameter.type.validate is not a function
          return false;
	    case 'geometry':
          // TypeError: parameter.type.validate is not a function
          return false;
	    case 'xml':
          // Unsupported Data Type for Bulk Load
          return false;		
        /*
 	    ** Upload images as VarBinary(MAX). Convert data to Buffer. This enables bulk upload and avoids Collation issues...
	    **
		
        case 'image':
           return false;
		  
	    **
        */
      }
    }
    return true
   
  }
  
  createBulkOperation(database, tableName, columnList, dataTypes) {

    const table = new sql.Table(database + '.' + this.dbi.parameters.TO_USER + '.' + tableName);
    table.create = false
    let precision
    dataTypes.forEach((dataType,idx) => {
      const length = dataType.length > 0 && dataType.length < 65535 ? dataType.length : sql.MAX
      switch (dataType.type.toLowerCase()) {
        case 'bit':
          table.columns.add(columnList[idx],sql.Bit);
          break;
        case 'bigint':
		  // Bind as VarChar to avoid rounding issues
          // table.columns.add(columnList[idx],sql.BigInt, {nullable: true});
		  table.columns.add(columnList[idx],sql.VarChar(21), {nullable: true});  
          break;
        case 'float':
          table.columns.add(columnList[idx],sql.Float, {nullable: true});
          break;
        case 'int':
          table.columns.add(columnList[idx],sql.Int, {nullable: true});
          break;
        case 'money':
          // table.columns.add(columnList[idx],sql.Money, {nullable: true});
          table.columns.add(columnList[idx],sql.Decimal(19,4), {nullable: true});
          break
        case 'decimal':
		  precision = dataType.length || 18
		  if (precision > 15) {
			// Bind as VarChar to avoid rounding issues
			table.columns.add(columnList[idx],sql.VarChar(precision+2), {nullable: true});  
		  }
		  else {
            // sql.Decimal ([precision], [scale])
            table.columns.add(columnList[idx],sql.Decimal(dataType.length || 18,dataType.scale || 0), {nullable: true});
		  }
          break;
        case 'numeric':
		  precision = dataType.length || 18
		  if (precision > 15) {
			// Bind as VarChar to avoid rounding issues
			table.columns.add(columnList[idx],sql.VarChar(precision+2), {nullable: true});  
		  }
		  else {
            // sql.Numeric ([precision], [scale])
            table.columns.add(columnList[idx],sql.Numeric(dataType.length || 18,dataType.scale || 0), {nullable: true});
		  }
          break;
        case 'smallint':
          table.columns.add(columnList[idx],sql.SmallInt, {nullable: true});
          break;
        case 'smallmoney':
          // table.columns.add(columnList[idx],sql.SmallMoney, {nullable: true});
          table.columns.add(columnList[idx],sql.Decimal(10,4), {nullable: true});
          break;
        case 'real':
          table.columns.add(columnList[idx],sql.Real, {nullable: true}, {nullable: true});
          break;
        case 'tinyint':
          table.columns.add(columnList[idx],sql.TinyInt, {nullable: true});
          break;
        case 'char':
          table.columns.add(columnList[idx],sql.Char(length), {nullable: true});
          break;
        case 'nchar':
          table.columns.add(columnList[idx],sql.NChar(length), {nullable: true});
          break;
        case 'text':
          table.columns.add(columnList[idx],sql.Text, {nullable: true});
          break;
        case 'ntext':
          table.columns.add(columnList[idx],sql.NText, {nullable: true});
          break;
        case 'varchar':
          table.columns.add(columnList[idx],sql.VarChar(length), {nullable: true});
          break;
        case 'nvarchar':
          table.columns.add(columnList[idx],sql.NVarChar(length), {nullable: true});
          break;
        case 'json':
          table.columns.add(columnList[idx],sql.NVarChar(sql.MAX), {nullable: true});
          break;
        case 'xml':
          // Added to Unsupported
          // Invalid column data type for bulk load
          table.columns.add(columnList[idx],sql.Xml, {nullable: true});
          break;
        case 'time':
          // sql.Time ([scale])
          // Binding as sql.Time must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.Time(length), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'date':
          // Binding as sql.Date must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.Date, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'datetime':
          // Binding as sql.DateTime must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.DateTime, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'datetime2':
          // sql.DateTime2 ([scale]
          // Binding as sql.DateTime2 must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.DateTime2(), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'datetimeoffset':
          // sql.DateTimeOffset ([scale])
          // Binding as sql.DateTime2 must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.DateTimeOffset(length), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'smalldatetime':
          // Binding as sql.SamllDateTime must supply values as type Date. 
          // table.columns.add(columnList[idx],sql.SmallDateTime, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columnList[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'uniqueidentifier':
          // table.columns.add(columnList[idx],sql.UniqueIdentifier, {nullable: true});
          // TypeError: parameter.type.validate is not a function
          table.columns.add(columnList[idx],sql.Char(36), {nullable: true});
          break;
        case 'variant':
          table.columns.add(columnList[idx],sql.Variant, {nullable: true});
          break;
        case 'binary':
          table.columns.add(columnList[idx],sql.Binary(length), {nullable: true});
          break;
        case 'varbinary':
          // sql.VarBinary ([length])
           table.columns.add(columnList[idx],sql.VarBinary(length), {nullable: true});
          break;
        case 'image':
  	      // Upload images as VarBinary(MAX). Convert data to Buffer. This enables bulk upload and avoids Collation issues...
          // table.columns.add(columnList[idx],sql.Image, {nullable: true});
          table.columns.add(columnList[idx],sql.VarBinary(sql.MAX), {nullable: true});
          break;
        case 'udt':
          table.columns.add(columnList[idx],sql.UDT, {nullable: true});
          break;
        case 'geography':
          // Added to Unsupported
          // TypeError: parameter.type.validate is not a function
          // table.columns.add(columnList[idx],sql.Geography, {nullable: true});
  	      // Upload geography as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer. This enables bulk upload.
		  switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
			case "WKB":
            case "EWKB":
              table.columns.add(columnList[idx],sql.VarBinary(sql.MAX), {nullable: true});
			  break;
			default:
		      table.columns.add(columnList[idx],sql.VarChar(sql.MAX), {nullable: true});
		  }
          break;
        case 'geometry':
          // Added to Unsupported
          // TypeError: parameter.type.validate is not a function
          // table.columns.add(columnList[idx],sql.Geometry, {nullable: true});
  	      // Upload geometry as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer. This enables bulk upload.
		  switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
			case "WKB":
            case "EWKB":
              table.columns.add(columnList[idx],sql.VarBinary(sql.MAX), {nullable: true});
			  break;
			default:
		      table.columns.add(columnList[idx],sql.VarChar(sql.MAX), {nullable: true});
		  }
          break;
        case 'hierarchyid':
          table.columns.add(columnList[idx],sql.VarChar(4000),{nullable: true});
          break;
        default:
          this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,`BULK OPERATION`,`"${tableName}"`],`Unmapped data type [${dataType.type}].`);
      }
    })
    return table
  }

  getMetadata() {
	return JSON.stringify({metadata : this.metadata})
  }

  async generateStatementCache (database) {
 
    const args = { 
	        inputs: [{
			  name: 'TARGET_DATABASE', type: sql.VARCHAR,  value: this.targetSchema
			},{
              name: 'SPATIAL_FORMAT',  type: sql.NVARCHAR, value: this.dbi.INBOUND_SPATIAL_FORMAT
	        },{
              name: 'METADATA',        type: sql.NVARCHAR, value: this.getMetadata()
			},{ 
			  name: 'DB_COLLATION',    type: sql.NVARCHAR, value: this.dbi.DB_COLLATION
			}]
	      }
  
    let results = await this.dbi.execute('master.dbo.sp_GENERATE_SQL',args,'SQL_STATEMENTS')
    results = results.output[Object.keys(results.output)[0]]
    const statementCache = JSON.parse(results)
	const tables = Object.keys(this.metadata); 
    const ddlStatements = tables.map((table,idx) => {
      const tableMetadata = this.metadata[table];
      const tableName = tableMetadata.tableName;
      statementCache[tableName] = statementCache[tableName]
      const tableInfo = statementCache[tableName]; 

      tableInfo.columnNames = tableMetadata.columnNames
      // msssql requires type and length information when generating a prepared statement.
      const dataTypes  = YadamuLibrary.decomposeDataTypes(tableInfo.targetDataTypes)
      
      tableInfo._BATCH_SIZE     = this.dbi.BATCH_SIZE
      tableInfo._COMMIT_COUNT   = this.dbi.COMMIT_COUNT
      tableInfo._SPATIAL_FORMAT = this.dbi.INBOUND_SPATIAL_FORMAT
       
      // Create table before attempting to Prepare Statement..
      tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + "\nVALUES (";
      dataTypes.forEach((dataType,idx) => {
        switch(dataType.type) {
          case 'image':
		    // Upload images as VarBinary(MAX). Convert data to Buffer. This enables bulk upload and avoids Collation issues...
            tableInfo.dml = tableInfo.dml + 'convert(image,@C' + idx + ')' + ','
            break;
          case "xml":
            tableInfo.dml = tableInfo.dml + 'convert(XML,@C' + idx + ',1)' + ','
            break;
          case "geography":
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
          case "geometry":
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
    	  case "numeric":
		  case "decimal":
		    if ((dataType.length || 18) > 15) {
              tableInfo.dml = tableInfo.dml + 'cast(@C' + idx+ ` as ${tableInfo.targetDataTypes[idx]}),`
			  break;
		    }
            tableInfo.dml = tableInfo.dml + '@C' + idx+ ','
			break;
		  case 'bigint':
		    tableInfo.dml = tableInfo.dml + 'cast(@C' + idx+ ` as ${tableInfo.targetDataTypes[idx]}),`
			break;
          default: 
             tableInfo.dml = tableInfo.dml + '@C' + idx+ ','
        }
      })
      tableInfo.dml = tableInfo.dml.slice(0,-1) + ")";
      tableInfo.bulkSupported = this.bulkSupported(dataTypes);
 	  tableInfo.dataTypes = dataTypes

	  try {
		
		/*
        if (tableInfo.bulkSupported) {
		  tableInfo.bulkOperations = [
		    this.createBulkOperation(database, tableName, tableMetadata.columnNames, dataTypes)
          , this.createBulkOperation(database, tableName, tableMetadata.columnNames, dataTypes)
		  ]
        }
        else {
          // Place holder for caching rows.
          tableInfo.bulkOperations = [
		    new sql.Table()                                            
          , new sql.Table()                                            
		  ]
        }
		*/
		
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

module.exports = StatementGenerator

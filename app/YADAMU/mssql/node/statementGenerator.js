"use strict";

const sql = require('mssql');

const Yadamu = require('../../common/yadamu.js');
const YadamuLibrary = require('../../common/yadamuLibrary.js');

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata,  spatialFormat, yadamuLogger) {
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.spatialFormat = spatialFormat
    this.yadamuLogger = yadamuLogger;
  }
  
  bulkSupported(dataTypes) {
    
    let supported = true;
    dataTypes.forEach((dataType,idx) => {
      switch (dataType.type.toLowerCase()) {
        case 'geography':
         // TypeError: parameter.type.validate is not a function
         supported = false;
         break;
	   case 'geometry':
         // TypeError: parameter.type.validate is not a function
         supported = false;
         break;
	   case 'xml':
         // Unsupported Data Type for Bulk Load
         supported = false;
         break;
        // Upload images as VarBinary(MAX). Convert data to Buffer. This enables bulk upload and avoids Collation issues...
		/*
        case 'image':
          supported = false;
          break;
        */
      }
     })
    return supported;
   
  }
  
  createBulkOperation(database, tableName, columnList, dataTypes) {

    const table = new sql.Table(database + '.' + this.dbi.parameters.TO_USER + '.' + tableName);
    table.create = false
    
    dataTypes.forEach((dataType,idx) => {
      const length = dataType.length > 0 && dataType.length < 65535 ? dataType.length : sql.MAX
      switch (dataType.type.toLowerCase()) {
        case 'bit':
          table.columns.add(columnList[idx],sql.Bit);
          break;
        case 'bigint':
          table.columns.add(columnList[idx],sql.BigInt, {nullable: true});
          break;
        case 'float':
          table.columns.add(columnList[idx],sql.Float, {nullable: true});
          break;
        case 'int':
          table.columns.add(columnList[idx],sql.Int, {nullable: true});
          break;
        case 'money':
          table.columns.add(columnList[idx],sql.Decimal(19,4), {nullable: true});
          // table.columns.add(columnList[idx],sql.Money, {nullable: true});
          break
        case 'decimal':
          // sql.Decimal ([precision], [scale])
          table.columns.add(columnList[idx],sql.Decimal(length,dataType.scale), {nullable: true});
          break;
        case 'smallint':
          table.columns.add(columnList[idx],sql.SmallInt, {nullable: true});
          break;
        case 'smallmoney':
          table.columns.add(columnList[idx],sql.Decimal(10,4), {nullable: true});
          // table.columns.add(columnList[idx],sql.SmallMoney, {nullable: true});
          break;
        case 'real':
          table.columns.add(columnList[idx],sql.Real, {nullable: true}, {nullable: true});
          break;
        case 'numeric':
          // sql.Numeric ([precision], [scale])
          table.columns.add(columnList[idx],sql.Numeric(length,dataType.scale), {nullable: true});
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
		  switch (this.spatialFormat) {
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
		  switch (this.spatialFormat) {
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

  async generateStatementCache (executeDDL, vendor, database) {
      
	const args = { 
	        inputs: [{
			  name: 'TARGET_DATABASE', type: sql.VARCHAR,   value: this.targetSchema
			},{
              name: 'SPATIAL_FORMAT',  type: sql.NVARCHAR,  value: this.spatialFormat
	        },{
              name: 'METADATA',         type: sql.NVARCHAR, value: JSON.stringify({metadata : this.metadata})
			},{ 
			  name: 'DB_COLLATION',     type: sql.NVARCHAR, value: this.dbi.DB_COLLATION
			}]
	      }
      				
    let results = await this.dbi.execute('master.dbo.sp_GENERATE_SQL',args,'SQL_STATEMENTS')
    results = results.output[Object.keys(results.output)[0]]
    const statementCache = JSON.parse(results)
    const tables = Object.keys(this.metadata); 
    const ddlStatements = tables.map((table,idx) => {
      const tableMetadata = this.metadata[table];
      const tableName = tableMetadata.tableName;
      statementCache[tableName] = JSON.parse(statementCache[tableName])
      const tableInfo = statementCache[tableName]; 

      tableInfo.columnNames = tableMetadata.columnNames
      // msssql requires type and length information when generating a prepared statement.
      const dataTypes  = YadamuLibrary.decomposeDataTypes(tableInfo.targetDataTypes)
      
      tableInfo._BATCH_SIZE     = this.dbi.BATCH_SIZE
      tableInfo._COMMIT_COUNT   = this.dbi.COMMIT_COUNT
      tableInfo._SPATIAL_FORMAT = this.spatialFormat
       
      // Create table before attempting to Prepare Statement..
      tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + "\nVALUES (";
      this.metadata[table].columnNames.forEach((column,idx) => {
        switch(tableInfo.targetDataTypes[idx]) {
          case 'image':
		    // Upload images as VarBinary(MAX). Convert data to Buffer. This enables bulk upload and avoids Collation issues...
            tableInfo.dml = tableInfo.dml + 'convert(image,@C' + idx + ')' + ','
            break;
          case "xml":
            tableInfo.dml = tableInfo.dml + 'convert(XML,@C' + idx + ',1)' + ','
            break;
          case "geography":
            switch (this.spatialFormat) {
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
            switch (this.spatialFormat) {
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
          default: 
             tableInfo.dml = tableInfo.dml + '@C' + idx+ ','
        }
      })
      tableInfo.dml = tableInfo.dml.slice(0,-1) + ")";
      tableInfo.bulkSupported = this.bulkSupported(dataTypes);
      try {
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
        return tableInfo.ddl;
      } catch (e) {
        this.yadamuLogger.logException([`${this.constructor.name}`],e)
        this.yadamuLogger.writeDirect(`${tableInfo.ddl}`)
      } 
    });
    
    if (executeDDL === true) {
      await this.dbi.executeDDL(ddlStatements);
    }
    return statementCache;
  }
}

module.exports = StatementGenerator

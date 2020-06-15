"use strict";

const sql = require('mssql');

const Yadamu = require('../../common/yadamu.js');

class StatementGenerator {
  
  constructor(dbi, targetSchema, metadata,  spatialFormat, batchSize, commitSize, status, yadamuLogger) {
    this.dbi = dbi;
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.spatialFormat = spatialFormat
    this.batchSize = batchSize
    this.commitSize = commitSize;
    
    this.status = status;
    this.yadamuLogger = yadamuLogger;
  }
  
  bulkSupported(dataTypes) {
    
    let supported = true;
    dataTypes.forEach((dataType,idx) => {
      switch (dataType.type) {
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
    
    const columns = JSON.parse('[' +  columnList + ']')
  
    dataTypes.forEach((dataType,idx) => {
      switch (dataType.type) {
        case 'bit':
          table.columns.add(columns[idx],sql.Bit);
          break;
        case 'bigint':
          table.columns.add(columns[idx],sql.BigInt, {nullable: true});
          break;
        case 'float':
          table.columns.add(columns[idx],sql.Float, {nullable: true});
          break;
        case 'int':
          table.columns.add(columns[idx],sql.Int, {nullable: true});
          break;
        case 'money':
          table.columns.add(columns[idx],sql.Decimal(19,4), {nullable: true});
          // table.columns.add(columns[idx],sql.Money, {nullable: true});
          break
        case 'decimal':
          // sql.Decimal ([precision], [scale])
          table.columns.add(columns[idx],sql.Decimal(dataType.length,dataType.scale), {nullable: true});
          break;
        case 'smallint':
          table.columns.add(columns[idx],sql.SmallInt, {nullable: true});
          break;
        case 'smallmoney':
          table.columns.add(columns[idx],sql.Decimal(10,4), {nullable: true});
          // table.columns.add(columns[idx],sql.SmallMoney, {nullable: true});
          break;
        case 'real':
          table.columns.add(columns[idx],sql.Real, {nullable: true}, {nullable: true});
          break;
        case 'numeric':
          // sql.Numeric ([precision], [scale])
          table.columns.add(columns[idx],sql.Numeric(dataType.length,dataType.scale), {nullable: true});
          break;
        case 'tinyint':
          table.columns.add(columns[idx],sql.TinyInt, {nullable: true});
          break;
        case 'char':
          table.columns.add(columns[idx],sql.Char(dataType.length), {nullable: true});
          break;
        case 'nchar':
          table.columns.add(columns[idx],sql.NChar(dataType.length), {nullable: true});
          break;
        case 'text':
          table.columns.add(columns[idx],sql.Text, {nullable: true});
          break;
        case 'ntext':
          table.columns.add(columns[idx],sql.NText, {nullable: true});
          break;
        case 'varchar':
          table.columns.add(columns[idx],sql.VarChar(dataType.length), {nullable: true});
          break;
        case 'nvarchar':
          table.columns.add(columns[idx],sql.NVarChar(dataType.length), {nullable: true});
          break;
        case 'json':
          table.columns.add(columns[idx],sql.NVarChar(sql.MAX), {nullable: true});
          break;
        case 'xml':
          // Added to Unsupported
          // Invalid column data type for bulk load
          table.columns.add(columns[idx],sql.Xml, {nullable: true});
          break;
        case 'time':
          // sql.Time ([scale])
          // Binding as sql.Time must supply values as type Date. 
          // table.columns.add(columns[idx],sql.Time(dataType.length), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columns[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'date':
          // Binding as sql.Date must supply values as type Date. 
          // table.columns.add(columns[idx],sql.Date, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columns[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'datetime':
          // Binding as sql.DateTime must supply values as type Date. 
          // table.columns.add(columns[idx],sql.DateTime, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columns[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'datetime2':
          // sql.DateTime2 ([scale]
          // Binding as sql.DateTime2 must supply values as type Date. 
          // table.columns.add(columns[idx],sql.DateTime2(), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columns[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'datetimeoffset':
          // sql.DateTimeOffset ([scale])
          // Binding as sql.DateTime2 must supply values as type Date. 
          // table.columns.add(columns[idx],sql.DateTimeOffset(dataType.length), {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columns[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'smalldatetime':
          // Binding as sql.SamllDateTime must supply values as type Date. 
          // table.columns.add(columns[idx],sql.SmallDateTime, {nullable: true});
          // Use String to avoid possible loss of precision
          table.columns.add(columns[idx],sql.VarChar(32), {nullable: true});
          break;
        case 'uniqueidentifier':
          // table.columns.add(columns[idx],sql.UniqueIdentifier, {nullable: true});
          // TypeError: parameter.type.validate is not a function
          table.columns.add(columns[idx],sql.Char(36), {nullable: true});
          break;
        case 'variant':
          table.columns.add(columns[idx],sql.Variant, {nullable: true});
          break;
        case 'binary':
          table.columns.add(columns[idx],sql.Binary, {nullable: true});
          break;
        case 'varbinary':
          // sql.VarBinary ([length])
           table.columns.add(columns[idx],sql.VarBinary(dataType.length), {nullable: true});
          break;
        case 'image':
  	      // Upload images as VarBinary(MAX). Convert data to Buffer. This enables bulk upload and avoids Collation issues...
          // table.columns.add(columns[idx],sql.Image, {nullable: true});
          table.columns.add(columns[idx],sql.VarBinary(sql.MAX), {nullable: true});
          break;
        case 'udt':
          table.columns.add(columns[idx],sql.UDT, {nullable: true});
          break;
        case 'geography':
          // Added to Unsupported
          // TypeError: parameter.type.validate is not a function
          // table.columns.add(columns[idx],sql.Geography, {nullable: true});
  	      // Upload geography as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer. This enables bulk upload.
		  switch (this.spatialFormat) {
			case "WKB":
            case "EWKB":
              table.columns.add(columns[idx],sql.VarBinary(sql.MAX), {nullable: true});
			  break;
			default:
		      table.columns.add(columns[idx],sql.VarChar(sql.MAX), {nullable: true});
		  }
          break;
        case 'geometry':
          // Added to Unsupported
          // TypeError: parameter.type.validate is not a function
          // table.columns.add(columns[idx],sql.Geometry, {nullable: true});
  	      // Upload geometry as VarBinary(MAX) or VarChar(MAX). Convert data to Buffer. This enables bulk upload.
		  switch (this.spatialFormat) {
			case "WKB":
            case "EWKB":
              table.columns.add(columns[idx],sql.VarBinary(sql.MAX), {nullable: true});
			  break;
			default:
		      table.columns.add(columns[idx],sql.VarChar(sql.MAX), {nullable: true});
		  }
          break;
        case 'hierarchyid':
          table.columns.add(columns[idx],sql.VarChar(4000),{nullable: true});
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
			  name: 'DB_COLLATiON',     type: sql.NVARCHAR, value: this.dbi.dbCollation
			}]
	      }
				
    let results = await this.dbi.execute('master.dbo.sp_GENERATE_SQL',args,'SQL_STATEMENTS')
    results = results.output[Object.keys(results.output)[0]]
    const statementCache = JSON.parse(results)
    const tables = Object.keys(this.metadata); 
    const ddlStatements = tables.map((table,idx) => {
      const tableName = this.metadata[table].tableName;
      statementCache[tableName] = JSON.parse(statementCache[tableName] );
      const tableInfo = statementCache[tableName];
	  tableInfo.dataTypes = this.dbi.decomposeDataTypes(tableInfo.targetDataTypes)
      tableInfo.batchSize =  this.batchSize;
      tableInfo.commitSize = this.commitSize;
	  tableInfo.spatialFormat = this.spatialFormat
      // Create table before attempting to Prepare Statement..
      tableInfo.dml = tableInfo.dml.substring(0,tableInfo.dml.indexOf(') select')+1) + "\nVALUES (";
      this.metadata[table].columns.split(',').forEach((column,idx) => {
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
      tableInfo.bulkSupported = this.bulkSupported(tableInfo.dataTypes);
      try {
        if (tableInfo.bulkSupported) {
          tableInfo.bulkOperation = this.createBulkOperation(database, tableName, this.metadata[table].columns, tableInfo.dataTypes);
        }
        else {
          // Place holder for caching rows.
          tableInfo.bulkOperation = new sql.Table()                                            
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

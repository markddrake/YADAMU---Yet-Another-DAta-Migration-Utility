
import path                     from 'path';
import crypto                   from 'crypto';

import YadamuLibrary            from '../../lib/yadamuLibrary.js';
import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

class VerticaStatementGenerator extends YadamuStatementGenerator {


  get TABLE_LOB_COUNT()         { return this._TABLE_LOB_COUNT }
  set TABLE_LOB_COUNT(v)        { this._TABLE_LOB_COUNT = v }
  
  set TABLE_UNUSED_BYTES(v)     { this._TABLE_UNUSED_BYTES = v }
  
  get TABLE_LOB_LIMIT()          { 
    const  allocatedSize = Math.floor(this._TABLE_UNUSED_BYTES / (this._TABLE_LOB_COUNT || 1)) 
    return allocatedSize < this.dbi.DATA_TYPES.LOB_LENGTH ? allocatedSize : this.dbi.DATA_TYPES.LOB_LENGTH
  }
  
  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }

  getMappedDataType(dataType,sizeConstraint) {
 
    if (this.SOURCE_VENDOR === this.dbi.DATABASE_VENDOR) {
      switch (dataType.toUpperCase()) {
        case 'JSON':                                                                  return this.dbi.DATA_TYPES.JSON_TYPE;
        case 'XML':                                                                   return this.dbi.DATA_TYPES.XML_TYPE;
	    default:                                                                      return dataType
	   }
      return dataType
	}
    const mappedDataType = super.getMappedDataType(dataType,sizeConstraint)
    const length = parseInt(sizeConstraint)
	
	switch (mappedDataType) {
	  case this.dbi.DATA_TYPES.BLOB_TYPE:
  		switch (true) {
          case (isNaN(length)) :                             return this.dbi.DATA_TYPES.MAX_BLOB_TYPE
          case (length === undefined) :                      return this.dbi.DATA_TYPES.MAX_BLOB_TYPE
          case (length === -1) :                             return this.dbi.DATA_TYPES.MAX_BLOB_TYPE
          case (length > this.dbi.DATA_TYPES.LOB_LENGTH) :   return this.dbi.DATA_TYPES.MAX_BLOB_TYPE
		  default:                                           return this.dbi.DATA_TYPES
		}
      case this.dbi.DATA_TYPES.CLOB_TYPE:
	    const length = parseInt(sizeConstraint)
		switch (true) {
          case (isNaN(length)) :                             return this.dbi.DATA_TYPES.MAX_CLOB_TYPE
          case (length === undefined) :                      return this.dbi.DATA_TYPES.MAX_CLOB_TYPE
          case (length === -1) :                             return this.dbi.DATA_TYPES.MAX_CLOB_TYPE
          case (length > this.dbi.DATA_TYPES.LOB_LENGTH) :      return this.dbi.DATA_TYPES.MAX_CLOB_TYPE
		  default:                                           return mappedDataType
		}
      default:                                               return mappedDataType
	}
  }
  
  
  generateDDLStatement(schema,tableName,columnDefinitions,mappedDataTypes) {
	  
    return `create table if not exists "${schema}"."${tableName}"(\n  ${columnDefinitions.join(',')})`;
	
  }

  generateDMLStatement(schema,tableName,columnNames,insertOperators) {
	return `insert into "${schema}"."${tableName}" ("${columnNames.join('","')}")  values `
  }
  
  generateCopyStatement(schema,tableName,remotePath,copyColumnDefinitions) {
	return `copy "${schema}"."${tableName}" (${copyColumnDefinitions.join(',')}) from '${remotePath}' PARSER fcsvparser(type='rfc4180', header=false, trim=${this.dbi.COPY_TRIM_WHITEPSPACE===true}) NULL ''`
  }
  
  calculateFixedRowSize(mappedDataTypes,sizeConstraints,lobList) {
	  
    let bytesUsed = 128
	
    mappedDataTypes.forEach((mappedDataType,idx) => {

	  const typeDefinition = YadamuLibrary.decomposeDataType(mappedDataType)
	
      if (this.dbi.DATA_TYPES.LOB_TYPES.includes(typeDefinition.type)) {
        lobList.push(idx)
	  }
	  
      switch (true) {
        case this.dbi.DATA_TYPES.ONE_BYTE_TYPES.includes(mappedDataType):
          bytesUsed+=1;
          break;
        case this.dbi.DATA_TYPES.EIGHT_BYTE_TYPES.includes(mappedDataType):
          bytesUsed+=8
          break;
        case this.dbi.DATA_TYPES.isSpatial(mappedDataType):
          bytesUsed+=this.dbi.DATA_TYPES.DEFAULT_SPATIAL_LENGTH
          break;
		case (typeDefinition.hasOwnProperty('scale')):
          let precision = typeDefinition.length
		  do {
            precision = precision - 18
            bytesUsed+=8
          } while (precision > 0) 
          break
		case (typeDefinition.hasOwnProperty('length')) :
		  bytesUsed = bytesUsed + typeDefinition.length
          break;
        default:
	      bytesUsed+= parseInt(sizeConstraints[idx])
      }        
	  // console.log(typeDefinition,mappedDataType,bytesUsed)
    })
	return bytesUsed
  }

  adjustLobSizes(tableName,bytesUsed,lobList,columnDefinitions,sizeConstraints) {

    /*
    **
    ** LOB SIZE CALCULATION
    **  
    ** Calculate bytes used by Non LOB columns
    ** Calculate Number of Lob Colums
    ** Calculate Max LOb Size (Bytes Remaining/LOB Columns)00
    ** Filter out any LOB colunms smaller than MAX_LOB_SIZE
    ** Split remaining Bytes between remaining LOB Columns
    **
    */
	
	const lobBytes = lobList.map((idx) => { return parseInt(sizeConstraints[idx])}).reduce((prev,current) => {return prev + current},0)
	bytesUsed = bytesUsed  - lobBytes
    this.TABLE_UNUSED_BYTES = this.dbi.DATA_TYPES.ROW_SIZE - bytesUsed
	this.TABLE_LOB_COUNT = lobList.length

    // Filter Lob Columns that are smaller than the Lob Limit
	  
    lobList = lobList.flatMap((idx) => {
      const lobSize = parseInt(sizeConstraints[idx])
      if ((lobSize > 0) && (lobSize < this.TABLE_LOB_LIMIT)) {
        bytesUsed+=lobSize
        return []
      }
      return [idx]
    })

    // Redo calculations based on remaining LOBs

    this.TABLE_UNUSED_BYTES = this.dbi.DATA_TYPES.ROW_SIZE - bytesUsed 
    this.TABLE_LOB_COUNT = lobList.length
       
	if (this.TABLE_LOB_LIMIT < this.dbi.DATA_TYPES.LOB_LENGTH) {
      this.yadamuLogger.ddl([this.dbi.DATABASE_VENDOR,tableName],`LONG VARCHAR and LONG VARBINARY columns restricted to ${this.TABLE_LOB_LIMIT} bytes`);
      
      lobList.forEach((idx) => {
		columnDefinitions[idx] = columnDefinitions[idx].replace(sizeConstraints[idx],this.TABLE_LOB_LIMIT.toString())
        sizeConstraints[idx] = this.TABLE_LOB_LIMIT.toString()
		/*
        // Vertica 10.x Raises out of Memory if copy buffer is > 32M
        if (columnDefinitions[idx].indexOf(`" ${DataTypes.BLOB_TYPE}`) > 0) {
          copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER long varchar(${this.TABLE_LOB_LIMIT}), "${columnNames[idx]}" as YADAMU.LONG_HEX_TO_BINARY("YADAMU_COL_${column_suffix}")`  
          copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER long varchar(${this.TABLE_LOB_LIMIT > 16000000 ? DataTypes.LOB_LENGTH : (this.TABLE_LOB_LIMIT * 2)}), "${columnNames[idx]}" as YADAMU.LONG_HEX_TO_BINARY("YADAMU_COL_${column_suffix}")`  
        } 
        */		
     	// console.log(columnDefinitions[idx])
      })
    }
  }

  generateTableInfo(tableMetadata) {
	
    let insertMode = 'Copy';
	
	console.log(new Error().stack)
	
    const columnNames = tableMetadata.columnNames
	const sizeConstraints = tableMetadata.sizeConstraints
    
    const mappedDataTypes = []
	const insertOperators = []
	const jsonColumns = new Array(columnNames.length).fill(false);
	const xmlColumns = new Array(columnNames.length).fill(false);
	
    const copyColumnDefinitions = []
	
    const columnDefinitions = columnNames.map((columnName,idx) => {

   	  jsonColumns[idx] =  this.isJSON(tableMetadata.dataTypes[idx])
   	  xmlColumns[idx] =  this.isXML(tableMetadata.dataTypes[idx])
      let mappedDataType = (tableMetadata.source) ? tableMetadata.dataTypes[idx] : this.getMappedDataType(tableMetadata.dataTypes[idx],tableMetadata.sizeConstraints[idx])
	  console.log(tableMetadata.dataTypes[idx],mappedDataType,jsonColumns,xmlColumns)
	  // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,tableMetadata.vendor,tableMetadata.dataTypes[idx],tableMetadata.sizeConstraints[idx]],`Mapped to "${mappedDataType}".`)

	  const columnStorageClause = this.generateStorageClause(mappedDataType,tableMetadata.sizeConstraints[idx])
	  const storageDefinition = YadamuLibrary.decomposeDataType(columnStorageClause);
	  
	  if (!storageDefinition.length && this.dbi.DATA_TYPES.LOB_TYPES.includes(storageDefinition.type)) {
        storageDefinition.length = this.dbi.DATA_TYPES.LOB_LENGTH
	  }
	  
      if (storageDefinition.hasOwnProperty('length')) {
		 sizeConstraints[idx] = (storageDefinition.hasOwnProperty('scale')) ? `${storageDefinition.length},${storageDefinition.scale}` : storageDefinition.length.toString()
	  }
	  
	  const args = new Array(tableMetadata.columnNames.length).fill('?')
      
      const column_suffix = String(idx+1).padStart(3,"0");

	  let columnLengthBytes = storageDefinition.length
	  
      switch (storageDefinition.type) {
        // Vertica's VARCHAR Size Constraint is size in bytes. Adjust size from other vendors to accomodate multi-byte characters by applying user controllable Fudge Factor. What percentage of the content requires more than one byte to store.      
        // Disable byte length calculation for CHAR as this leads to issues related to blank padding.
		// case this.dbi.DATA_TYPES.CHAR
        case this.dbi.DATA_TYPES.VARCHAR_TYPE:
        case this.dbi.DATA_TYPES.CLOB_TYPE:
	      columnLengthBytes = this.SOURCE_VENDOR === this.dbi.DATABASE_VENDOR ? columnLengthBytes : Math.ceil(columnLengthBytes * this.dbi.BYTE_TO_CHAR_RATIO);
          if (columnLengthBytes > this.dbi.DATA_TYPES.VARCHAR_LENGTH) {
            mappedDataType = this.dbi.DATA_TYPES.CLOB_TYPE
          }
          if (columnLengthBytes > this.dbi.DATA_TYPES.LOB_LENGTH) {
            columnLengthBytes = this.dbi.DATA_TYPES.LOB_LENGTH;
          }
		  // console.log('VARCHAR',storageDefinition.length,'==>',columnLengthBytes)
		  sizeConstraints[idx] = columnLengthBytes?.toString() || ''
		  copyColumnDefinitions[idx] =`"${columnName}"`
		  break
        case this.dbi.DATA_TYPES.BINARY_TYPE:
        case this.dbi.DATA_TYPES.VARBINARY_TYPE:
        case this.dbi.DATA_TYPES.BLOB_TYPE:
          columnLengthBytes = ( isNaN(columnLengthBytes) || ((columnLengthBytes < 1) || (columnLengthBytes > this.dbi.DATA_TYPES.LOB_LENGTH))) ? this.dbi.DATA_TYPES.LOB_LENGTH : columnLengthBytes
          let hexLengthBytes = columnLengthBytes * 2
          hexLengthBytes = ((hexLengthBytes < 2) || (hexLengthBytes > this.dbi.DATA_TYPES.LOB_LENGTH)) ? this.dbi.DATA_TYPES.LOB_LENGTH : hexLengthBytes
          switch (true) {
             case (columnLengthBytes > this.dbi.DATA_TYPES.LOB_LENGTH) :
               // LONG VARBINARY
               // copyColumnDefinitions[idx] = `"${columnName}" FORMAT 'HEX'`
               copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER long varchar(${hexLengthBytes}), "${columnName}" as YADAMU.LONG_HEX_TO_BINARY("YADAMU_COL_${column_suffix}")`
               break;
             case (hexLengthBytes > this.dbi.DATA_TYPES.VARCHAR_LENGTH) :
               // VARBINARY > 32500 < 65000 - Read HEX value into LONG VARCHAR. Cast result to VARBINARY
               // copyColumnDefinitions[idx] = `"${columnName}" FORMAT 'HEX'`
               copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER long varchar(${hexLengthBytes}), "${columnName}" as CAST(YADAMU.LONG_HEX_TO_BINARY("YADAMU_COL_${column_suffix}") as ${mappedDataType}(${columnLengthBytes}))`
               break;
             default:   
               copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER varchar(${hexLengthBytes}), "${columnName}" as HEX_TO_BINARY("YADAMU_COL_${column_suffix}")`
               // copyColumnDefinitions[idx] = `"${columnName}" FORMAT 'HEX'`
          }
          insertOperators[idx] = {
            prefix  : 'X'
          , suffix  : `::${storageDefinition.type.toUpperCase()}(${columnLengthBytes})`
          }
          break
        case 'CIRCLE':
          if (this.dbi.INBOUND_CIRCLE_FORMAT === 'CIRCLE') {
            copyColumnDefinitions.push(`"${columnName}"`)
            break;
          }
        case this.dbi.DATA_TYPES.GEOMETRY_TYPE:
          switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
            case 'WKT':
            case 'EWKT':
            case 'GeoJSON':
              copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeomFromText("YADAMU_COL_${column_suffix}")`
              insertOperators[idx] = { 
                prefix  : '('
              , suffix  : ')'
              }
              break;
            case 'WKB':
            case 'EWKB':
              copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeomFromWKB(HEX_TO_BINARY("YADAMU_COL_${column_suffix}"))`
              insertOperators[idx] = { 
                prefix  : 'ST_GeomFromWKB(HEX_TO_BINARY('
              , suffix  : '))'
              }
              break;
            /*
            case 'GeoJSON':
              copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeomFromGeoJSON("YADAMU_COL_${column_suffix}")`
              insertOperators[idx] = { 
                prefix  : 'ST_GeomFromGeoJSON('
              , suffix  : ')'
              }
              break;
            */
          }
          break;
        case  this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:
          switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
            case 'WKT':
            case 'EWKT':
            case 'GeoJSON':
              copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeographyFromText("YADAMU_COL_${column_suffix}")`
              insertOperators[idx] = { 
                prefix  : 'ST_GeographyFromText('
              , suffix  : ')'
              }
              break;
            case 'WKB':
            case 'EWKB':
              copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeographyFromWKB(HEX_TO_BINARY("YADAMU_COL_${column_suffix}"))`
              insertOperators[idx] = { 
                prefix  : 'ST_GeographyFromWKB(HEX_TO_BINARY('
              , suffix  : '))'
              }
              break;
            /*
            case 'GeoJSON':
              copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeographyFromGeoJSON("YADAMU_COL_${column_suffix}")`
              insertOperators[idx] = { 
                prefix  : 'ST_GeographyFromGeoJSON('
              , suffix  : ')'
              }
              break;
            */
          }
          break;
        case this.dbi.DATA_TYPES.TIME_TYPE:
          if (tableMetadata.hasOwnProperty('dataFile')) {
            copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast(TO_TIMESTAMP("YADAMU_COL_${column_suffix}",'YYYY-MM-DD"T"HH24:MI:SS.US') as TIME)`
          }
          else {
			switch (this.SOURCE_VENDOR) {
              case "Postgres":
              case "MSSQLSERVER":
              case "MySQL":
              case "MariaDB":
              case "Vertica":
                copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast("YADAMU_COL_${column_suffix}" as TIME)`
                break;
              default:
                copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast(TO_TIMESTAMP("YADAMU_COL_${column_suffix}",'YYYY-MM-DD"T"HH24:MI:SS.US') as TIME)`
            }
          }
          insertOperators[idx] = { 
            prefix  : 'cast('
          , suffix  : ' as TIME)'
          }
          break;
        case this.dbi.DATA_TYPES.TIME_TZ_TYPE:
          copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast("YADAMU_COL_${column_suffix}" as TIME WITH TIME ZONE)`
          insertOperators[idx] = { 
            prefix  : 'cast('
          , suffix  : ' as TIME WITH TIME ZONE)'
          }
          break;
        case this.dbi.DATA_TYPES.INTERVAL_DAY_TO_SECOND_TYPE:
          copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(64), "${columnName}" as CAST("YADAMU_COL_${column_suffix}" AS INTERVAL DAY TO SECOND)`
          insertOperators[idx] = { 
            prefix  : 'cast('
          , suffix  : ' as INTERVAL DAY TO SECOND)'
          }
          break;
        case this.dbi.DATA_TYPES.INTERVAL_YEAR_TO_MONTH_TYPE:
          copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(64), "${columnName}" as CAST("YADAMU_COL_${column_suffix}" AS INTERVAL YEAR TO MONTH)`
          insertOperators[idx] = { 
            prefix  : 'cast('
          , suffix  : ' as INTERVAL YEAR TO MONTH)'
          }
          break;
        case this.dbi.DATA_TYPES.UUID_TYPE:
          copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as CAST("YADAMU_COL_${column_suffix}" AS UUID)`
          insertOperators[idx] = null
          break;
        default:
          copyColumnDefinitions[idx] =`"${columnName}"`
          insertOperators[idx] = null
		  
      }
	  // Generate the final storage clause based on the any adjustments made to the column length
	  let checkConstraint = undefined
	  if (jsonColumns[idx]) {
        checkConstraint = `check(YADAMU.IS_JSON("${columnName}"))`
      }
	  if (xmlColumns[idx]) {
        checkConstraint = `check(YADAMU.IS_XML("${columnName}"))`
      }
	  console.log(mappedDataType)
      mappedDataTypes.push(mappedDataType)      
      return `"${columnName}" ${this.generateStorageClause(mappedDataType,sizeConstraints[idx])}${checkConstraint ? ` ${checkConstraint}` : ''}`
    })
	
	// Check and Adjust Row Size
	console.log(columnDefinitions)
    const lobList = []
	let bytesUsed = this.calculateFixedRowSize(mappedDataTypes,sizeConstraints,lobList)
	if (bytesUsed > this.dbi.DATA_TYPES.ROW_SIZE) {
	  this.adjustLobSizes(tableMetadata.tableName,bytesUsed,lobList,columnDefinitions,sizeConstraints)
	}
	
    // All remote paths must use POSIX/Linux seperators (Vertica does not run on MS-Windows)

	const stagingFileName =  `YST-${crypto.randomBytes(16).toString("hex").toUpperCase()}`;
	const stagingFilePath =  path.join(this.dbi.LOCAL_STAGING_AREA,stagingFileName)
	const localPath       =  path.resolve(stagingFilePath); 
	let remotePath        =  path.join(this.dbi.REMOTE_STAGING_AREA,stagingFileName).split(path.sep).join(path.posix.sep)
	
    const stagingFileName =  `YST-${crypto.randomBytes(16).toString("hex").toUpperCase()}`;
    const stagingFilePath =  path.join(this.dbi.LOCAL_STAGING_AREA,stagingFileName)
    const localPath       =  path.resolve(stagingFilePath); 
    const remotePath      =  tableMetadata.dataFile || path.join(this.dbi.REMOTE_STAGING_AREA,stagingFileName)
    
    const tableInfo =  { 
      ddl             : this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,mappedDataTypes)
    , dml             : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,columnNames,insertOperators)
    , mergeout        : `select do_tm_task('mergeout','${this.targetSchema}.${tableMetadata.tableName}')`
    , stagingFileName : stagingFileName
    , localPath       : localPath
    , remotePath      : remotePath
    , columnNames     : tableMetadata.columnNames
    , targetDataTypes : mappedDataTypes
	, jsonColumns     : jsonColumns
	, xmlColumns      : xmlColumns
    , sizeConstraints : sizeConstraints
    , insertOperators : insertOperators
    , insertMode      : insertMode
    , _BATCH_SIZE     : this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT : this.dbi.INBOUND_SPATIAL_FORMAT
    , _SCHEMA_NAME    : this.targetSchema
    , _TABLE_NAME     : tableMetadata.tableName
    }
	
    if (Array.isArray(tableMetadata.dataFile)) {
      tableInfo.copy = tableMetadata.dataFile.map((remotePath,idx) => {
        remotePath = remotePath.split(path.sep).join(path.posix.sep)
        return  {
          dml             : this.generateCopyStatement(this.targetSchema,tableMetadata.tableName,remotePath,copyColumnDefinitions)
        , partitionCount  : tableMetadata.partitionCount
        , partitionID     : idx+1
        }
      })
    }
    else {
       remotePath = tableMetadata.dataFile ? tableMetadata.dataFile.split(path.sep).join(path.posix.sep) : remotePath
	   tableInfo.copy = {
        dml         :  this.generateCopyStatement(this.targetSchema,tableMetadata.tableName,remotePath,copyColumnDefinitions)
      }
    }

	return tableInfo
  }

}

export { VerticaStatementGenerator as default }

/*
**
mapForeignDataType(vendor, dataType, dataTypeLength, dataTypeScale) {
    switch (vendor) {
       case "Vertica": 
         switch (dataType.toUpperCase()) {
           case 'JSON':                                                                  return VerticaStatementGenerator.JSON_TYPE;
           case 'XML':                                                                   return VerticaStatementGenerator.XML_TYPE;
           default:                                                                      return dataType.toLowerCase();
         }
         break;
       case 'Oracle':
         switch (dataType.toUpperCase()) {
           case 'VARCHAR2':                                                              return 'varchar';
           case 'NVARCHAR2':                                                             return 'varchar';
           case 'NUMBER':                                                                return dataTypeLength === undefined ? VerticaStatementGenerator.ORACLE_NUMERIC_TYPE : 'decimal';
           case 'BINARY_FLOAT':                                                          return 'float';
           case 'BINARY_DOUBLE':                                                         return 'float';
           case 'CLOB':                                                                  return VerticaStatementGenerator.CLOB_TYPE;
           case 'BLOB':                                                                  return VerticaStatementGenerator.BLOB_TYPE;
           case 'NCLOB':                                                                 return VerticaStatementGenerator.CLOB_TYPE;
           case 'XMLTYPE':                                                               return VerticaStatementGenerator.XML_TYPE;
           case 'TIMESTAMP':                                                             return dataTypeLength > 6 ? 'datetime' : 'datetime';
           case 'BFILE':                                                                 return VerticaStatementGenerator.BFILE_TYPE;
           case 'ROWID':                                                                 return VerticaStatementGenerator.ROWID_TYPE;
           case 'RAW':                                                                   return 'varbinary';
           case 'ANYDATA':                                                               return VerticaStatementGenerator.CLOB_TYPE;
           case 'JSON':                                                                  return VerticaStatementGenerator.JSON_TYPE;
           case '"MDSYS"."SDO_GEOMETRY"':                                                return 'geometry';
           case 'BOOLEAN':                                                               return 'boolean'
           default :
             switch (true) {
               case (dataType.indexOf('INTERVAL') > -1):
                 switch (true) {
                   case (dataType.indexOf('YEAR') > -1):                                 return 'interval year to month';
                   case (dataType.indexOf('DAY') > -1):                                  return 'interval day to second';
                   default:                                                              return 'interval year to month';
                 }                                                           
               case (dataType.indexOf('TIME ZONE') > -1):                                return 'datetime'; 
               case (dataType.indexOf('XMLTYPE') > -1):                                  return VerticaStatementGenerator.XML_TYPE;
               case (dataType.indexOf('.') > -1):                                        return VerticaStatementGenerator.CLOB_TYPE;
               default:                                                                  return dataType.toLowerCase();
             }
         }
         break;
       case 'MSSQLSERVER':
         switch (dataType.toLowerCase()) {
           case 'varchar':
             switch (true) {
               case (dataTypeLength === -1):                                             return VerticaStatementGenerator.CLOB_TYPE;
               case (dataTypeLength > VerticaStatementGenerator.LARGEST_VARCHAR_SIZE):          return VerticaStatementGenerator.CLOB_TYPE;
               default:                                                                  return 'varchar';
             }                                                                          
           case 'char':                                                                 
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return VerticaStatementGenerator.CLOB_TYPE;
               case (dataTypeLength > VerticaStatementGenerator.LARGEST_CHAR_SIZE):             return  tatementGenerator.CLOB_TYPE;
               default:                                                                  return 'char';
             }                                                                          
           case 'nvarchar':                                                             
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return VerticaStatementGenerator.CLOB_TYPE;
               case (dataTypeLength > VerticaStatementGenerator.LARGEST_VARCHAR_SIZE):          return VerticaStatementGenerator.CLOB_TYPE;
               default:                                                                  return 'varchar';
             }                                                                          
           case 'nchar':                                                                
             switch (true) {                                                            
               case (dataTypeLength === -1):                                             return VerticaStatementGenerator.CLOB_TYPE;
               case (dataTypeLength > VerticaStatementGenerator.LARGEST_CHAR_SIZE):             return VerticaStatementGenerator.CLOB_TYPE;
               default:                                                                  return 'char';
             }
             
           case 'text':                                                                  return VerticaStatementGenerator.CLOB_TYPE;                   
           case 'ntext':                                                                 return VerticaStatementGenerator.CLOB_TYPE;
           case 'binary':
             switch (true) {
               case (dataTypeLength === -1):                                             return VerticaStatementGenerator.BLOB_TYPE;
               case (dataTypeLength > DataTypes._SIZE):      return VerticaStatementGenerator.BLOB_TYPE;
               default:                                                                  return 'binary';
             }
           case 'varbinary':
             switch (true) {
               case (dataTypeLength === -1):                                             return VerticaStatementGenerator.BLOB_TYPE;
               case (dataTypeLength > VerticaStatementGenerator.LARGEST_VARBINARY_SIZE_SIZE):   return VerticaStatementGenerator.BLOB_TYPE;
               default:                                                                  return 'varbinary';
             }
           case 'image':                                                                 return VerticaStatementGenerator.BLOB_TYPE;
           case 'boolean':                                                               return 'boolean'
           case 'tinyint':                                                               return 'smallint';
           case 'mediumint':                                                             return 'int';
           case 'money':                                                                 return VerticaStatementGenerator.MSSQL_MONEY_TYPE
           case 'smallmoney':                                                            return VerticaStatementGenerator.MSSQL_SMALL_MONEY_TYPE;
           case 'real':                                                                  return 'float';
           case 'bit':                                                                   return 'boolean'
           case 'datetime':                                                              return 'datetime';
           case 'time':                                                                  return 'datetime';
           case 'datetime2':                                                             return 'datetime';
           case 'datetimeoffset':                                                        return 'datetime';
           case 'smalldate':                                                             return 'datetime';
           case 'geography':                                                             return 'geography';
           case 'geometry':                                                              return 'geometry';
           case 'hierarchyid':                                                           return VerticaStatementGenerator.HIERARCHY_TYPE
           case 'rowversion':                                                            return 'binary(8)';
           case 'uniqueidentifier':                                                      return 'uuid';
           case 'json':                                                                  return VerticaStatementGenerator.JSON_TYPE;
           case 'xml':                                                                   return VerticaStatementGenerator.XML_TYPE;
           default:                                                                      return dataType.toLowerCase();
         }
         break;
       case 'Postgres':    
         switch (dataType.toLowerCase()) {
           case 'character varying':     
             switch (true) {
               case (dataTypeLength === undefined):                                       return VerticaStatementGenerator.CLOB_TYPE;
               case (dataTypeLength > VerticaStatementGenerator.LARGEST_VARCHAR_SIZE):           return VerticaStatementGenerator.CLOB_TYPE;
               default:                                                                   return 'varchar';
             }
           case 'character':
             switch (true) {
               case (dataTypeLength === undefined):                                       return VerticaStatementGenerator.CLOB_TYPE;
               case (dataTypeLength > VerticaStatementGenerator.LARGEST_CHAR_SIZE):              return VerticaStatementGenerator.CLOB_TYPE;
               default:                                                                   return 'char';
             }
           case 'text':                                                                   return VerticaStatementGenerator.CLOB_TYPE;
           case 'char':                                                                   return VerticaStatementGenerator.PGSQL_SINGLE_CHAR_TYPE;
           case 'name':                                                                   return VerticaStatementGenerator.PGSQL_NAME_TYPE
           case 'bpchar':                     
             switch (true) {
               case (dataTypeLength === undefined):                                       return VerticaStatementGenerator.CLOB_TYPE;
               case (dataTypeLength > VerticaStatementGenerator.LARGEST_CHAR_SIZE):              return VerticaStatementGenerator.CLOB_TYPE;
               default:                                                                   return 'char';
             }
           case 'bytea':
             switch (true) {
               case (dataTypeLength === undefined):                                       return VerticaStatementGenerator.BLOB_TYPE;
               case (dataTypeLength > VerticaStatementGenerator.LARGEST_VARBINARY_SIZE_SIZE):    return VerticaStatementGenerator.BLOB_TYPE;
               default:                                                                   return 'varbinary';
             }
           case 'decimal':
           case 'numeric':                                                               return dataTypeLength === undefined ? VerticaStatementGenerator.PGSQL_NUMERIC_TYPE : 'decimal';
           case 'money':                                                                 return VerticaStatementGenerator.PGSQL_MONEY_TYPE
           case 'integer':                                                               return 'int';
           case 'real':                                                                  return 'float';
           case 'double precision':                                                      return 'float';
           case 'boolean':                                                               return 'boolean'
           case 'timestamp':                                                             return 'timestamp'
           case 'timestamp with time zone':                                              return 'timestamp with timezone'                                 
           case 'timestamp without time zone':                                           return 'timestamp'
           case 'time with time zone':                                                   return 'time with timezone'
           case 'time without time zone':                                                return 'time';
           case 'json':
           case 'jsonb':                                                                 return VerticaStatementGenerator.JSON_TYPE;
           case 'xml':                                                                   return VerticaStatementGenerator.XML_TYPE;
           case 'geography':                                                             return 'geography'; 
           case 'geometry':                                                             
           case 'point':                                                                 
           case 'lseg':                                                               
           case 'path':                                                                  
           case 'box':                                                                   
           case 'polygon':                                                               return 'geometry';     
           case 'circle':                                                                return this.dbi.INBOUND_CIRCLE_FORMAT === 'CIRCLE' ? VerticaStatementGenerator.JSON_TYPE : 'geometry';
           case 'line':                                                                  return VerticaStatementGenerator.JSON_TYPE;     
           case 'uuid':                                                                  return 'uuid'
           case 'bit':
           case 'bit varying':    
             switch (true) {
               case (dataTypeLength === undefined):                                      return VerticaStatementGenerator.LARGEST_VARCHAR_TYPE;
               case (dataTypeLength > VerticaStatementGenerator.LARGEST_VARCHAR_SIZE):          return VerticaStatementGenerator.CLOB_TYPE;
               default:                                                                  return 'varchar'
             }
           case 'cidr':
           case 'inet':                                                                  return VerticaStatementGenerator.INET_ADDR_TYPE
           case 'macaddr':                                                              
           case 'macaddr8':                                                              return VerticaStatementGenerator.MAC_ADDR_TYPE
           case 'int4range':                                                            
           case 'int8range':                                                            
           case 'numrange':                                                             
           case 'tsrange':                                                              
           case 'tstzrange':                                                            
           case 'daterange':                                                             return VerticaStatementGenerator.JSON_TYPE;
           case 'tsvector':                                                             
           case 'gtsvector':                                                             return VerticaStatementGenerator.JSON_TYPE;
           case 'tsquery':                                                               return VerticaStatementGenerator.LARGEST_VARCHAR_TYPE;
           case 'oid':                                                                  
           case 'regcollation':                                                         
           case 'regclass':                                                             
           case 'regconfig':                                                            
           case 'regdictionary':                                                        
           case 'regnamespace':                                                         
           case 'regoper':                                                              
           case 'regoperator':                                                          
           case 'regproc':                                                              
           case 'regprocedure':                                                         
           case 'regrole':                                                              
           case 'regtype':                                                              return VerticaStatementGenerator.PGSQL_IDENTIFIER
           case 'tid':                                                                  
           case 'xid':                                                                  
           case 'cid':                                                                  
           case 'txid_snapshot':                                                        return VerticaStatementGenerator.PGSQL_IDENTIFIER;
           case 'aclitem':                                                              
           case 'refcursor':                                                            return VerticaStatementGenerator.JSON_TYPE;
           default :
             switch (true) {
               case (dataType.indexOf('interval') > -1):
                 switch (true) {
                   case (dataType.indexOf('year') > -1):                                return 'interval day to second'
                   case (dataType.indexOf('day') > -1):                                 return 'interval year to month'
                   default:                                                             return VerticaStatementGenerator.C_UNTYPED_INTERVAL_TYPE
                 }                                                           
               default:                                                                 return dataType.toLowerCase();
             }
         }
         break
       case 'MySQL':
       case 'MariaDB':
         switch (dataType.toLowerCase()) {
           case 'boolean':                                                             return 'boolean'
           case 'double':                                                              return 'float';
           case 'mediumint':                                                           return 'int'
           case 'longtext':                 
           case 'mediumtext':               
           case 'text':                                                                return VerticaStatementGenerator.CLOB_TYPE;
           case 'year':                                                                return VerticaStatementGenerator.MYSQL_YEAR_TYPE;
           case 'longblob':                 
           case 'mediumblob':                 
           case 'blob':                                                                return VerticaStatementGenerator.BLOB_TYPE;
           case 'json':                                                                return VerticaStatementGenerator.JSON_TYPE;
           case 'set':                                                                 return VerticaStatementGenerator.JSON_TYPE;
           case 'enum':                                                                return VerticaStatementGenerator.ENUM_TYPE
           case 'point':
           case 'linestring':
           case 'polygon':
           case 'multipoint':
           case 'multilinestring':
           case 'multipolygon':
           case 'geometrycollection':
           case 'geomcollection':      
           case 'geometry':                                                            return 'geometry';
           case 'geography':                                                           return 'geography';
           default:                                                                    return dataType.toLowerCase();
         }
         break;
       case 'MongoDB':
         switch (dataType) {
           case "string":
             switch (true) {
               case (dataTypeLength === undefined):                                    return VerticaStatementGenerator.CLOB_TYPE;
               case (dataTypeLength > VerticaStatementGenerator.LARGEST_VARCHAR_SIZE):        return VerticaStatementGenerator.CLOB_TYPE;
               default:                                                                return 'varchar';
             }
           case "int":                                                                 return 'int';
           case "long":                                                                return 'bigint';
           case "double":                                                              return 'float';
           case "decimal":                                                             return VerticaStatementGenerator.MONGO_DECIMAL_TYPE;
           case "binData":                                                             return VerticaStatementGenerator.BLOB_TYPE;
           case "bool":                                                                return 'boolean';
           case "date":                                                                return 'datetime';
           case "timestamp":                                                           return 'datetime';
           case "objectId":                                                            return VerticaStatementGenerator.MONGO_OBJECT_ID
           case "json":                                                            
           case "object":                                                            
           case "array":                                                               return VerticaStatementGenerator.JSON_TYPE;
           case "null":                                                                return VerticaStatementGenerator.MONGO_UNKNOWN_TYPE
           case "regex":                                                               return VerticaStatementGenerator.MONGO_REGEX_TYPE
           case "javascript":                                                          return VerticaStatementGenerator.CLOB_TYPE;
           case "javascriptWithScope":                                                 return VerticaStatementGenerator.CLOB_TYPE;
           case "minkey":                                                            
           case "maxkey":                                                              return VerticaStatementGenerator.JSON_TYPE;
           case "undefined":                                                         
           case 'dbPointer':                                                         
           case 'function':                                                          
           case 'symbol':                                                              return VerticaStatementGenerator.JSON_TYPE;
           default:                                                                    return dataType.toLowerCase();
         }
         break;
       case 'SNOWFLAKE':
         switch (dataType.toLowerCase()) {
           case "number":                                                              return 'decimal';
           case "float":                                                               return 'float';
           case "geography":                                                           return 'geography';
           case "text":                                                                return dataTypeLength > VerticaStatementGenerator.LARGEST_VARCHAR_SIZE ? VerticaStatementGenerator.CLOB_TYPE: 'varchar'; 
           case "binary":                                                              return dataTypeLength > VerticaStatementGenerator.LARGEST_VARBINARY_SIZE ? VerticaStatementGenerator.BLOB_TYPE : 'varbinary'; 
           case 'json':                                                                return VerticaStatementGenerator.JSON_TYPE;
           case "xml":                                                                 return VerticaStatementGenerator.XML_TYPE
           case "variant":                                                             return VerticaStatementGenerator.BLOB_TYPE;
           case "timestamp_ltz":                                                    
           case "timestamp_ntz":                                                       return 'datetime'; 
           default:
             return dataType.toLowerCase();
         }
       default :
         return dataType.toLowerCase();
    }  
  }
**
*/
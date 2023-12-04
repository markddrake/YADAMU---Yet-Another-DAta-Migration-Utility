
import path                     from 'path';
import crypto                   from 'crypto';

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

class VerticaStatementGenerator extends YadamuStatementGenerator {

  get TABLE_LOB_COUNT()         { return this._TABLE_LOB_COUNT }
  set TABLE_LOB_COUNT(v)        { this._TABLE_LOB_COUNT = v }
  
  //  get MAX_POOL_USAGE()          { return 55*1024*1024 }
  
  set TABLE_UNUSED_BYTES(v)     { this._TABLE_UNUSED_BYTES = v }
  
  get GENERAL_POOL_LIMIT()      { return this._GENERAL_POOL_LIMIT }
  set GENERAL_POOL_LIMIT(v)     { this._GENERAL_POOL_LIMIT = v }
  
  get TABLE_LOB_LIMIT()          { 
    const  allocatedSize = Math.floor(this._TABLE_UNUSED_BYTES / (this._TABLE_LOB_COUNT || 1)) 
    return allocatedSize < this.dbi.DATA_TYPES.LOB_LENGTH ? allocatedSize:  this.dbi.DATA_TYPES.LOB_LENGTH
  }
  
  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }
  
  async init() {
    // Set it to the value of the resolved promise..
    await super.init();
	const results = await this.dbi.executeSQL(`SELECT min(memory_size_kb) FROM resource_pool_status WHERE pool_name='general'`)
	this.GENERAL_POOL_LIMIT = results.rows[0][0]
  }  
  
  generateDDLStatement(schema,tableName,columnDefinitions,targetDataTypes) {
	  
    return `create table if not exists "${schema}"."${tableName}"(\n  ${columnDefinitions.join(',')})`;
	
  }

  generateDMLStatement(schema,tableName,columnNames,insertOperators) {
	return `insert into "${schema}"."${tableName}" ("${columnNames.join('","')}")  values `
  }
  
  generateCopyStatement(schema,tableName,remotePath,copyColumnDefinitions) {
	return `copy "${schema}"."${tableName}" (${copyColumnDefinitions.join(',')}) from '${remotePath}' PARSER fcsvparser(type='rfc4180', header=false, trim=${this.dbi.COPY_TRIM_WHITEPSPACE===true}) NULL ''`
  }
  
    
  generateCopyOperation(tableMetadata,remotePath,copyColumnDefinitions) {
	
    // Remote Path is the path to be used if data is going to be copied to a staging table by the vertica driver. 
	
    // Partitioned Tables need one entry per partition 

	if (Array.isArray(tableMetadata.dataFile)) {
      return tableMetadata.dataFile.map((dataFile,idx) => {
        remotePath = dataFile.split(path.sep).join(path.posix.sep)
        return  {
          dml:              this.generateCopyStatement(this.targetSchema,tableMetadata.tableName,remotePath,copyColumnDefinitions)
        , partitionCount:   tableMetadata.partitionCount
        , partitionID:      idx+1
        }
      })
    }
    else {
      remotePath = tableMetadata.dataFile ? tableMetadata.dataFile.split(path.sep).join(path.posix.sep):  remotePath
	  return {
        dml:                this.generateCopyStatement(this.targetSchema,tableMetadata.tableName,remotePath,copyColumnDefinitions)
      }
    }
  }
	
  calculateFixedRowSize(columnDataTypes,sizeConstraints,lobList) {
	  
    let bytesUsed = 128
    columnDataTypes.forEach((columnDataType,idx) => {

	  const typeDefinition = YadamuDataTypes.decomposeDataType(columnDataType)
	 
      if (this.dbi.DATA_TYPES.LOB_TYPES.includes(typeDefinition.type)) {
        lobList.push(idx)
	  }
	  
      switch (true) {
        case this.dbi.DATA_TYPES.ONE_BYTE_TYPES.includes(columnDataType):
          bytesUsed+=1;
          break;
        case this.dbi.DATA_TYPES.EIGHT_BYTE_TYPES.includes(columnDataType):
          bytesUsed+=8
          break;
        case YadamuDataTypes.isSpatial(columnDataType):
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
		  bytesUsed += typeDefinition.length
          break;
		case (columnDataType === this.dbi.DATA_TYPES.UUID_TYPE):
		  bytesUsed += 16
          break;
        default:
	      bytesUsed+= sizeConstraints[idx][0]
      }        
	  // console.log(typeDefinition,columnDataType,bytesUsed)
    })
	
	return bytesUsed
  }

  adjustLobSizes(tableName,bytesUsed,lobList,columnDefinitions,copyColumnDefinitions,sizeConstraints) {

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
	
	const lobBytes = lobList.map((idx) => { return sizeConstraints[idx][0]}).reduce((prev,current) => {return prev + current},0)
	bytesUsed = bytesUsed  - lobBytes
    this.TABLE_UNUSED_BYTES = this.dbi.DATA_TYPES.ROW_SIZE - bytesUsed
	this.TABLE_LOB_COUNT = lobList.length

    // Filter Lob Columns that are smaller than the Lob Limit
	  
    lobList = lobList.flatMap((idx) => {
      const lobSize = sizeConstraints[idx][0]
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
      this.LOGGER.ddl([this.dbi.DATABASE_VENDOR,tableName],`LONG VARCHAR and LONG VARBINARY columns restricted to ${this.TABLE_LOB_LIMIT} bytes`);
      
      lobList.forEach((idx) => {
        const column_suffix = String(idx+1).padStart(3,"0");
		columnDefinitions[idx] = columnDefinitions[idx].replace(sizeConstraints[idx][0],this.TABLE_LOB_LIMIT)
        sizeConstraints[idx][0] = this.TABLE_LOB_LIMIT
      })
    }
  }

  generateTableInfo(tableMetadata) {
	
    let insertMode = 'Copy';
    this.SPATIAL_FORMAT = this.getSpatialFormat(tableMetadata)

	// console.log(tableMetadata)
    
	const insertOperators       = []
    const copyColumnDefinitions = []
	const sizeConstraints       = [...tableMetadata.sizeConstraints]
	const args                  = new Array(tableMetadata.columnNames.length).fill('?')
    
	const targetDataTypes = this.getTargetDataTypes(tableMetadata)
	const columnDataTypes = [...targetDataTypes]

    let poolUsage = 0;
	  
	const columnDefinitions = targetDataTypes.map((targetDataType,idx) => {
		
	  const columnName = tableMetadata.columnNames[idx]
      const column_suffix = String(idx+1).padStart(3,"0");
	  
	  let checkConstraint = ''
	  
	  switch (targetDataType) {
        // Disable byte length adjustment for CHAR as this leads to issues related to blank padding.
     	// case this.dbi.DATA_TYPES.CHAR_TYPE:
     	case this.dbi.DATA_TYPES.VARCHAR_TYPE:
           // Vertica's CHAR/VARCHAR Size Constraint is size in bytes. Adjust size from other vendors to accomodate multi-byte characters by applying user controllable Fudge Factor. What percentage of the content requires more than one byte to store.      
		   sizeConstraints[idx][0] = (this.dbi.DATABASE_VENDOR === this.SOURCE_VENDOR) ? sizeConstraints[idx][0] : Math.ceil(sizeConstraints[idx][0] * this.dbi.BYTE_TO_CHAR_RATIO)
		   if (sizeConstraints[idx][0] > this.dbi.DATA_TYPES.VARCHAR_LENGTH) {
		     columnDataTypes[idx] = this.dbi.DATA_TYPES.CLOB_TYPE
	       }			 
		   break
		 case this.dbi.DATA_TYPES.XML_TYPE:
		   columnDataTypes[idx] = this.dbi.DATA_TYPES.storageOptions.XML_TYPE
		   sizeConstraints[idx][0] = (this.dbi.DATABASE_VENDOR === this.SOURCE_VENDOR) ? sizeConstraints[idx][0] : this.dbi.DATA_TYPES.LOB_LENGTH
           checkConstraint = `check(YADAMU.IS_XML("${columnName}"))`
		   break;
		case this.dbi.DATA_TYPES.JSON_TYPE:
		   columnDataTypes[idx] = this.dbi.DATA_TYPES.storageOptions.JSON_TYPE
		   sizeConstraints[idx][0] = (this.dbi.DATABASE_VENDOR === this.SOURCE_VENDOR) ? sizeConstraints[idx][0] : this.dbi.DATA_TYPES.LOB_LENGTH
           checkConstraint = `check(YADAMU.IS_JSON("${columnName}"))`
		   break;
		case this.dbi.DATA_TYPES.CLOB_TYPE:
		case this.dbi.DATA_TYPES.BLOB_TYPE:
		   sizeConstraints[idx][0] = sizeConstraints[idx].length === 0 || sizeConstraints[idx][0] >  this.dbi.DATA_TYPES.LOB_LENGTH ? this.dbi.DATA_TYPES.LOB_LENGTH : sizeConstraints[idx][0]
		   break;
		default:
	  }
	  
	  let columnFunction  = ''
	  const columnDefinition = YadamuDataTypes.decomposeDataType(columnDataTypes[idx])
	  
	  switch (columnDefinition.type) {
        case this.dbi.DATA_TYPES.BINARY_TYPE:
        case this.dbi.DATA_TYPES.VARBINARY_TYPE:
        case this.dbi.DATA_TYPES.BLOB_TYPE:
		  const length = sizeConstraints[idx][0]
          let hexLength = length * 2
          hexLength = hexLength > this.dbi.DATA_TYPES.LOB_LENGTH ? this.dbi.DATA_TYPES.LOB_LENGTH:  hexLength
		  poolUsage += hexLength;
		  switch (true) {
             case (length > this.dbi.DATA_TYPES.VARBINARY_LENGTH) :
               // LONG VARBINARY
               // copyColumnDefinitions[idx] = `"${columnName}" FORMAT 'HEX'`
               copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER long varchar(${hexLength}), "${columnName}" as YADAMU.LONG_HEX_TO_BINARY("YADAMU_COL_${column_suffix}")`
               break;
             case (hexLength > this.dbi.DATA_TYPES.VARCHAR_LENGTH) :
               // VARBINARY > 32500 < 65000 - Read HEX value into LONG VARCHAR. Cast result to VARBINARY
               // copyColumnDefinitions[idx] = `"${columnName}" FORMAT 'HEX'`
               copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER long varchar(${hexLength}), "${columnName}" as CAST(YADAMU.LONG_HEX_TO_BINARY("YADAMU_COL_${column_suffix}") as ${columnDefinition.type}(${length}))`
               break;
             default:   
               copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER varchar(${hexLength}), "${columnName}" as HEX_TO_BINARY("YADAMU_COL_${column_suffix}")`
               // copyColumnDefinitions[idx] = `"${columnName}" FORMAT 'HEX'`
          }
          insertOperators[idx] = {
            prefix:   'x'
          , suffix:   `::${columnDefinition.type.toUpperCase()}(${length})`
          }
          break
        case 'CIRCLE':
          if (this.dbi.INBOUND_CIRCLE_FORMAT === 'CIRCLE') {
            copyColumnDefinitions.push(`"${columnName}"`)
            break;
          }
        case this.dbi.DATA_TYPES.GEOMETRY_TYPE:
          switch (this.SPATIAL_FORMAT) {
            case 'WKT':
            case 'EWKT':
            case 'GeoJSON':
              copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeomFromText("YADAMU_COL_${column_suffix}")`
              insertOperators[idx] = { 
                prefix:   '('
              , suffix:   ')'
              }
              break;
            case 'WKB':
            case 'EWKB':
              copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeomFromWKB(HEX_TO_BINARY("YADAMU_COL_${column_suffix}"))`
              insertOperators[idx] = { 
                prefix:   'ST_GeomFromWKB(HEX_TO_BINARY('
              , suffix:   '))'
              }
              break;
            /*
            case 'GeoJSON':
              copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeomFromGeoJSON("YADAMU_COL_${column_suffix}")`
              insertOperators[idx] = { 
                prefix:   'ST_GeomFromGeoJSON('
              , suffix:   ')'
              }
              break;
            */
          }
          break;
        case  this.dbi.DATA_TYPES.GEOGRAPHY_TYPE:
          switch (this.SPATIAL_FORMAT) {
            case 'WKT':
            case 'EWKT':
            case 'GeoJSON':
              copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeographyFromText("YADAMU_COL_${column_suffix}")`
              insertOperators[idx] = { 
                prefix:   'ST_GeographyFromText('
              , suffix:   ')'
              }
              break;
            case 'WKB':
            case 'EWKB':
              copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeographyFromWKB(HEX_TO_BINARY("YADAMU_COL_${column_suffix}"))`
              insertOperators[idx] = { 
                prefix:   'ST_GeographyFromWKB(HEX_TO_BINARY('
              , suffix:   '))'
              }
              break;
            /*
            case 'GeoJSON':
              copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(65000), "${columnName}" as ST_GeographyFromGeoJSON("YADAMU_COL_${column_suffix}")`
              insertOperators[idx] = { 
                prefix:   'ST_GeographyFromGeoJSON('
              , suffix:   ')'
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
              case "Oracle":
              case "MSSQLSERVER":
              case "Postgres":
              case "MySQL":
              case "MariaDB":
              case "MongoDB":
              case "Vertica":
                copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast("YADAMU_COL_${column_suffix}" as TIME)`
                break;
              default:
                copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast(TO_TIMESTAMP("YADAMU_COL_${column_suffix}",'YYYY-MM-DD"T"HH24:MI:SS.US') as TIME)`
            }
          }
          insertOperators[idx] = { 
            prefix:   'cast('
          , suffix:   ' as TIME)'
          }
          break;
        case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
          copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast(SUBSTR("YADAMU_COL_${column_suffix}",1,26) as TIMESTAMP)`
          insertOperators[idx] = { 
            prefix:   ''
          , suffix:   ''
          }
          break;
        case this.dbi.DATA_TYPES.TIME_TZ_TYPE:
          columnFunction = tableMetadata.hasOwnProperty('dataFile') 
		  ? copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast(TO_TIMESTAMP("YADAMU_COL_${column_suffix}",'YYYY-MM-DD"T"HH24:MI:SS.US') as TIME WITH TIME ZONE)`
		  : copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast("YADAMU_COL_${column_suffix}" as TIME WITH TIME ZONE)`
          insertOperators[idx] = { 
            prefix:   'cast('
          , suffix:   ' as TIME WITH TIME ZONE)'
          }
          break;
		case this.dbi.DATA_TYPES.INTERVAL_TYPE:
        case this.dbi.DATA_TYPES.INTERVAL_DAY_TO_SECOND_TYPE:
		  // If the tableMetadata has a dataFile property we are in COPY Mode and the File to be copied was generated by the Loader and contains ISO8601 formatted interval types
		  columnFunction = tableMetadata.hasOwnProperty('dataFile') 
		  ? `CAST(YADAMU.PARSE_ISO8601_INTERVAL("YADAMU_COL_${column_suffix}") AS INTERVAL DAY TO SECOND) ` 
		  : `CAST("YADAMU_COL_${column_suffix}" AS INTERVAL DAY TO SECOND)`
          copyColumnDefinitions[idx] =  `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(64), "${columnName}" as ${columnFunction}`
          insertOperators[idx] = { 
            prefix:   'cast('
          , suffix:   ' as INTERVAL DAY TO SECOND)'
          }
          break;
        case this.dbi.DATA_TYPES.INTERVAL_YEAR_TO_MONTH_TYPE:
		  columnFunction = tableMetadata.hasOwnProperty('dataFile') ? `CAST(YADAMU.PARSE_ISO8601_INTERVAL("YADAMU_COL_${column_suffix}") AS INTERVAL YEAR TO MONTH) ` : ` CAST("YADAMU_COL_${column_suffix}" AS INTERVAL YEAR TO MONTH)`
          copyColumnDefinitions[idx] =  `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(64), "${columnName}" as ${columnFunction}`
          insertOperators[idx] = { 
            prefix:   'cast('
          , suffix:   ' as INTERVAL YEAR TO MONTH)'
          }
          break;
        case this.dbi.DATA_TYPES.UUID_TYPE:
          copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as CAST("YADAMU_COL_${column_suffix}" AS UUID)`
          insertOperators[idx] = null
          break;
        case this.dbi.DATA_TYPES.NUMBER_TYPE:
		  if (this.dbi.CSV_NUMBER_PARSING_ISSUE) {
	        copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(1026), "${columnName}" as CAST("YADAMU_COL_${column_suffix}" AS ${this.generateStorageClause(columnDataTypes[idx],sizeConstraints[idx])})`
            insertOperators[idx] = null
            break;
	      }
        default:
          copyColumnDefinitions[idx] =`"${columnName}"`
          insertOperators[idx] = null
		  
      }
	  // Generate the final storage clause based on the any adjustments made to the column length
      return `"${columnName}" ${this.generateStorageClause(columnDataTypes[idx],sizeConstraints[idx])}${checkConstraint ? ` ${checkConstraint}`:  ''}`
    })


    this.dbi.applyDataTypeMappings(tableMetadata.tableName,tableMetadata.columnNames,targetDataTypes,this.dbi.IDENTIFIER_MAPPINGS,true)
	
	// Check Row Size and adjust as necessary
	
	if (this.dbi.DATABASE_VENDOR !== tableMetadata.vendor) {
      const lobList = []
	  let bytesUsed = this.calculateFixedRowSize(columnDataTypes,sizeConstraints,lobList)
	  // console.log(tableMetadata,bytesUsed,lobList.length)
	  if (bytesUsed > this.dbi.DATA_TYPES.ROW_SIZE) {
  	    this.adjustLobSizes(tableMetadata.tableName,bytesUsed,lobList,columnDefinitions,copyColumnDefinitions,sizeConstraints)
	  }

      // Vertica Raises out of Memory if copy buffer is > 55M ?
	
	}

	if (poolUsage > this.GENERAL_POOL_LIMIT) {
	    const longBinaryColumns = copyColumnDefinitions.filter((colDef) => {return colDef.includes('YADAMU.LONG_HEX_TO_BINARY')})
	    const poolAllowed = Math.floor(this.GENERAL_POOL_LIMIT / longBinaryColumns.length)
	    copyColumnDefinitions.forEach((copyColumnDefinition,idx) => {
          if (columnDefinitions[idx].indexOf(`" ${this.dbi.DATA_TYPES.BLOB_TYPE}`) > 0) {
            copyColumnDefinitions[idx] = copyColumnDefinitions[idx].replace(/\(\d*?\)/,`(${poolAllowed})`)
          } 
        })
	  }

    // All remote paths must use POSIX/Linux seperators (Vertica does not run on MS-Windows)

	const stagingFileName =  `YST-${crypto.randomBytes(16).toString("hex").toUpperCase()}`;
	const stagingFilePath =  path.join(this.dbi.LOCAL_STAGING_AREA,stagingFileName)
	const localPath       =  path.resolve(stagingFilePath)
	const remotePath      =  path.join(this.dbi.REMOTE_STAGING_AREA,stagingFileName).split(path.sep).join(path.posix.sep)
	
	const maxLengths  = sizeConstraints.map((sizeConstraint) => {
      const maxLength = sizeConstraint[0]
      return maxLength > 0 ? maxLength : undefined
    })
	
    const tableInfo = {
      ddl             :  this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,targetDataTypes)
    , dml             :  this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,tableMetadata.columnNames,insertOperators)
	, copy            :  this.generateCopyOperation(tableMetadata,remotePath,copyColumnDefinitions)
    , mergeout        : `select do_tm_task('mergeout','${this.targetSchema}.${tableMetadata.tableName}')`
    , stagingFileName : stagingFileName
    , localPath       : localPath
    , columnNames     : tableMetadata.columnNames
    , targetDataTypes : targetDataTypes
    , maxLengths      : maxLengths
    , insertOperators : insertOperators
    , insertMode      : insertMode
    , _SCHEMA_NAME    : this.targetSchema
    , _BATCH_SIZE     : this.dbi.BATCH_SIZE
    , _TABLE_NAME     : tableMetadata.tableName
    , _SPATIAL_FORMAT : this.SPATIAL_FORMAT
    }
    
    // Add Support for Copy based Operations
    return tableInfo
  }

}

export { VerticaStatementGenerator as default }


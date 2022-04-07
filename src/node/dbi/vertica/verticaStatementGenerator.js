
import path                     from 'path';
import crypto                   from 'crypto';

import YadamuDataTypes          from '../base/yadamuDataTypes.js'
import YadamuStatementGenerator from '../base/yadamuStatementGenerator.js'

class VerticaStatementGenerator extends YadamuStatementGenerator {

  get TABLE_LOB_COUNT()         { return this._TABLE_LOB_COUNT }
  set TABLE_LOB_COUNT(v)        { this._TABLE_LOB_COUNT = v }
  
  set TABLE_UNUSED_BYTES(v)     { this._TABLE_UNUSED_BYTES = v }
  
  get TABLE_LOB_LIMIT()          { 
    const  allocatedSize = Math.floor(this._TABLE_UNUSED_BYTES / (this._TABLE_LOB_COUNT || 1)) 
    return allocatedSize < this.dbi.DATA_TYPES.LOB_LENGTH ? allocatedSize:  this.dbi.DATA_TYPES.LOB_LENGTH
  }
  
  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {
    super(dbi, vendor, targetSchema, metadata, yadamuLogger)
  }

  getMappedDataType(dataType,idx,sizeConstraints) {
 
    const mappedDataType = super.getMappedDataType(dataType,sizeConstraints[idx])
    let length = parseInt(sizeConstraints[idx])
	 
	switch (mappedDataType) {
	  // Disable byte length calculation for CHAR as this leads to issues related to blank padding.
      // case this.dbi.DATA_TYPES.CHAR_TYPE:  
      case this.dbi.DATA_TYPES.VARCHAR_TYPE:
        // Vertica's CHAR/VARCHAR Size Constraint is size in bytes. Adjust size from other vendors to accomodate multi-byte characters by applying user controllable Fudge Factor. What percentage of the content requires more than one byte to store.      
        length = this.SOURCE_VENDOR === this.dbi.DATABASE_VENDOR ? length :  Math.ceil(length * this.dbi.BYTE_TO_CHAR_RATIO);
        length = length > this.dbi.DATA_TYPES.LOB_LENGTH ?  this.dbi.DATA_TYPES.LOB_LENGTH : length
		// console.log('VARCHAR',sizeConstraints[idx],'==>',length)  
        sizeConstraints[idx] = length.toString()		
      case this.dbi.DATA_TYPES.CLOB_TYPE:
		switch (true) {
          case (isNaN(length)):                                 return this.dbi.DATA_TYPES.CLOB_TYPE
          case (length === undefined):                          return this.dbi.DATA_TYPES.CLOB_TYPE
          case (length === -1):                                 return this.dbi.DATA_TYPES.CLOB_TYPE
          case (length > this.dbi.DATA_TYPES.LOB_LENGTH):       return this.dbi.DATA_TYPES.CLOB_TYPE
		  case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH):   return this.dbi.DATA_TYPES.CLOB_TYPE
		  default:                                              return this.dbi.DATA_TYPES.VARCHAR_TYPE
		  
		}
     	break
      case this.dbi.DATA_TYPES.BINARY_TYPE:
      case this.dbi.DATA_TYPES.VARBINARY_TYPE:
      case this.dbi.DATA_TYPES.BLOB_TYPE:
  		switch (true) {
          case (isNaN(length)):                                 return this.dbi.DATA_TYPES.BLOB_TYPE
          case (length === undefined):                          return this.dbi.DATA_TYPES.BLOB_TYPE
          case (length === -1):                                 return this.dbi.DATA_TYPES.BLOB_TYPE
          case (length > this.dbi.DATA_TYPES.LOB_LENGTH):       return this.dbi.DATA_TYPES.BLOB_TYPE
          case (length > this.dbi.DATA_TYPES.VARBINARY_LENGTH): return this.dbi.DATA_TYPES.BLOB_TYPE
          default:                                              return this.dbi.DATA_TYPES.VARBINARY_TYPE
		}
      default:                                                  return mappedDataType
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
  
    
  generateCopyOperation(tableMetadata,remotePath,copyColumnDefinitions) {
    // Partitioned Tables need one entry per partition 

    if (tableMetadata.hasOwnProperty('partitionCount')) {
      return tableMetadata.dataFile.map((remotePath,idx) => {
        remotePath = remotePath.split(path.sep).join(path.posix.sep)
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
		  bytesUsed = bytesUsed + typeDefinition.length
          break;
        default:
	      bytesUsed+= parseInt(sizeConstraints[idx])
      }        
	  // console.log(typeDefinition,columnDataType,bytesUsed)
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
          copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER long varchar(${this.TABLE_LOB_LIMIT > 16000000 ? DataTypes.LOB_LENGTH:  (this.TABLE_LOB_LIMIT * 2)}), "${columnNames[idx]}" as YADAMU.LONG_HEX_TO_BINARY("YADAMU_COL_${column_suffix}")`  
        } 
        */		
     	// console.log(columnDefinitions[idx])
      })
    }
  }


  generateTableInfo(tableMetadata) {
	
    let insertMode = 'Copy';
	
	// console.log(tableMetadata)
    
	const columnNames           = tableMetadata.columnNames
    const mappedDataTypes       = []
	const columnDataTypes       = []
	const insertOperators       = []
    const copyColumnDefinitions = []
	const sizeConstraints       = [...tableMetadata.sizeConstraints]
	const args                  = new Array(tableMetadata.columnNames.length).fill('?')
      
    const columnContentType = new Array(columnNames.length).fill(null);
	
	
	const columnDefinitions = columnNames.map((columnName,idx) => {
		
      const column_suffix = String(idx+1).padStart(3,"0");

   	  let mappedDataType = (tableMetadata.source) ? tableMetadata.dataTypes[idx] : this.getMappedDataType(tableMetadata.dataTypes[idx],idx,sizeConstraints)
	  // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,tableMetadata.vendor,tableMetadata.dataTypes[idx],tableMetadata.sizeConstraints[idx]],`Mapped to "${mappedDataType}".`)
      let columnDataType = mappedDataType
      let length = parseInt(sizeConstraints[idx])
	  let checkConstraint = ''
	  
	  switch (mappedDataType) {
		 case this.dbi.DATA_TYPES.XML_TYPE:
		   columnDataType = this.dbi.DATA_TYPES.storageOptions.XML_TYPE
		   sizeConstraints[idx] = this.dbi.DATA_TYPES.LOB_LENGTH.toString()
           checkConstraint = `check(YADAMU.IS_XML("${columnName}"))`
		   break;
		case this.dbi.DATA_TYPES.JSON_TYPE:
		   columnDataType = this.dbi.DATA_TYPES.storageOptions.JSON_TYPE
		   sizeConstraints[idx] = this.dbi.DATA_TYPES.LOB_LENGTH.toString()
           checkConstraint = `check(YADAMU.IS_JSON("${columnName}"))`
		   break;
		case this.dbi.DATA_TYPES.CLOB_TYPE:
		case this.dbi.DATA_TYPES.BLOB_TYPE:
		   sizeConstraints[idx] = isNaN(length) ? this.dbi.DATA_TYPES.LOB_LENGTH.toString() : (((length < 1) || (length > this.dbi.DATA_TYPES.LOB_LENGTH)) ? this.dbi.DATA_TYPES.LOB_LENGTH:  length).toString()
		default:
	  }
	  
	  const columnDefinition = YadamuDataTypes.decomposeDataType(columnDataType)
	  
	  switch (columnDefinition.type) {
        case this.dbi.DATA_TYPES.BINARY_TYPE:
        case this.dbi.DATA_TYPES.VARBINARY_TYPE:
        case this.dbi.DATA_TYPES.BLOB_TYPE:
          length = ( isNaN(length) || ((length < 1) || (length > this.dbi.DATA_TYPES.LOB_LENGTH))) ? this.dbi.DATA_TYPES.LOB_LENGTH:  length
          let hexLength = length * 2
          hexLength = ((hexLength < 2) || (hexLength > this.dbi.DATA_TYPES.LOB_LENGTH)) ? this.dbi.DATA_TYPES.LOB_LENGTH:  hexLength
          switch (true) {
             case (length > this.dbi.DATA_TYPES.LOB_LENGTH) :
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
            prefix:   'X'
          , suffix:   `::${columnDefinition.type.toUpperCase()}(${length})`
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
          switch (this.dbi.INBOUND_SPATIAL_FORMAT) {
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
            prefix:   'cast('
          , suffix:   ' as TIME)'
          }
          break;
        case this.dbi.DATA_TYPES.TIME_TZ_TYPE:
          copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(36), "${columnName}" as cast("YADAMU_COL_${column_suffix}" as TIME WITH TIME ZONE)`
          insertOperators[idx] = { 
            prefix:   'cast('
          , suffix:   ' as TIME WITH TIME ZONE)'
          }
          break;
        case this.dbi.DATA_TYPES.INTERVAL_DAY_TO_SECOND_TYPE:
          copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(64), "${columnName}" as CAST("YADAMU_COL_${column_suffix}" AS INTERVAL DAY TO SECOND)`
          insertOperators[idx] = { 
            prefix:   'cast('
          , suffix:   ' as INTERVAL DAY TO SECOND)'
          }
          break;
        case this.dbi.DATA_TYPES.INTERVAL_YEAR_TO_MONTH_TYPE:
          copyColumnDefinitions[idx] = `"YADAMU_COL_${column_suffix}" FILLER VARCHAR(64), "${columnName}" as CAST("YADAMU_COL_${column_suffix}" AS INTERVAL YEAR TO MONTH)`
          insertOperators[idx] = { 
            prefix:   'cast('
          , suffix:   ' as INTERVAL YEAR TO MONTH)'
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
      mappedDataTypes.push(mappedDataType)     
	  columnDataTypes.push(columnDataType)
      return `"${columnName}" ${this.generateStorageClause(columnDataType,sizeConstraints[idx])}${checkConstraint ? ` ${checkConstraint}`:  ''}`
    })
	
	// Check Row Size and adjust as necessary
	
    const lobList = []
	let bytesUsed = this.calculateFixedRowSize(columnDataTypes,sizeConstraints,lobList)
	// console.log(tableMetadata.tableName,bytesUsed,lobList.length)
	if (bytesUsed > this.dbi.DATA_TYPES.ROW_SIZE) {
	  this.adjustLobSizes(tableMetadata.tableName,bytesUsed,lobList,columnDefinitions,sizeConstraints)
	}
	
    // All remote paths must use POSIX/Linux seperators (Vertica does not run on MS-Windows)

	const stagingFileName =  `YST-${crypto.randomBytes(16).toString("hex").toUpperCase()}`;
	const stagingFilePath =  path.join(this.dbi.LOCAL_STAGING_AREA,stagingFileName)
	const localPath       =  path.resolve(stagingFilePath)
	let remotePath        =  path.join(this.dbi.REMOTE_STAGING_AREA,stagingFileName).split(path.sep).join(path.posix.sep)
	
	const maxLengths  = sizeConstraints.map((sizeConstraint) => {
      const maxLength = parseInt(sizeConstraint) 
      return maxLength > 0 ? maxLength : undefined
    })
	
    const tableInfo = {
      ddl            :  this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,mappedDataTypes)
    , dml            :  this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,columnNames,insertOperators)
	, copy           :  this.generateCopyOperation(tableMetadata,remotePath,copyColumnDefinitions)
    , mergeout:         `select do_tm_task('mergeout','${this.targetSchema}.${tableMetadata.tableName}')`
    , stagingFileName:  stagingFileName
    , localPath:        localPath
    , columnNames    :  tableMetadata.columnNames
    , targetDataTypes:  mappedDataTypes
    , maxLengths:       maxLengths
    , insertOperators:  insertOperators
    , insertMode     :  insertMode
    , _SCHEMA_NAME:     this.targetSchema
    , _BATCH_SIZE    :  this.dbi.BATCH_SIZE
    , _TABLE_NAME:      tableMetadata.tableName
    , _SPATIAL_FORMAT:  this.dbi.INBOUND_SPATIAL_FORMAT
    }
    
    // Add Support for Copy based Operations
    
    return tableInfo
  }

}

export { VerticaStatementGenerator as default }


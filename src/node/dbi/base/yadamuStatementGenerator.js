
import fs                     from 'fs';

import YadamuDataTypes        from './yadamuDataTypes.js'

class YadamuStatementGenerator {
      
  get SOURCE_VENDOR()        { return this._TARGET_VENDOR }
  set SOURCE_VENDOR(v)       { this._TARGET_VENDOR = v }
  
  get SPATIAL_FORMAT()        { return this._SPATIAL_FORMAT || this.dbi.INBOUND_SPATIAL_FORMAT }
  set SPATIAL_FORMAT(v)       { this._SPATIAL_FORMAT = v }
  
  /*
  **
  ** Return an ins eek nce of the YadamuDataTypes class specific to the value of SOURCE_VENDOR
  **
  ** Initially returns a promise (since it uses the import() function.
  ** The promise is resolved and _SOURCE_DATA_TYPES is updated with the resolved value in init()
  **
  */
  
  get SOURCE_DATA_TYPES()    { 
    this._SOURCE_DATA_TYPES = this._SOURCE_DATA_TYPES || new Promise(async(resolve,reject) => {
      const classFile = YadamuDataTypes.DATA_TYPE_CONFIGURATION[this.SOURCE_VENDOR].class
      const VendorDataTypes = (await import(classFile)).default
      const vendorDataTypes = new VendorDataTypes()
      resolve(vendorDataTypes)
    })
    return this._SOURCE_DATA_TYPES
  }
  
  set SOURCE_DATA_TYPES(v)    { this._SOURCE_DATA_TYPES = v }

  /*
  **
  ** Return a data type mapping between the types supported by the source vendor to types supported by the target vendor
  **
  */

  get TYPE_MAPPINGS() {

    this._TYPE_MAPPINGS = this._TYPE_MAPPINGS || (() => {
      const typeMappings = new Map()
      for (let c = this.SOURCE_DATA_TYPES; c !== null; c = Object.getPrototypeOf(c)) {
        Object.getOwnPropertyNames(c).filter((name) => { 
          return Object.getOwnPropertyDescriptor(c,name).get
        }).forEach((name) => { 
          const sourceType = this.SOURCE_DATA_TYPES[name]
          if (typeof sourceType === 'string') {
            const sourceTypeDefinition = YadamuDataTypes.decomposeDataType(sourceType)
            if (sourceType === sourceTypeDefinition.type) {
              // const targetType = this.dbi.DATA_TYPES.storageOptions[name] || this.dbi.DATA_TYPES[name]
              const targetType = this.dbi.DATA_TYPES[name]
              if (typeof targetType === 'string') {
                typeMappings.has(sourceType) ? typeMappings.set(sourceType,typeMappings.get(sourceType).add(targetType)) : typeMappings.set(sourceType,new Set().add(targetType))
              }
            }
          }   
        })
      }
      // console.log(typeMappings)
      typeMappings.forEach((value,key) => {
        typeMappings.set(key, value.size === 1 ? value.values().next().value : this.dbi.DATA_TYPES.coalesceTypeMappings(Array.from(value.values())))
      })
      // console.log(typeMappings)
	  return typeMappings
    })()
    return this._TYPE_MAPPINGS
  }
  
  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {

    this.dbi = dbi;
    this.SOURCE_VENDOR = vendor
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;   
  }
  
  async init() {
    // Set it to the value of the resolved promise..
    this.SOURCE_DATA_TYPES = await this.SOURCE_DATA_TYPES;
  }  
  
  async debugStatementGenerator(options,statementCache) {  
    
    console.log(JSON.stringify(this.metadata))
    console.log(JSON.stringify(Array.from(this.TYPE_MAPPINGS.entries())))
    console.log(options)
    console.log(statementCache)
    
  } 

  getSpatialFormat(tableMetadata) {
	 
	if (tableMetadata.hasOwnProperty("source")) {
   	  const spatialColumnList = tableMetadata.dataTypes.flatMap((dataType,idx) => { return (YadamuDataTypes.isSpatial(dataType) && (idx < tableMetadata.source.length)) ? [idx] : [] })
      const spatialFormats = spatialColumnList.map((idx) => {
		return YadamuDataTypes.isSpatial(tableMetadata.source.dataTypes[idx]) ?  this.dbi.INBOUND_SPATIAL_FORMAT  
		     : YadamuDataTypes.isBinary(tableMetadata.source.dataTypes[idx])  ? 'WKB'  
	         : YadamuDataTypes.isJSON(tableMetadata.source.dataTypes[idx])    ? 'GeoJSON'
			 : 'WKT'
	  })
	  if (spatialFormats.length > 0) {
  	    // ToDo ### Multiple spatial formats
	    if (spatialFormats.length > 1)  {
		  this.yadamuLogger.warning([this.dbi.DATABASE_VENDOR,tableMetadata.tableName],`Multiple spatial formats detected : ${spatialFormats}`)
	    }
	    if (this.dbi.INBOUND_SPATIAL_FORMAT !== spatialFormats[1]) {
		  this.yadamuLogger.qa([this.dbi.DATABASE_VENDOR,tableMetadata.tableName],`Spatial format mismatch detected :${this.dbi.INBOUND_SPATIAL_FORMAT} Vs ${spatialFormats}`)
	    }		  
	    return spatialFormats[0]
	  }
	}		  
	return this.dbi.INBOUND_SPATIAL_FORMAT  
  }
  
  isJSON(dataType) {
    return YadamuDataTypes.isJSON(dataType)
  }
 
  isXML(dataType) {
    return YadamuDataTypes.isXML(dataType)
  }

  mapUserDefinedDataType(dataType) {
      
    /*
    **
    ** Handle mappings for custom / user defined types. e.g. 
    **
    **   Oracle: Map all user defined types to CLOB ...
    **
    ** Currently hard-wired. If most drivers require this functionality it would be better to load the Source Vendors DBI Class dynamically 
    ** and have the DBI class provide a static method that determines the mapping
    **
    */
    
    switch (this.SOURCE_VENDOR) {
      case 'Oracle':
        switch (true) {
           case (dataType.indexOf('.') > -1):             
           return this.dbi.DATA_TYPES.USER_DEFINED_TYPE
        }
        break;
     default:
    }
    return undefined
  }    

  isOracleObjectType(dataType) {
    return ((dataType.indexOf('"."') > -1) && (this.SOURCE_VENDOR === "Oracle")) ? 'ORACLE_OBJECT_TYPE' : undefined
  }
  
  validateNumericProperties(typeDefinition) {
    typeDefinition.length = typeDefinition.length > this.dbi.DATA_TYPES.NUMERIC_PRECISION ? this.dbi.DATA_TYPES.NUMERIC_PRECISION : typeDefinition.length
    typeDefinition.scale = typeDefinition.scale > this.dbi.DATA_TYPES.NUMERIC_SCALE ? this.dbi.DATA_TYPES.NUMERIC_SCALE : typeDefinition.scale
  }  
    
  refactorBySizeConstraint(sourceDataType,targetDataType,sizeConstraint) {

	 /*
	 **
	 ** Adjust data type based on size constraints
	 **
	 */
	 
    switch (targetDataType) {
      case this.dbi.DATA_TYPES.CHAR_TYPE:
      case this.dbi.DATA_TYPES.VARCHAR_TYPE:
      case this.dbi.DATA_TYPES.CLOB_TYPE:
        switch (true) {
		  case (sizeConstraint.length === 0):                                     return this.dbi.DATA_TYPES.CLOB_TYPE
          case (sizeConstraint[0] > this.dbi.DATA_TYPES.VARCHAR_LENGTH):          return this.dbi.DATA_TYPES.CLOB_TYPE
          case (sizeConstraint[0] < this.dbi.DATA_TYPES.VARCHAR_LENGTH):          return this.dbi.DATA_TYPES.VARCHAR_TYPE
          default:                                                                return targetDataType
        }

      case this.dbi.DATA_TYPES.NCHAR_TYPE:
      case this.dbi.DATA_TYPES.NVARCHAR_TYPE:
      case this.dbi.DATA_TYPES.NCLOB_TYPE:
        switch (true) {
		  case (sizeConstraint.length === 0):                                     return this.dbi.DATA_TYPES.NCLOB_TYPE
          case (sizeConstraint[0] > this.dbi.DATA_TYPES.NVARCHAR_LENGTH):         return this.dbi.DATA_TYPES.NCLOB_TYPE
          case (sizeConstraint[0] < this.dbi.DATA_TYPES.NVARCHAR_LENGTH):         return this.dbi.DATA_TYPES.NVARCHAR_TYPE
          default:                                                                return targetDataType
        }

      case this.dbi.DATA_TYPES.BINARY_TYPE:
      case this.dbi.DATA_TYPES.VARBINARY_TYPE:
      case this.dbi.DATA_TYPES.BLOB_TYPE:
        switch (true) {
		  case (sizeConstraint.length === 0):                                     return this.dbi.DATA_TYPES.BLOB_TYPE
          case (sizeConstraint[0] > this.dbi.DATA_TYPES.BINARY_LENGTH):           return this.dbi.DATA_TYPES.BLOB_TYPE
          case (sizeConstraint[0] < this.dbi.DATA_TYPES.BINARY_LENGTH):           return this.dbi.DATA_TYPES.VARBINARY_TYPE
          default:                                                                return targetDataType
        }

      case this.dbi.DATA_TYPES.DEIMAL_TYPE:
      case this.dbi.DATA_TYPES.NUMERIC_TYPE:
	    if  (sizeConstraint.length === 0) {
  		  return  this.dbi.DATA_TYPES.UNBOUNDED_NUMERIC_TYPE
		}
    	const sourceNumericType = `${sourceDataType}(${sizeConstraint[0]},${sizeConstraint[1]})`
        if (YadamuDataTypes.isUnboundedNumeric(this.SOURCE_VENDOR,sourceNumericType)) {
  		  return  this.dbi.DATA_TYPES.UNBOUNDED_NUMERIC_TYPE
		}
	    return targetDataType
                    
      case this.dbi.DATA_TYPES.TIME_TYPE:
      case this.dbi.DATA_TYPES.DATETIME_TYPE:
      case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
        switch (true) {
          case (sizeConstraint[0] > this.dbi.DATA_TYPES.TIMESTAMP_PRECISION):     return `${targetDataType}(${this.dbi.DATA_TYPES.TIMESTAMP_PRECISION})`
          default:                                                                return targetDataType
        }

	  case this.dbi.DATA_TYPES.TIME_TZ_TYPE:
	  case this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE:
	    const components = targetDataType.split(" ")
	    const leadIn = components.shift()
        switch (true) {
          case (sizeConstraint.length === 0):                                     return targetDataType
          case (sizeConstraint[0] > this.dbi.DATA_TYPES.TIMESTAMP_PRECISION):     return `${leadIn}(${this.dbi.DATA_TYPES.TIMESTAMP_PRECISION}) ${components.join(" ")}`
          default:                                                                return `${leadIn}(${sizeConstraint[0]}) ${components.join(" ")}`
        }
      default:                                                                    return targetDataType
    }   
  }

  getUserSpecifiedMapping(tableName,columnName) {
   
    const dataType = this.dbi.IDENTIFIER_MAPPINGS?.[tableName]?.columnMappings?.[columnName]?.dataType
    return dataType
	
  }
  
  getTargetDataTypes(tableMetadata) {

  	 /*
	 **
	 ** Get the target data type.
     **
     ** The mapped data type is determined as follows :
     **
	 */
     
	 // If the table metadata contains a 'source' key then the target table already exists then the target data types are defined by the target table. 
	
     if (tableMetadata.source) {
       return [...tableMetadata.dataTypes]
     }
	 
	 // If the soure and target vendor are the same no mapping operations are required ### this.dbi.DATATYPE_IDENTITY_MAPPING ???
	
	 
     if (this.dbi.DATABASE_VENDOR === this.SOURCE_VENDOR) {
       return [...tableMetadata.dataTypes]
     }
	 
	 return tableMetadata.dataTypes.map((dataType,idx) => {
	   let targetDataType = this.getUserSpecifiedMapping(tableMetadata.tableName,tableMetadata.columnNames[idx]) 
		                 || this.TYPE_MAPPINGS.get(dataType) 
		                 || this.TYPE_MAPPINGS.get(dataType.toLowerCase()) 
			             || this.TYPE_MAPPINGS.get(dataType.toUpperCase()) 
	                     || this.mapUserDefinedDataType(dataType,tableMetadata.sizeConstraints[idx])
                         || this.yadamuLogger.logInternalError([this.dbi.DATABASE_VENDOR,`MAPPING NOT FOUND`],`Missing Mapping for "${dataType}" in mappings for "${this.SOURCE_VENDOR}".`)
						   
	   targetDataType = this.refactorBySizeConstraint(dataType,targetDataType,tableMetadata.sizeConstraints[idx])
       // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,this.SOURCE_VENDOR,dataType,tableMetadata.sizeConstraints[idx]],`Mapped to "${targetDataType}".`)
       return targetDataType
	 })
  }
	
  generateStorageClause(mappedDataType,sizeConstraint) {
	  
    if (sizeConstraint.length > 0) {

      if (RegExp(/\(.*\)/).test(mappedDataType)) {
         /* Already has a Size specified */
         return mappedDataType
      }
      
      if (this.dbi.DATA_TYPES.UNBOUNDED_TYPES.includes(mappedDataType)) {
        return mappedDataType
      }
      
      const dataTypeDefinition = YadamuDataTypes.composeDataType(mappedDataType,sizeConstraint)
      switch (mappedDataType) {
        case this.dbi.DATA_TYPES.NUMERIC_TYPE:
        case this.dbi.DATA_TYPES.DEIMAL_TYPE:
          this.validateNumericProperties(dataTypeDefinition)
        default:
      }
          
      if (dataTypeDefinition.scale && (dataTypeDefinition.scale > 0)) {
        return `${dataTypeDefinition.type}(${dataTypeDefinition.length},${dataTypeDefinition.scale})`
      }                                                   
      
      if (dataTypeDefinition.length && (dataTypeDefinition.length > 0)) {
        const type = dataTypeDefinition.type.toLowerCase()
        return (type.includes(' ') && !type.toLowerCase().startsWith('long')  && !type.toLowerCase().startsWith('bit') && !type.toLowerCase().startsWith('character')) ? type.replace(' ',`(${dataTypeDefinition.length}) `) : `${dataTypeDefinition.type}(${dataTypeDefinition.length})`
      }
    }
    else {
      if (YadamuDataTypes.isBCD(mappedDataType)) {
        return this.dbi.DATA_TYPES.UNBOUNDED_NUMERIC_TYPE
      }
    }

    return mappedDataType
  }
      
  generateDDLStatement(schema,tableName,columnDefinitions,mappedDataTypes) {
      
    return `create table "${schema}"."${tableName}"(\n  ${columnDefinitions.join(',')})`;
    
  }

  generateDMLStatement(schema,tableName,columnNames,insertOperators) {
    return `insert into "${schema}"."${tableName}" ("${columnNames.join('","')}") values (${insertOperators.join(',')})`;
  }

  getInsertOperator(targetDataType) {
    return '?'
  }
  
  generateTableInfo(tableMetadata) {

    let insertMode = 'Batch';
    this.SPATIAL_FORMAT = this.getSpatialFormat(tableMetadata)
   
    const insertOperators = []
    
	const targetDataTypes = this.getTargetDataTypes(tableMetadata)
	
	// this.debugStatementGenerator(null,null)

    const columnDefinitions = targetDataTypes.map((targetDataType,idx) => {		
      insertOperators.push(this.getInsertOperator(targetDataType))
      return `"${tableMetadata.columnNames[idx]}" ${this.generateStorageClause(targetDataType,tableMetadata.sizeConstraints[idx])}`    
    })

    this.dbi.applyDataTypeMappings(tableMetadata.tableName,tableMetadata.columnNames,targetDataTypes,this.dbi.IDENTIFIER_MAPPINGS,true)
	    
    const tableInfo =  { 
      ddl              : tableMetadata.source ? null : this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,targetDataTypes)
    , dml              : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,tableMetadata.columnNames,insertOperators)
    , columnNames      : tableMetadata.columnNames
    , targetDataTypes  : targetDataTypes
    , insertMode       : insertMode
    , _BATCH_SIZE      : this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT  : this.SPATIAL_FORMAT
    }
    
    return tableInfo
  }
  
  async generateStatementCache() {

    await this.init()

    const statementCache = {}
    const tables = Object.keys(this.metadata);
    const ddlStatements = tables.map((table,idx) => {
      const tableMetadata = this.metadata[table];
      const tableInfo = this.generateTableInfo(tableMetadata);
      statementCache[this.metadata[table].tableName] = tableInfo;
      return tableInfo.ddl;
    })
   
    // await this.debugStatementGenerator({},statementCache)
	
    return statementCache;
  }

}

export { YadamuStatementGenerator as default }



import fs                     from 'fs';

import YadamuDataTypes        from './yadamuDataTypes.js'

class YadamuStatementGenerator {
      
  get SOURCE_VENDOR()        { return this._TARGET_VENDOR }
  set SOURCE_VENDOR(v)       { this._TARGET_VENDOR = v }
  
  /*
  **
  ** Return an instance of the YadamuDataTypes class specific to the value of SOURCE_VENDOR
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
      // this.dbi.yadamu.initializeDataTypes(VendorDataTypes,JSON.parse(fs.readFileSync(YadamuDataTypes.DATA_TYPE_CONFIGURATION[this.SOURCE_VENDOR].file)))
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
 
  isJSON(dataType) {
    return YadamuDataTypes.isJSON(dataType)
  }
 
  isXML(dataType) {
    return YadamuDataTypes.isXML(dataType)
  }
  
  refactorByLength(mappedDataType,length) {

      switch (mappedDataType) {
     
        case this.dbi.DATA_TYPES.CHAR_TYPE:
        case this.dbi.DATA_TYPES.VARCHAR_TYPE:
        case this.dbi.DATA_TYPES.CLOB_TYPE:
          switch (true) {
            case (isNaN(length)):                                        return this.dbi.DATA_TYPES.CLOB_TYPE
            case (length === undefined):                                 return this.dbi.DATA_TYPES.CLOB_TYPE
            case (length === -1):                                        return this.dbi.DATA_TYPES.CLOB_TYPE
            case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH):          return this.dbi.DATA_TYPES.CLOB_TYPE
            case (length < this.dbi.DATA_TYPES.BINARY_LENGTH):           return this.dbi.DATA_TYPES.VARCHAR_TYPE
            default:                                                     return mappedDataType
          }

        case this.dbi.DATA_TYPES.BINARY_TYPE:
        case this.dbi.DATA_TYPES.VARBINARY_TYPE:
        case this.dbi.DATA_TYPES.BLOB_TYPE:
          switch (true) {
            case (isNaN(length)):                                        return this.dbi.DATA_TYPES.BLOB_TYPE
            case (length === undefined):                                 return this.dbi.DATA_TYPES.BLOB_TYPE
            case (length === -1):                                        return this.dbi.DATA_TYPES.BLOB_TYPE
            case (length > this.dbi.DATA_TYPES.BINARY_LENGTH):           return this.dbi.DATA_TYPES.BLOB_TYPE
            case (length < this.dbi.DATA_TYPES.BINARY_LENGTH):           return this.dbi.DATA_TYPES.VARBINARY_TYPE
            default:                                                     return mappedDataType
          }

        case this.dbi.DATA_TYPES.NUMERIC_TYPE:        
          switch (true) {
            // Need to address P,S and linits on P,S
            case (isNaN(length)):                                        
            case (length === undefined):                                 return this.dbi.DATA_TYPES.UNBOUNDED_NUMERIC_TYPE
            default:                                                     return mappedDataType
          }
          
        case this.dbi.DATA_TYPES.TIME_TYPE:
        case this.dbi.DATA_TYPES.DATETIME_TYPE:
        case this.dbi.DATA_TYPES.TIMESTAMP_TYPE:
          switch (true) {
            case (length > this.dbi.DATA_TYPES.TIMESTAMP_PRECISION):     return `${mappedDataType}(${this.dbi.DATA_TYPES.TIMESTAMP_PRECISION})`
            default:                                                     return mappedDataType
          }

	   case this.dbi.DATA_TYPES.TIME_TZ_TYPE:
	   case this.dbi.DATA_TYPES.TIMESTAMP_TZ_TYPE:
          switch (true) {
            case (length === undefined):                                 return mappedDataType
            case (length > this.dbi.DATA_TYPES.TIMESTAMP_PRECISION):     return mappedDataType.replace(' ',`(${this.dbi.DATA_TYPES.TIMESTAMP_PRECISION}) WITH TIME ZONE`)
            default:                                                     return mappedDataType.replace(' ',`(${length}) WITH TIME ZONE`)
          }
        default:                                                         return mappedDataType
      }   
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
  
  mapDataType(dataTypeDefinition) {
       
     // Virtual data types (XML,JSON,BOOLEAN) may not match the case used by the key in the map object.
     let mappedDataType = this.TYPE_MAPPINGS.get(dataTypeDefinition.type) ||  this.TYPE_MAPPINGS.get(dataTypeDefinition.type.toLowerCase()) || this.TYPE_MAPPINGS.get(dataTypeDefinition.type.toUpperCase()) 
    
     switch (mappedDataType) {
       case this.dbi.DATA_TYPES.CHAR_TYPE:
         switch (true) {
           case (dataTypeDefinition.length === undefined) :
           case (dataTypeDefinition.length < 0) :
           case (dataTypeDefinition.length > this.dbi.DATA_TYPES.CHAR_LENGTH) :
             mappedDataType = this.dbi.DATA_TYPES.CLOB_TYPE
             break; 
           default:          
         }
         break
       case this.dbi.DATA_TYPES.VARCHAR_TYPE:
         switch (true) {
           case (dataTypeDefinition.length === undefined) :
           case (dataTypeDefinition.length < 0) :
           case (dataTypeDefinition.length > this.dbi.DATA_TYPES.VARCHAR_LENGTH) :
             mappedDataType = this.dbi.DATA_TYPES.CLOB_TYPE
             break
           default:          
         }
         break
       case this.dbi.DATA_TYPES.NUMERIC_TYPE:
       case this.dbi.DATA_TYPES.DEIMAL_TYPE:
         const sourceDataType = `${dataTypeDefinition.type}(${dataTypeDefinition.length},${dataTypeDefinition.scale})`
         if (YadamuDataTypes.isUnboundedNumeric(this.SOURCE_VENDOR,sourceDataType)) {
           return this.dbi.DATA_TYPES.UNBOUNDED_NUMERIC_TYPE
         }
         this.validateNumericProperties(dataTypeDefinition)
       default:
     }
     // console.log(key,':',dataTypeDefinition.type,dataTypeDefinition.length,'==>',mappedDataType)
     // console.log(dataTypeDefinition.type,'==>',mappedDataType)
     return mappedDataType
     
  }

  generateStorageClause(mappedDataType,sizeConstraint) {

    if (sizeConstraint) {

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
        return (type.includes(' ') && !type.toLowerCase().startsWith('long')  &&!type.startsWith('bit')) ? type.replace(' ',`(${dataTypeDefinition.length}) `) : `${dataTypeDefinition.type}(${dataTypeDefinition.length})`
      }
    }
    else {
      if (YadamuDataTypes.isBCD(mappedDataType)) {
        return this.dbi.DATA_TYPES.UNBOUNDED_NUMERIC_TYPE
      }
    }

    return mappedDataType
  }
      
  getMappedDataType(dataType,sizeConstraint) {
      
    /*
    **
    **  Map the source datatype to the target.
    **
    ** If the target table already exists then the data type is defined by the target table. 
    ** If the target table does not exist use that mapping mechansim to determine the target type
    ** First objtain the name of the key that describes the source data type in the source table mapping object
    ** USe tje key to obtain the corresponding data type from the target environemnt's type mappings
    **
    */

    const dataTypeDefinition = YadamuDataTypes.composeDataType(dataType,sizeConstraint)
    
    if ((!this.dbi.DATATYPE_IDENTITY_MAPPING) || (this.dbi.DATABASE_VENDOR !== this.SOURCE_VENDOR)) {

      const mappedDataType = this.mapDataType(dataTypeDefinition);
   
      if (mappedDataType === undefined) {
        let userDefinedDataType = this.mapUserDefinedDataType(dataType,sizeConstraint)
        if (userDefinedDataType === undefined) {
          // console.log(this.TYPE_MAPPINGS)
          this.yadamuLogger.logInternalError([this.dbi.DATABASE_VENDOR,`MAPPING NOT FOUND`],`Missing Mapping for "${dataType}" in mappings for "${this.SOURCE_VENDOR}".`)
        }
        return userDefinedDataType
      }

      // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,this.SOURCE_VENDOR,dataTypeDefinition.type,dataTypeDefinition.length,dataTypeDefinition.scale],`Mapped to "${mappedDataType}".`)
      return this.refactorByLength(mappedDataType,dataTypeDefinition.length)
    }
    else {
      return this.generateStorageClause(dataType,sizeConstraint)
    }
  }

  generateDDLStatement(schema,tableName,columnDefinitions,mappedDataTypes) {
      
    return `create table "${schema}"."${tableName}"(\n  ${columnDefinitions.join(',')})`;
    
  }

  generateDMLStatement(schema,tableName,columnNames,insertOperators) {
    return `insert into "${schema}"."${tableName}" ("${columnNames.join('","')}") values (${insertOperators.join(',')})`;
  }

  getInsertOperator(mappedDataType) {
    return '?'
  }
  
  generateTableInfo(tableMetadata) {

    let insertMode = 'Batch';
   
    const columnNames = tableMetadata.columnNames
    
    const mappedDataTypes = [];
    const insertOperators = []
    
    const columnDefinitions = columnNames.map((columnName,idx) => {
      const sourceDataType = tableMetadata.dataTypes[idx]
      const mappedDataType = tableMetadata.source ? sourceDataType : this.getMappedDataType(sourceDataType,tableMetadata.sizeConstraints[idx])
      mappedDataTypes.push(mappedDataType)
      insertOperators.push(this.getInsertOperator(mappedDataType))
      return `"${columnName}" ${this.generateStorageClause(mappedDataType,tableMetadata.sizeConstraints[idx])}`    
    })
    
    const tableInfo =  { 
      ddl              : tableMetadata.source ? null : this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,mappedDataTypes)
    , dml              : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,columnNames,insertOperators)
    , columnNames      : tableMetadata.columnNames
    , targetDataTypes  : mappedDataTypes
    , virtualDataTypes : mappedDataTypes
    , insertMode       : insertMode
    , _BATCH_SIZE      : this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT  : this.dbi.INBOUND_SPATIAL_FORMAT
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





import fs                     from 'fs';

import YadamuDataTypes        from './yadamuDataTypes.js'

class YadamuStatementGenerator {
	  
  get SOURCE_VENDOR()        { return this._TARGET_VENDOR }
  set SOURCE_VENDOR(v)       { this._TARGET_VENDOR = v }
  
  get TYPE_CONFIGURATION()        { 
    this._TYPE_CONFIGURATION = this._TYPE_CONFIGURATION || (() => {
      this._TYPE_CONFIGURATION = JSON.parse(fs.readFileSync(YadamuDataTypes.DATA_TYPE_CONFIGURATION[this.SOURCE_VENDOR].file))
	  return this._TYPE_CONFIGURATION
    })()
    return this._TYPE_CONFIGURATION
  }

   
  /* async */ get VENDOR_DATA_TYPES()        { 
    this._VENDOR_DATA_TYPES = this._VENDOR_DATA_TYPES || (() => {
	  return new Promise(async(resolve,reject) => {
		const classFile = YadamuDataTypes.DATA_TYPE_CONFIGURATION[this.SOURCE_VENDOR].class
		const VendorDataTypes = (await import(classFile)).default
		const vendorDataTypes = new VendorDataTypes()
		// this.dbi.yadamu.initializeDataTypes(VendorDataTypes,JSON.parse(fs.readFileSync(YadamuDataTypes.DATA_TYPE_CONFIGURATION[this.SOURCE_VENDOR].file)))
		resolve(vendorDataTypes)
      })
	})()
    return this._VENDOR_DATA_TYPES
  }

  get SQL_TYPE_MAPPINGS()    {	  
	this._SQL_TYPE_MAPPINGS = this._SQL_TYPE_MAPPINGS || (() => {
  	  this._SQL_TYPE_MAPPINGS = new Map() 
	  for (let c = this.dbi.DATA_TYPES; c !== null; c = Object.getPrototypeOf(c)) {
        Object.getOwnPropertyNames(c).filter((name) => { 
	      return Object.getOwnPropertyDescriptor(c,name).get
	    }).forEach((name) => { 
	      const value = this.dbi.DATA_TYPES[name]
		  if ((!Array.isArray(value)) && typeof value !== 'object') {
	        this._SQL_TYPE_MAPPINGS.set(name,value)
		  } 
	    })
	  }
	  return this._SQL_TYPE_MAPPINGS
	})()
	return this._SQL_TYPE_MAPPINGS
  }
  
  get DEFINED_DATA_TYPES() {
	this._DEFINED_DATA_TYPES = this._DEFINED_DATA_TYPES || (() => {
      this._DEFINED_DATA_TYPES = []
	  for (let c = this.dbi.DATA_TYPES; c !== null; c = Object.getPrototypeOf(c)) {
        Object.getOwnPropertyNames(c).filter((name) => { 
	      return Object.getOwnPropertyDescriptor(c,name).get
	    }).forEach((name) => { 
		  const value = this.dbi.DATA_TYPES[name]
		  if ((!Array.isArray(value)) && typeof value !== 'object') {
	        this._DEFINED_DATA_TYPES.push(name)
		  } 
	    })
	  }
	  return this._DEFINED_DATA_TYPES
	})()
	return this._DEFINED_DATA_TYPES
  }
  
  /* async */ get VENDOR_TYPE_MAPPINGS() {

	/* 
	**
    const key = Object.keys(this.TYPE_CONFIGURATION).find(key => this.TYPE_CONFIGURATION[key] === dataTypeDefinition.type)
    let mappedDataType = this.dbi.DATA_TYPES[key]
	**
	*/
	
	this._VENDOR_TYPE_MAPPINGS = this._VENDOR_TYPE_MAPPINGS || (() => {
	  return new Promise(async(resolve,reject) => {
        const vendorTypeMappings = new Map()
	    const vendorDataTypes = await this.VENDOR_DATA_TYPES
		for (let c = vendorDataTypes; c !== null; c = Object.getPrototypeOf(c)) {
		  Object.getOwnPropertyNames(c).filter((name) => { 
	        return Object.getOwnPropertyDescriptor(c,name).get
	      }).forEach((name) => { 
		    const sourceType = vendorDataTypes[name]
			if (typeof sourceType === 'string') {
	           const sourceTypeDefinition = YadamuDataTypes.decomposeDataType(sourceType)
			   if (sourceType === sourceTypeDefinition.type) {
			    const targetType = this.dbi.DATA_TYPES[name]
		        if (typeof targetType === 'string') {
		          vendorTypeMappings.has(sourceType) ? vendorTypeMappings.set(sourceType,vendorTypeMappings.get(sourceType).add(targetType)) : vendorTypeMappings.set(sourceType,new Set().add(targetType))
				}
			  }
		    }   
	      })
		}
		// console.log(vendorTypeMappings)
		vendorTypeMappings.forEach((value,key) => {
		  vendorTypeMappings.set(key, value.size === 1 ? value.values().next().value : this.dbi.DATA_TYPES.coalesceTypeMappings(Array.from(value.values())))
		})
		// console.log(vendorTypeMappings)
		resolve(vendorTypeMappings)
	  })
	})()
    return this._VENDOR_TYPE_MAPPINGS
  }
  
  

  constructor(dbi, vendor, targetSchema, metadata, yadamuLogger) {

	this.dbi = dbi;
    this.SOURCE_VENDOR = vendor
    this.targetSchema = targetSchema
    this.metadata = metadata
    this.yadamuLogger = yadamuLogger;	

  }
  
  async debugStatementGenerator(options) {	
	
	console.log(JSON.stringify(this.metadata))
	console.log(JSON.stringify(Array.from( (await this.VENDOR_TYPE_MAPPINGS).entries())))
	console.log(options)
	
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
  
  mapDataType(dataType, length) {
	  
     const dataTypeDefinition = YadamuDataTypes.decomposeDataType(dataType)
     const key = Object.keys(this.TYPE_CONFIGURATION.mappings).find(key => this.TYPE_CONFIGURATION.mappings[key].toLowerCase() === dataTypeDefinition.type.toLowerCase())
	 let mappedDataType = this.dbi.DATA_TYPES[key]
	 switch (mappedDataType) {
	   case this.dbi.DATA_TYPES.CHAR_TYPE:
	     switch (true) {
		   case (length === undefined) :
		   case (length < 0) :
		   case (length > this.dbi.DATA_TYPES.CHAR_LENGTH) :
	         mappedDataType = this.dbi.DATA_TYPES.CLOB_TYPE
		     break; 
           default:			 
	     }
		 break
	   case this.dbi.DATA_TYPES.VARCHAR_TYPE:
	     switch (true) {
		   case (length === undefined) :
		   case (length < 0) :
		   case (length > this.dbi.DATA_TYPES.VARCHAR_LENGTH) :
	         mappedDataType = this.dbi.DATA_TYPES.CLOB_TYPE
		     break; 
           default:			 
	     }
		 break
	   default:
	 }
     // console.log(key,':',dataType,length,'==>',mappedDataType)
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

	  const mappedDataType = this.mapDataType(dataTypeDefinition.type,dataTypeDefinition.length);
   
      if (mappedDataType === undefined) {
		let userDefinedDataType = this.mapUserDefinedDataType(dataType,sizeConstraint)
	    if (userDefinedDataType === undefined) {
          this.yadamuLogger.logInternalError([this.dbi.DATABASE_VENDOR,`MAPPING NOT FOUND`],`Missing Mapping for "${dataType}" in mappings for "${this.SOURCE_VENDOR}".`)
	    }
		return userDefinedDataType
	  }

	  // this.yadamuLogger.trace([this.dbi.DATABASE_VENDOR,this.SOURCE_VENDOR,dataTypeDefinition.type,dataTypeDefinition.length,dataTypeDefinition.scale],`Mapped to "${mappedDataType}".`)
      return mappedDataType
	}
	else {
	  return this.generateStorageClause(dataType,sizeConstraint)
	}
  }
  
  validateTypeDefinition(typeDefinition) {
	switch (typeDefinition.type) {
	  case (this.dbi.DATA_TYPES.NUMERIC_TYPE):
	    typeDefinition.length > this.dbi.DATA_TYPES.NUMERIC_PRECISION ? this.dbi.DATA_TYPES.NUMERIC_PRECISION : typeDefinition.length
		typeDefinition.scale > this.dbi.DATA_TYPES.NUMERIC_SCALE ? this.dbi.DATA_TYPES.NUMERIC_SCALE : typeDefinition.scale
		break
	  default:
	}
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
	  
	  const dataTypeDefinition = {
		type: mappedDataType
	  }
      
      const sizeComponents = sizeConstraint.split(',')
      dataTypeDefinition.length = sizeComponents[0] === 'max' ? -1 :  parseInt(sizeComponents[0])
      dataTypeDefinition.scale = (sizeComponents.length > 1) ? parseInt(sizeComponents[1]) : undefined
	  
	  this.validateTypeDefinition(dataTypeDefinition)
	  
      if (dataTypeDefinition.scale && (dataTypeDefinition.scale > 0)) {
        return `${dataTypeDefinition.type}(${dataTypeDefinition.length},${dataTypeDefinition.scale})`
      }                                                   
      
      if (dataTypeDefinition.length && (dataTypeDefinition.length > 0)) {    
        return (dataTypeDefinition.type.includes(' ') && !dataTypeDefinition.type.startsWith('long')  &&!dataTypeDefinition.type.startsWith('bit')) ? dataTypeDefinition.type.replace(' ',`(${dataTypeDefinition.length}) `) : `${dataTypeDefinition.type}(${dataTypeDefinition.length})`
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

  getInsertOperator(mappedDataType) {
    return '?'
  }
  
  generateTableInfo(tableMetadata) {

    let insertMode = 'Batch';

    const columnNames = tableMetadata.columnNames
    
    const mappedDataTypes = [];
	const insertOperators = []
	
    const columnDefinitions = columnNames.map((columnName,idx) => {
				
      const mappedDataType = tableMetadata.source ? tableMetadata.dataTypes[idx] : this.getMappedDataType(tableMetadata.dataTypes[idx],tableMetadata.sizeConstraints[idx])
      mappedDataTypes.push(mappedDataType)
      insertOperators.push(this.getInsertOperator(mappedDataType))
	  return `"${columnName}" ${this.generateStorageClause(mappedDataType,tableMetadata.sizeConstraints[idx])}`	   
    })
	
    const tableInfo =  { 
      ddl              : this.tableMetadata.source ? null : this.generateDDLStatement(this.targetSchema,tableMetadata.tableName,columnDefinitions,mappedDataTypes)
    , dml              : this.generateDMLStatement(this.targetSchema,tableMetadata.tableName,columnNames,insertOperators)
    , columnNames      : tableMetadata.columnNames
    , sourceDataTypes  : tableMetadata.source ? tableMetadata.source.dataTypes : dataTypes
    , targetDataTypes  : mappedDataTypes
	, virtualDataTypes : mappedDataTypes
    , insertMode       : insertMode
    , _BATCH_SIZE      : this.dbi.BATCH_SIZE
    , _SPATIAL_FORMAT  : this.dbi.INBOUND_SPATIAL_FORMAT
    }
	
	return tableInfo
  }
  
  async generateStatementCache() {
    const statementCache = {}
    const tables = Object.keys(this.metadata);
	const ddlStatements = tables.map((table,idx) => {
      const tableMetadata = this.metadata[table];
      const tableInfo = this.generateTableInfo(tableMetadata);
      statementCache[this.metadata[table].tableName] = tableInfo;
      return tableInfo.ddl;
    })
	return statementCache;
  }

}

export { YadamuStatementGenerator as default }


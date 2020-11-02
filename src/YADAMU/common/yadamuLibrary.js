"use strict"

const fs = require('fs');
const path = require('path');
const {ConfigurationFileError} = require('./yadamuError.js');

class YadamuLibrary {

  static get BOOLEAN_DATA_TYPES() {
     this._BOOLEAN_DATA_TYPES = this._BOOLEAN_DATA_TYPES || Object.freeze(['BOOLEAN','BIT','RAW','TINYINT'])
	 return this._BOOLEAN_DATA_TYPES;
  }

  static get BOOLEAN_SIZE_CONSTRAINTS() {
     this._BOOLEAN_SIZE_CONSTRAINTS = this._BOOLEAN_SIZE_CONSTRAINTS || Object.freeze(['','','1','1'])
     return this._BOOLEAN_SIZE_CONSTRAINTS;
  }
  static get BINARY_DATA_TYPES() {
     this._BINARY_DATA_TYPES = this._BINARY_DATA_TYPES || Object.freeze(["RAW","BLOB","BINARY","VARBINARY","IMAGE","BYTEA","TINYBLOB","MEDIUMBLOB","LONGBLOB","ROWVERSION","OBJECTID","BINDATA"])
     return this._BINARY_DATA_TYPES;
  }

  static get DATE_DATA_TYPES() {
     this._DATE_DATA_TYPES = this._DATE_DATA_TYPES || Object.freeze(["DATE","TIME","TIMESTAMP","TIMEZONETZ","DATETIME","DATETIME2",,"TIMESTAMP WITH TIME ZONE","TIMESTAMP WITH LOCAL TIME ZONE","TIMESTAMP WITHOUT TIME ZONE","DATETIMEOFFSET","SMALLDATE","TIME WITHOUT TIME ZONE",""])
     return this._DATE_DATA_TYPES;
  }

  static get NUMERIC_DATA_TYPES() {
     this._NUMERIC_DATA_TYPES = this._NUMERIC_DATA_TYPES || Object.freeze(["NUMBER","BINARY_FLOAT","BINARY_DOUBLE","MONEY","SMALLMONEY","TINYINT","SMALLINT","INT","BIGINT","REAL","FLOAT","DOUBLE","DECIMAL","numeric","DOUBLE PRECISION","INTEGER","LONG"])
     return this._NUMERIC_DATA_TYPES;
  }

  static get XML_DATA_TYPES() {
     this._XML_DATA_TYPES = this._XML_DATA_TYPES || Object.freeze(["XML","XMLTYPE"])
     return this._XML_DATA_TYPES;
  }

  static get JSON_DATA_TYPES() {
     this._JSON_DATA_TYPES = this._JSON_DATA_TYPES || Object.freeze(["JSON","JSONB","SET","BFILE"])
     return this._JSON_DATA_TYPES;
  }

  static get CLOB_DATA_TYPES() {
     this._CLOB_DATA_TYPES = this._CLOB_DATA_TYPES || Object.freeze(["CLOB","NCLOB","JAVASCRIPTWITHSCOPE","LONGTEXT","MEDIUMTEXT","TEXT"])
     return this._CLOB_DATA_TYPES;
  }

  static get SPATIAL_DATA_TYPES() {
     this._SPATIAL_DATA_TYPES = this._SPATIAL_DATA_TYPES || Object.freeze(["\"MDSYS\".\"SDO_GEOMETRY\"","GEOGRAPHY","GEOMETRY"])
     return this._SPATIAL_DATA_TYPES;
  }

  static stringifyDuration(duration) {

  
   let milliseconds = 0
   let seconds = 0
   let minutes = 0
   let hours = 0
   let days = 0

   if (duration > 0) {
     milliseconds = Math.trunc(duration%1000)
     seconds = Math.trunc((duration/1000)%60)
     minutes = Math.trunc((duration/(1000*60))%60)
     hours = Math.trunc((duration/(1000*60*60))%24);
     days = Math.trunc(duration/(1000*60*60*24));
   }
  
    hours = (hours < 10) ? "0" + hours : hours;
    minutes = (minutes < 10) ? "0" + minutes : minutes;
    seconds = (seconds < 10) ? "0" + seconds : seconds;

    return (days > 0 ? `${days} days ` : '' ) + `${hours}:${minutes}:${seconds}.${(milliseconds + '').padStart(3,'0')}`;
  }
  
  static convertQuotedIdentifer(parameterValue) {

    if (parameterValue.startsWith('"') && parameterValue.endsWith('"')) {
      return parameterValue.slice(1,-1);
    }
    else {
      return parameterValue.toUpperCase()
    }	
  }
  
  static convertIdentifierCase(identifierCase, metadata) {
            
    switch (identifierCase) {
      case 'UPPER':
        for (let table of Object.keys(metadata)) {
          metadata[table].sqlColumnList = metadata[table].sqlColumnList.toUpperCase();
          if (table !== table.toUpperCase()){
            metadata[table].tableName = metadata[table].tableName.toUpperCase();
            Object.assign(metadata, {[table.toUpperCase()]: metadata[table]});
            delete metadata[table];
          }
        }           
        break;
      case 'LOWER':
        for (let table of Object.keys(metadata)) {
          metadata[table].sqlColumnList = metadata[table].sqlColumnList.toLowerCase();
          if (table !== table.toLowerCase()) {
            metadata[table].tableName = metadata[table].tableName.toLowerCase();
            Object.assign(metadata, {[table.toLowerCase()]: metadata[table]});
            delete metadata[table];
          } 
        }     
        break;         
      default: 
    }             
    return metadata
  }

  static pathSubstitutions(path) {
    return path.replace(/%date%/g,).replace(/%time%/g,).replace(/%isoDateTime%/,new Date().toISOString().replace(/:/g,'.'))
  }

  static isBooleanType(dataType,sizeConstraint) {
	const idx = this.BOOLEAN_DATA_TYPES.indexOf(dataType.toUpperCase())
	return ((idx > -1) && (this.BOOLEAN_SIZE_CONSTRAINTS[idx] === sizeConstraint))
  }
  
  static isBinaryDataType(dataType) {
	return this.BINARY_DATA_TYPES.includes(dataType.toUpperCase());
  }
   
  static isDateDataType(dataType) {
	return this.DATE_DATA_TYPES.includes(dataType.toUpperCase());
  }
   
  static isNumericDataType(dataType) {
	return this.NUMERIC_DATA_TYPES.includes(dataType.toUpperCase());
  }

  static isXML(dataType) {
    return this.XML_DATA_TYPES.includes(dataType.toUpperCase());
  }

  static isJSON(dataType) {
    return this.JSON_DATA_TYPES.includes(dataType.toUpperCase());
  }
  
  static isCLOB(dataType) {
    return this.CLOB_DATA_TYPES.includes(dataType.toUpperCase());
  }

  static isSpatialDataType(dataType) {
	return this.SPATIAL_DATA_TYPES.includes(dataType.toUpperCase());
  }

  static composeDataType(sourceDataType, sizeConstraint) {
    
    const dataType = {
      type : sourceDataType
    }    

    if ((sizeConstraint !== null) && (sizeConstraint.length > 0)) {
      const components = sizeConstraint.split(',');
      dataType.length = parseInt(components[0])
      if (components.length > 1) {
        dataType.scale = parseInt(components[1])
      }
    }
    
    return dataType
  }
  
  static decomposeDataType(targetDataType) {
    
    const results = {};
    let components = targetDataType.split('(');
    results.type = components[0].split(' ')[0];
    if (components.length > 1 ) {
      components = components[1].split(')');
      if (components.length > 1 ) {
        results.qualifier = components[1]
      }
      components = components[0].split(',');
      if (components.length > 1 ) {
        results.length = parseInt(components[0]);
        results.scale = parseInt(components[1]);
      }
      else {
        if (components[0] === 'max') {
          results.length = -1;
        }
        else {
          results.length = parseInt(components[0])
        }
      }
    }           
    return results;      
    
  } 
  
  static decomposeDataTypes(targetDataTypes) {
     return targetDataTypes.map((targetDataType) => {
       return this.decomposeDataType(targetDataType)
     })
  }
  
  static bfileToJSON(bfile) {
    return {dir: bfile.substr(bfile.indexOf("'"),bfile.indexOf(',')-2), file: bfile.substr(bfile.indexOf(',')+2,bfile.length-2)}
  }
  
  static jsonToBfile(jsonBfile) {
     return `BFILENAME('${jsonBfile.dir}','${jsonBfile.file}')`
  }
  
  static toBoolean(booleanValue) {
    const dataType = typeof booleanValue
    switch (dataType) {
      case 'boolean': 
        return booleanValue;
      case 'number':
        switch (booleanValue) {
          case 0:
            return false;
          case 1:
            return true;
          default:
            return undefined;
        }
        return undefined;
      case 'string':
        booleanValue = booleanValue.toUpperCase;
        switch(booleanValue) {
          case "TRUE":
          case "T":   
          case "YES":
          case "Y":
          case "01": // HexBinary 
          case "1":
            return true;
          case "FALSE":
          case "F":
          case "NO":
          case "N":
          case "00": // HexBinary
          case "0":
            return false;
          default:
            return undefined
        }
        return undefined;
      case 'object':
        if (Buffer.isBuffer(booleanValue)) {
          return booleanValue.length === 1 ? ((booleanValue[0] === 0) ? false : ((booleanValue[0] === 1) ? true : /* Not 0 or 1 */ undefined)) :  /* Length > 1 */ undefined
        }
      default: 
        return undefined
    }
  }
  
  static booleanToInt(booleanValue) {
    return this.toBoolean(booleanValue) === true ? 1 : 0
  }
  
  static booleanToBuffer(booleanValue) {
    return new Buffer.from([this.booleanToInt(booleanValue)])
  }
              
  static nameMatch(source,target,rule) {
    switch (rule)  {
      case 'EXACT':
        return source === target
      case 'UPPERCASE':
        return source.toUpperCase() === target
        break;
      case 'LOWERCASE': 
        return source.toLowerCase() === target
        break;
      case 'INSENSITIVE':
        return source.toLowerCase() === target.toLowerCase()
      default:
        return false;
    }
  }
  
  static isEmpty(obj) {
	for (const i in obj) {
	  return false;
	}
	return true;
  }
  
   static loadJSON(filePath,yadamuLogger) {
  
    /*
    ** 
    ** Use instead of 'requiring' configuration files. Avoids loading configuration files into node's "Require" cache
    **
    ** ### TODO : Check file exists and has reasonable upper limit for size before processeing
    ** 
    */ 
		 
	try {
      const fileContents = fs.readFileSync(filePath);
	  try {
	    return JSON.parse(fileContents);
      } catch (e) {
        const message = `JSON parsing error "${e.message}" while parsing "${filePath}".`;
		yadamuLogger.error(['YadamuLibrary','loadJSON()'],message)   
        throw new ConfigurationFileError(`[YadamuLibrary][loadJSON()] ${message}`) 
      } 
	} catch (e) {
      switch (true) {
		case (e.errno && (e.errno === -4058)):
		  const message = `Cannot load JSON file "${filePath}".`;
		  yadamuLogger.error(['YadamuLibrary','loadJSON()'],message)   
          throw new ConfigurationFileError(`[YadamuLibrary][loadJSON()] ${message}`) 
	    default:
          throw e;		
	  }
	}
  }
	 
  static loadIncludeFile(includePath,parentFile,yadamuLogger) {
	return this.loadJSON(path.basename(includePath) === includePath ? path.join(path.dirname(parentFile),includePath) : path.resolve(includePath),yadamuLogger)
  }	
  
  static macroSubstitions(string,macros) {
	Object.keys(macros).forEach((macro) => {
      const re = new RegExp(`%${macro}%`,'g')
	  string = string.replace(re,macros[macro])
	})
	return string
  }
  
}


module.exports = YadamuLibrary

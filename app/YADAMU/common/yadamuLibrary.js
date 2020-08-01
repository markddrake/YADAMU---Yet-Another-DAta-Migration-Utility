"use strict"

class YadamuLibrary {

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
  
  static isBinaryDataType(dataType) {
    const dataTypes = ["RAW","BLOB","BINARY","VARBINARY","IMAGE","BYTEA","TINYBLOB","MEDIUMBLOB","LONGBLOB","ROWVERSION","OBJECTID","BINDATA"]
	return dataTypes.includes(dataType.toUpperCase());
  }
   
  static isDateDataType(dataType) {
    const dataTypes = ["DATE","TIME","TIMESTAMP","TIMEZONETZ","DATETIME","DATETIME2",,"TIMESTAMP WITH TIME ZONE","TIMESTAMP WITH LOCAL TIME ZONE","TIMESTAMP WITHOUT TIME ZONE","DATETIMEOFFSET","SMALLDATE","TIME WITHOUT TIME ZONE",""]
	return dataTypes.includes(dataType.toUpperCase());
  }
   
  static isNumericDataType(dataType) {
    const dataTypes = ["NUMBER","BINARY_FLOAT","BINARY_DOUBLE","MONEY","SMALLMONEY","TINYINT","SMALLINT","INT","BIGINT","REAL","FLOAT","DOUBLE","DECIMAL","numeric","DOUBLE PRECISION","INTEGER","LONG"]
	return dataTypes.includes(dataType.toUpperCase());
  }

  static isXML(dataType) {
    const dataTypes = ["XML","XMLTYPE"]
	return dataTypes.includes(dataType.toUpperCase());
  }

  static isJSON(dataType) {
    const dataTypes = ["JSON","JSONB","SET","BFILE"]
	return dataTypes.includes(dataType.toUpperCase());
  }
  
  static isCLOB(dataType) {
    const dataTypes = ["CLOB","NCLOB","JAVASCRIPTWITHSCOPE","LONGTEXT","MEDIUMTEXT","TEXT"]
	return dataTypes.includes(dataType.toUpperCase());
  }

  static isSpatialDataType(dataType) {
    const dataTypes = ["\"MDSYS\".\"SDO_GEOMETRY\"","GEOGRAPHY","GEOMETRY"]
	return dataTypes.includes(dataType.toUpperCase());
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

}


module.exports = YadamuLibrary

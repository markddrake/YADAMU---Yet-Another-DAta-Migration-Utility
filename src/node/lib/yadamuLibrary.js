
import fs from 'fs';
import path from 'path';
import url from 'url';
import {YadamuError, UserError, CommandLineError, ConfigurationFileError, ConnectionError} from '../core/yadamuException.js';
import {FileNotFound} from '../dbi/file/fileException.js';1

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
        booleanValue = booleanValue.toUpperCase();
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
    return YadamuLibrary.toBoolean(booleanValue) === true ? 1 : 0
  }
  
  static booleanToBit(booleanValue) {
    return YadamuLibrary.toBoolean(booleanValue) === true ? 1 : 0
  }
  
  static booleanToString(booleanValue) {
    return YadamuLibrary.toBoolean(booleanValue) === true ? 'true' : 'false'
  }
  
  static booleanToBuffer(booleanValue) {
    return new Buffer.from([YadamuLibrary.booleanToInt(booleanValue)])
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
  

  static reportError(e) {
	if ((e instanceof UserError) || (e instanceof FileNotFound) || (e instanceof CommandLineError)) {
      console.log(e.message);
	  if (process.env.YADAMU_SHOW_CAUSE === 'TRUE') {	  
	    console.log(e); 
      }
  	}
	else {
      console.log(e);
  	}
  }
  
  static intervalYearMonthTo8601(interval) {
	let components = interval.split('-')
	return components.length > 1 ? `P${components[0]}Y${components[1]}M` : `P${components[0]}Y`
  }
  
  static intervalDaySecondTo8601(interval) {
	  
    let components = interval.split(' ')
	// Length > 1 : Interval contains Day and Time components.
	
	const days = components[0].includes(':') ? undefined : components[0]
	const time = components[0].includes(':') ? components[0] : components.length === 2 ? components[1] : undefined
	components = time ? time.split(':') : []
	const hours = components.length > 0 ? components[0] : undefined
	const mins  = components.length > 1 ? components[1] : undefined
	let secs  = components.length > 2 ? components[2] : undefined
    const interval8601 = `P${days ? `${days}D` : ''}${time ? `T${hours ? `${hours}H${mins ? `${mins}M` : ''}${secs ? `${secs}S` : ''}` : ''}` : ''}`
    return interval8601
	
  }
  
  static parse8601Interval(interval) {
	let results = {}
	// Strip the leading 'P'
	let remainingString = interval.substring(1);
	
	results.type = ((remainingString.indexOf('Y') > -1) || ((remainingString.indexOf('M') > -1) && ((remainingString.indexOf('T') === -1) || (remainingString.indexOf('M') < remainingString.indexOf('T'))))) ? 'YM' : 'DMS'

	let components
	if (remainingString.indexOf('Y') > -1) {
	  // Has a Years component - be careful: May also have minitues
	  components = remainingString.split('Y')
      results.years = Number(components[0]) 
	  if (components.length === 1) return results
      remainingString = components[1]
	}
	
	if ((remainingString.indexOf('M') > -1) && ((remainingString.indexOf('T') === -1) || (remainingString.indexOf('M') < remainingString.indexOf('T')))) {
      // Has a Months component - be careful: May also have minitues
	  components = remainingString.split('M')
      results.months = Number(components[0]) 
      if (components.length === 1) return results
	  components.shift()
      remainingString = components.join('M')
    }	   
      
    if (remainingString.indexOf('W') > -1) {
	  // Has a Weeks component
	  components = remainingString.split('W')
      results.weeks = Number(components[0]) 
      if (components.length === 1) return results
      remainingString = components[1];	
    }
    
    if (remainingString.indexOf('D') > -1) {
	  // Has a Days component
	  components = remainingString.split('D')
      results.days = Number(components[0]) 
      if (components.length === 1) return results
      remainingString = components[1];
    }
	
	if (remainingString.indexOf('T') > -1) {
	  components = remainingString.split('T')
	  if (components.length === 1) return results
      remainingString = components[1];
    }
    
	if (remainingString.indexOf('H') > -1) {
	  // Has a Hours component
	  components = remainingString.split('H')
      results.hours = Number(components[0]) 
      if (components.length === 1) return results
	  remainingString = components[1]
	}
	
    if (remainingString.indexOf('M') > -1) {
	  // Has a Minutes component
	  components = remainingString.split('M')
      results.minutes = Number(components[0]) 
      if (components.length === 1) return results
      remainingString = components[1];
	}
	
	if (remainingString.indexOf('S') > -1) {
 	  // Has a Weeks component
	  components = remainingString.split('S')
      results.seconds = Number(components[0]) 
	}
	
	return results
  }

  static  parseDTSIntervalUnits(interval) {
	
	const jsInterval = {}
	let components = interval.split(' ')
	
	
	switch (components.length) {
	  case 8:
        switch (components[7]) {
	      case "seconds":
          case "second":
   		    jsInterval.secs = components[6]
			jsInterval.time = true
		    break;
		  default:
		    console.log('Unparseable Interval:',components)  
        }
      case 6:
        switch (components[5]) {
	      case "minutes":
          case "minute":
   	        jsInterval.mins = components[4]
			jsInterval.time = true
		    break;
	      case "seconds":
          case "second":
   		    jsInterval.secs = components[4]
			jsInterval.time = true
		    break;
		  default:
		    console.log('Unparseable Interval:',components)  
        }
	  case 4:
        switch (components[3]) {
	      case "hours":
          case "hour":
   	        jsInterval.hours = components[2]
			jsInterval.time = true
		    break;
	      case "minutes":
          case "minute":
   	        jsInterval.mins = components[2]
			jsInterval.time = true
		    break;
	      case "seconds":
          case "second":
   		    jsInterval.secs = components[2]
			jsInterval.time = true
		    break;
		  default:
		    console.log('Unparseable Interval:',components)  
		}
	  case 2:	  
        switch (components[1]) {
          case "days":
          case "day":
            jsInterval.days = components[0]
	        break;
	      case "hours":
          case "hour":
   	        jsInterval.hours = components[0]
			jsInterval.time = true
		    break;
	      case "minutes":
          case "minute":
   	        jsInterval.mins = components[0]
			jsInterval.time = true
		    break;
	      case "seconds":
          case "second":
   		    jsInterval.secs = components[0]
			jsInterval.time = true
		    break;
		  default:
		    console.log('Unparseable Interval:',components)  
		}
        break
      default:
	    console.log('Unparseable Interval:',components)  
	}

    const interval8601 = `P${jsInterval.days ? `${jsInterval.days}D` : ''}${jsInterval.time ? `T${jsInterval.hours ? `${jsInterval.hours}H${jsInterval.mins ? `${jsInterval.mins}M` : ''}${jsInterval.secs ? `${jsInterval.secs}S` : ''}` : ''}` : ''}`
	return interval8601

  }

  static getVendorName(connectionProperties) {
    const keys = Object.keys(connectionProperties)
	
    switch (keys.length) {
      case 0:
	    throw new ConfigurationFileError('Empty Connection Object')
	  case 1: 
        return keys[0]
  	  default:
	    if (connectionProperties.hasOwnProperty("vendor") && connectionProperties.hasOwnProperty(connectionProperties.vendor)) {
	      return connectionProperties.vendor
	    }
		const vendor = keys.filter((key) => {return !["parameters","vendor","settings"].includes(key)})
		if (vendor.length !== 1) {
	      throw new ConfigurationFileError('Unable to determine vendor')
		}
		return vendor[0]
    } 
  }

  static getVendorSettings(connectionProperties) {
    return connectionProperties(this.getVendorName(connectionProperties))
  }
  
  static stripInsignificantZeros(numericValue) {
	return numericValue.replace(/(\.0*|(?<=(\..*))0*)$/, '')
	/*
	numericValue = !numericValue.includes('.') ? numericValue : (() => {
      const v = numericValue
	  let i = numericValue.length - 1
      while (i > 0)  {
        if ((numericValue[i] !== '0') || (numericValue[i-1] === '.')) {
		  console.log(v,i,numericValue.substring(0, i + 1))
          return numericValue.substring(0, i + 1);	  
        }
		i--
	  }
	})()
    return numericValue;
	*/
  }
}

export { YadamuLibrary as default}
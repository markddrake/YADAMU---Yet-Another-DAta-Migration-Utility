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
          metadata[table].columns = metadata[table].columns.toUpperCase();
          if (table !== table.toUpperCase()){
            metadata[table].tableName = metadata[table].tableName.toUpperCase();
            Object.assign(metadata, {[table.toUpperCase()]: metadata[table]});
            delete metadata[table];
          }
        }           
        break;
      case 'LOWER':
        for (let table of Object.keys(metadata)) {
          metadata[table].columns = metadata[table].columns.toLowerCase();
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
  
}

module.exports.convertQuotedIdentifer  = YadamuLibrary.convertQuotedIdentifer
module.exports.convertIdentifierCase  = YadamuLibrary.convertIdentifierCase
module.exports.stringifyDuration = YadamuLibrary.stringifyDuration

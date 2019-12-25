"use strict" 

const YadamuParser = require('../../common/yadamuParser.js')

class DBParser extends YadamuParser {
  
  constructor(tableInfo,objectMode,yadamuLogger) {
    super(tableInfo,objectMode,yadamuLogger);      
    this.columnList = JSON.parse(`[${this.tableInfo.COLUMN_LIST}]`);
    // this.dataTypes = JSON.parse(`[${this.tableInfo.DATA_TYPES}]`);    
  }

  async _transform (data,encodoing,done) {
    let output = []

    this.counter++;

    for (let i=0; i < this.columnList.length; i++) {
      if (data[this.columnList[i]] instanceof Buffer) {
        output.push(data[this.columnList[i]].toString('hex'))
      }
      else {
        /*
        if (this.dataTypes[i] === 'xml') {
          if (data[this.columnList[i]] !== null) {
            // ### Hack for MsSQL 's extremley strange and inconsistant use of entity encoding rules when ingesting XML
            output.push('<?xml version="1.0"?>' + data[this.columnList[i]].replace(/&#x0A;/gi,'\n').replace(/&#x20;/gi,' ')); 
          }
          else {
            output.push(data[this.columnList[i]]);
          }
        } 
        else {
          output.push(data[this.columnList[i]]);
        } 
        */
        output.push(data[this.columnList[i]]);
      }
    }
    if (!this.objectMode) {
      output = JSON.stringify(output)
    }
    this.push({data : output})
    done();
  }
}

module.exports = DBParser
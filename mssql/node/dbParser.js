"use strict" 

const Transform = require('stream').Transform;

class DBParser extends Transform {
  
  constructor(query,objectMode,logWriter) {
    super({objectMode: true });   
    this.query = query;
    this.objectMode = objectMode
    this.logWriter = logWriter;
    this.counter = 0
    this.columnList = JSON.parse(`[${this.query.COLUMN_LIST}]`);
    // this.dataTypes = JSON.parse(`[${this.query.DATA_TYPES}]`);    
  }

  getCounter() {
    return this.counter;
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
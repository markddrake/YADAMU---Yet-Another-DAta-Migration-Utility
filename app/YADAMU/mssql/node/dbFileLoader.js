"use strict";
const sql = require('mssql');
const Writable = require('stream').Writable

class DBFileLoader extends Writable {
     
  constructor(dbi) {
    super({})
    this.dbi = dbi;
  }
     
  async _write(chunk, encoding, next) {
    try {
      const data = chunk.toString()
      var results = await this.dbi.executeCachedStatement({C0: data})
      next(null,results);
    } catch (e) {
      next(e);
    }   
  } 
}   
 
module.exports = DBFileLoader

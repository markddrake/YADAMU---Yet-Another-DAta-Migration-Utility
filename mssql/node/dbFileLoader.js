"use strict";
const sql = require('mssql');
const Writable = require('stream').Writable

class DBFileLoader extends Writable {
     
  constructor(request,statement,status) {
    super({})
    this.request = request;
    this.statement = statement;
    this.status = status;
  }
     
  async _write(chunk, encoding, next) {
    try {
      const data = chunk.toString()
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(`${this.statement}\n\/\n`)
      }
      var results = await this.request.input('data',sql.NVARCHAR,data).batch(this.statement);
      next(null,results);
    } catch (e) {
      next(e);
    }   
  } 
}   
 
module.exports = DBFileLoader

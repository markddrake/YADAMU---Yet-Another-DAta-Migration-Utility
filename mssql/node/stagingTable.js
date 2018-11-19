"use strict";
const fs = require('fs');
const sql = require('mssql');
const DBFileLoader = require('./dbFileLoader');

class StagingTable {

  constructor(dbConn,tableSpec,importFilePath,status) {   
    this.request = new sql.Request(dbConn);    
    this.tableSpec = tableSpec;
    this.filePath = importFilePath;
    this.status = status;
  }
  
  async dropStagingTable() {
    try {
      const statement = `drop table if exists "${this.tableSpec.tableName}"`;
      if (this.status.sqlTrace) {
        this.status.sqlTrace.write(`${statement}\n\/\n`)
      }
      const results = await this.request.batch(statement)
      return results;
    } catch (e) {}
  }
    
  async createStagingTable() {
    const statement = `create table "${this.tableSpec.tableName}" ("${this.tableSpec.columnName}" NVARCHAR(MAX))`;
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${statement}\n\/\n`)
    }
    const results = await this.request.batch(statement)
    return results;
  } 

  async initializeStagingTable() {
    const statement = `insert into "${this.tableSpec.tableName}" values ('')`;
    if (this.status.sqlTrace) {
      this.status.sqlTrace.write(`${statement}\n\/\n`)
    }
    const results = await this.request.batch(statement)
    return results;
  } 
  
  async uploadFile() {

    let results = await this.dropStagingTable();
    results = await this.createStagingTable();
    results = await this.initializeStagingTable();


    const statement = `update "${this.tableSpec.tableName}" set "${this.tableSpec.columnName}" .write(@data,null,null)`;     
    const inputStream = fs.createReadStream(this.filePath);
    const loader = new DBFileLoader(this.request,statement,this.status);
  
    let startTime;
    return new Promise(function(resolve, reject) {
	  loader.on('finish',function(chunk) {resolve(new Date().getTime() - startTime)})
	  inputStream.on('error',function(err){reject(err)});
	  loader.on('error',function(err){reject(err)});
	  startTime = new Date().getTime();
      inputStream.pipe(loader);
    })

  }
}
 
module.exports = StagingTable;


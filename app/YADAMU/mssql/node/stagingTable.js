"use strict";

const fs = require('fs');
const { performance } = require('perf_hooks');

const sql = require('mssql');

const DBFileLoader = require('./dbFileLoader');

class StagingTable {

  constructor(dbi,tableSpec,importFilePath,status) {   
    this.dbi = dbi;
    this.tableSpec = tableSpec;
    this.filePath = importFilePath;
    this.status = status;
  }
  
  async uploadFile() {

    let results
	await this.dbi.beginTransaction();

    let statement = `drop table if exists "${this.tableSpec.tableName}"`;
	results = await this.dbi.executeBatch(statement)

	statement = `create table "${this.tableSpec.tableName}" ("${this.tableSpec.columnName}" NVARCHAR(MAX) collate ${this.dbi.defaultCollation})`;
    results = await this.dbi.executeBatch(statement)

    statement = `insert into "${this.tableSpec.tableName}" values ('')`;
    results = await this.dbi.executeBatch(statement)
  
    statement = `update "${this.tableSpec.tableName}" set "${this.tableSpec.columnName}" .write(@C0,null,null)`;  
	await this.dbi.cachePreparedStatement(statement, [{type : "nvarchar"}]) 
	
    const inputStream = await new Promise((resolve,reject) => {
      const inputStream = fs.createReadStream(this.filePath);
      inputStream.on('open',() => {resolve(inputStream)}).on('error',(err) => {reject(err)})
    })

    const loader = new DBFileLoader(this.dbi,this.status);
  
    let startTime;
    const fileUpload = new Promise((resolve, reject) => {
	  loader.on('finish',async (chunk) => {
		  try {
		    resolve(performance.now() - startTime)
		  } catch (e) {
			reject(e);
		  }
      })
	  inputStream.on('error',(err) => {reject(err)});
	  loader.on('error',(err) => {reject(err)});
	  startTime = performance.now();
      inputStream.pipe(loader);
    })
	
	const elapsedTime = await fileUpload;
    await this.dbi.clearCachedStatement(); 
    await this.dbi.commitTransaction();
	return elapsedTime;

  }
}
 
module.exports = StagingTable;


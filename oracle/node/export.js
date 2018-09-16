
"use strict";

const EXPORT_VERSION = 1.0;
const DATABASE_VENDOR = 'Oracle';

const fs = require('fs');
const common = require('./common.js');
const oracledb = require('oracledb');
const JSONStream = require('JSONStream')
const Transform = require('stream').Transform;
const Readable = require('stream').Readable;

async function getSystemInformation(conn) {    	
	const sqlStatement = `select NODE_EXPORT.JSON_FEATURES() JSON_FEATURES, 
	                             NODE_EXPORT.DATABASE_RELEASE() DATABASE_RELEASE, 
								 SYS_CONTEXT('USERENV','SESSION_USER') SESSION_USER, 
								 SYS_CONTEXT('USERENV','DB_NAME') DATABASE_NAME, 
								 SYS_CONTEXT('USERENV','SERVER_HOST') SERVER_HOST,
                                 JSON_OBJECTAGG(parameter, value) NLS_PARAMETERS
					        from NLS_DATABASE_PARAMETERS` ;
    const results = await conn.execute(sqlStatement,[],{outFormat: oracledb.OBJECT , fetchInfo: {COLUMN_LIST:{type: oracledb.STRING},DATA_TYPE_LIST:{type: oracledb.STRING},SIZE_CONSTRAINTS:{type: oracledb.STRING},SQL_STATEMENT:{type: oracledb.STRING}}});
	return results.rows[0];
}

async function generateQueries(conn,schema) {    	
	const sqlStatement = `select * from table(NODE_EXPORT.JSON_EXPORT_DDL(:schema))`;
    const results = await conn.execute(sqlStatement,{schema: schema},{outFormat: oracledb.OBJECT , fetchInfo: {
		                                                                                             COLUMN_LIST:{type: oracledb.STRING}
																									,DATA_TYPE_LIST:{type: oracledb.STRING}
																									,SIZE_CONSTRAINTS:{type: oracledb.STRING}
																									,EXPORT_SELECT_LIST:{type: oracledb.STRING}
																									,IMPORT_SELECT_LIST:{type: oracledb.STRING}
																									,COLUMN_PATTERN_LIST:{type: oracledb.STRING}
																									,DESERIALIZATION_INFO:{type: oracledb.STRING}
																									,SQL_STATEMENT:{type: oracledb.STRING}}});
	return results.rows;
}

function fetchDDL(conn,schema,outStream) {

  const ddlStatement = `select COLUMN_VALUE JSON from TABLE(JSON_EXPORT_DDL.FETCH_DDL_STATEMENTS(:schema))`;
    
  const parser = new Transform({objectMode:true});
  parser._transform = function(data,encodoing,done) {
	this.push(data.JSON);
	done();
  }
  
  return new Promise(async function(resolve,reject) {  
    const stream = await conn.queryStream(ddlStatement,{schema: schema},{outFormat: oracledb.OBJECT,fetchInfo:{JSON:{type: oracledb.STRING}}})
    stream.on('end',function() {resolve()})
	stream.on('error',function(err){console.log('Error'),reject(err)});
    stream.pipe(parser).pipe(JSONStream.stringify('[',',',']')).pipe(outStream,{end: false })
  })
}

async function fetchData(conn,sqlQuery,outStream) {

  let counter = 0;
  const parser = new Transform({objectMode:true});
  parser._transform = function(data,encodoing,done) {
	counter++;
	this.push(JSON.parse(data.JSON));
	done();
  }

  const stream = await conn.queryStream(sqlQuery,[],{outFormat: oracledb.OBJECT,fetchInfo:{JSON:{type: oracledb.STRING}}})
  const jsonStream = JSONStream.stringify('[',',',']');
  
  return new Promise(function(resolve,reject) {  
    jsonStream.on('end',function() {resolve(counter)})
    // outStream.on('finish',function() {resolve(counter)})
    stream.on('error',function(err){console.log('Error'),reject(err)});
    stream.pipe(parser).pipe(jsonStream).pipe(outStream,{end: false })
  })
}

function writeTableName(i,tableName,outStream) {

  const s = new Readable();
  if (i > 0) {
	s.push(',');
  }
  s.push(tableName);
  s.push(null);
	
  return new Promise(function(resolve,reject) {  
    s.on('end',function() {resolve()})
	s.pipe(outStream,{end: false })
  })
}

async function main(){
  try {
    const parameters = common.processArguments(process.argv,'export');
	const schema = parameters.OWNER;
	
    const dumpFilePath = parameters.FILE;
    const dumpFile = fs.createWriteStream(dumpFilePath);
    dumpFile.on('error',function(err) {console.log(err)})
	
	let logWriter = process.stdout;
	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }
	
	const conn = await common.doConnect(parameters.USERID);
	
	const sysInfo = await getSystemInformation(conn);
	dumpFile.write('{"systemInformation":');
	dumpFile.write(JSON.stringify({
		                 "date"               : new Date().toISOString()
					    ,"vendor"             : DATABASE_VENDOR
					    ,"schema"             : schema
					    ,"exportVersion"      : EXPORT_VERSION
	    				,"sessionUser"        : sysInfo.SESSION_USER
						,"dbName"             : sysInfo.DATABASE_NAME
						,"serverHostName"     : sysInfo.SERVER_HOST
						,"databaseVersion"    : sysInfo.DATABASE_RELEASE
						,"jsonFeatures"       : JSON.parse(sysInfo.JSON_FEATURES)
						,"nlsParameters"      : JSON.parse(sysInfo.NLS_PARAMETERS)
	}));


    if (parameters.MODE !== 'DATA_ONLY') {
  	  dumpFile.write(',"ddl":');
	  await fetchDDL(conn,schema,dumpFile);
	}
	
	if (parameters.MODE !== 'DDL_ONLY') {
	  dumpFile.write(',"metadata":{');
	  const sqlQueries = await generateQueries(conn,schema);
	  for (let i=0; i < sqlQueries.length; i++) {
	    if (i > 0) {
	  	  dumpFile.write(',');
        }
	    dumpFile.write(`"${sqlQueries[i].TABLE_NAME}" : ${JSON.stringify({
		   				                                   "owner"                    : sqlQueries[i].OWNER
                                                          ,"tableName"                : sqlQueries[i].TABLE_NAME
                                                          ,"columns"                  : sqlQueries[i].COLUMN_LIST
                                                          ,"dataTypes"                : sqlQueries[i].DATA_TYPE_LIST
														  ,"dataTypeSizing"           : sqlQueries[i].SIZE_CONSTRAINTS
														  ,"exportSelectList"         : sqlQueries[i].EXPORT_SELECT_LIST
														  ,"insertSelectList"         : sqlQueries[i].IMPORT_SELECT_LIST
														  ,"deserializationFunctions" : sqlQueries[i].DESERIALIZATION_INFO
														  ,"columnPatterns"           : sqlQueries[i].COLUMN_PATTERN_LIST
	                  })}`)				   
	  }
	
	  dumpFile.write('},"data":{');
	   for (let i=0; i < sqlQueries.length; i++) {
	    const startTime = new Date().getTime()
	    await writeTableName(i,`"${sqlQueries[i].TABLE_NAME}" :`,dumpFile);
        const rows = await fetchData(conn,sqlQueries[i].SQL_STATEMENT,dumpFile) 
        const elapsedTime = new Date().getTime() - startTime
	    logWriter.write(`${new Date().toISOString()} - Table: "${sqlQueries[i].TABLE_NAME}. Rows: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
	  }
      dumpFile.write('}');
	}
    
    dumpFile.write('}');
	dumpFile.close();
	conn.close();
	console.log('Export Operation completed successfully.');
  } catch (e) {
	console.log('Export operation Failed.');
    console.log(e);
	conn.close();
  }
}

main();

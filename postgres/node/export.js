"use strict";
 
const connectionDetails = {
  user: 'postgres',
  host: '192.168.1.250',
  database: 'postgres',
  password: null,
  port: 5432,
}

const fs = require('fs');
const common = require('./common.js');
const {Client} = require('pg')
const QueryStream = require('pg-query-stream')
const JSONStream = require('JSONStream')
const Transform = require('stream').Transform;

const pgClient = new Client(connectionDetails);

async function getSystemInformation(pgClient) {    	
	const sqlStatement = `select current_database() database_name,current_user,session_user,current_setting('server_version_num') database_version`;					   
	const results = await pgClient.query(sqlStatement);
	return results.rows[0];
}

async function generateQueries(pgClient,schema) {    	
	const sqlStatement = `select table_schema, table_name, 
	                             string_agg('"' || column_name || '"',',') "columns", 
								 string_agg('"' || data_type || '"',',') "dataTypes", 
								 string_agg('"' || case when data_type = 'numeric' 
								                           then numeric_precision || ',' || numeric_scale
														when data_type = 'character varying' 
														  then cast(character_maximum_length as varchar)
														when data_type = 'character' 
														  then cast(character_maximum_length as varchar)
														else
														  ''
												  end
								 || '"',',') "sizeConstraints",
								 'select jsonb_build_array(' || string_agg('"' || column_name || '"',',')|| ') "json" from "' || table_schema || '"."' || table_name ||'"' QUERY 
					        from information_schema.columns 
						   where table_schema = $1 group by table_schema, table_name`;
						   
	const results = await pgClient.query(sqlStatement,[schema]);
	return results.rows;
}

function fetchData(pgClient,sqlQuery,outStream) {

  let counter = 0;

  const parser = new Transform({objectMode:true});
  parser._transform = function(data,encodoing,done) {
	counter++;
	this.push(data.json);
	done();
  }
 
  const query = new QueryStream(sqlQuery)
  const jsonStream = JSONStream.stringify('[',',',']');
  
  return new Promise(async function(resolve,reject) {  
    const stream = await pgClient.query(query)
    jsonStream.on('end',function() {resolve(counter)})
	stream.on('error',function(err){console.log('Error'),reject(err)});
    stream.pipe(parser).pipe(jsonStream).pipe(outStream,{end: false })
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
	
	pgClient.on('notification',function(msg){ console.log(msg)});
	
	await pgClient.connect();
    
	const sysInfo = await getSystemInformation(pgClient);
	dumpFile.write('{"systemInformation":');
	dumpFile.write(JSON.stringify({
		                 "date" : new Date().toISOString()
					    ,"vendor" : "Postges"
					    ,"schema" : schema
					    ,"exportVersion": 1
						,"sessionUser" : sysInfo.session_user
						,"dbName" : sysInfo.database_name
						,"databaseVersion" : sysInfo.database_version
	}));
	
	
	dumpFile.write(',"metadata":{');
	const sqlQueries = await generateQueries(pgClient,schema);
	for (let i=0; i < sqlQueries.length; i++) {
	  if (i > 0) {
		dumpFile.write(',');
      }
	  dumpFile.write(`"${sqlQueries[i].table_name}" : ${JSON.stringify({
						                                 "owner"          : sqlQueries[i].table_schema
                                                        ,"tableName"      : sqlQueries[i].table_name
                                                        ,"columns"        : sqlQueries[i].columns
                                                        ,"dataTypes"      : sqlQueries[i].dataTypes
														,"dataTypeSizing" : sqlQueries[i].sizeConstraints
	                 })}`)				   
	}
	dumpFile.write('},"data":{');
	for (let i=0; i < sqlQueries.length; i++) {
	  const startTime = new Date().getTime()
	  if (i > 0) {
		dumpFile.write(',');
      }
	  dumpFile.write(`"${sqlQueries[i].table_name}" :`);
      const rows = await fetchData(pgClient,sqlQueries[i].query,dumpFile) 
      const elapsedTime = new Date().getTime() - startTime
      logWriter.write(`${new Date().toISOString()} - Table: "${sqlQueries[i].table_name}. Rows: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
	}

    dumpFile.write('}}');
	dumpFile.close();
	await pgClient.end();
	console.log('Export Operation completed successfully.');
  } catch (e) {
	console.log('Export operation Failed.');
	console.log(e)
	await pgClient.end();
  }
}

main();
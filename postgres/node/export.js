"use strict";
 
const fs = require('fs');
const common = require('./common.js');
const {Client} = require('pg')
const QueryStream = require('pg-query-stream')
const JSONStream = require('JSONStream')
const Transform = require('stream').Transform;


const sqlGetSystemInformation =
`select current_database() database_name,current_user,session_user,current_setting('server_version_num') database_version`;					   

const sqlGenerateQueries =
`select t.table_schema, t.table_name
	   ,string_agg('"' || column_name || '"',',' order by ordinal_position) "columns" 
	   ,string_agg('"' || data_type || '"',',' order by ordinal_position) "dataTypes"
       ,string_agg('"' ||
                   case
                     when (numeric_precision is not null) and (numeric_scale is not null) 
                       then cast(numeric_precision as varchar) || ',' || cast(numeric_scale as varchar)
                     when (numeric_precision is not null) 
                       then cast(numeric_precision as varchar)
                     when (character_maximum_length is not null)
                       then cast(character_maximum_length as varchar)
                     else
                       ''
                   end
                   || '"'
                  ,',' order by ordinal_position
                 ) "sizeConstraints"
	   ,'select jsonb_build_array(' || string_agg('"' || column_name || '"',',' order by ordinal_position)|| ') "json" from "' || t.table_schema || '"."' || t.table_name ||'"' QUERY 
   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
	and t.table_schema = c.table_schema
	and t.table_type = 'BASE TABLE'
    and t.table_schema = $1
  group by t.table_schema, t.table_name`;

async function getSystemInformation(pgClient) {    	

	const results = await pgClient.query(sqlGetSystemInformation);
	return results.rows[0];
}

async function generateQueries(pgClient,schema) {    	
						   
  const results = await pgClient.query(sqlGenerateQueries,[schema]);
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
	stream.on('error',function(err){reject(err)});
    stream.pipe(parser).pipe(jsonStream).pipe(outStream,{end: false })
  })
}

async function main(){

  let pgClient = undefined;
  let parameters = undefined;
  let sqlTrace = undefined;
  let logWriter = process.stdout;

  try {
    parameters = common.processArguments(process.argv,'export');

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }

    if (parameters.SQLTRACE) {
      sqlTrace = fs.createWriteStream(parameters.SQLTRACE);
    }
	
	const connectionDetails = {
      user      : parameters.USERNAME
     ,host      : parameters.HOSTNAME
     ,database  : parameters.DATABASE
     ,password  : parameters.PASSWORD
     ,port      : parameters.PORT
    }

    pgClient = new Client(connectionDetails);
	pgClient.on('notice',function(msg){ console.log(msg)});
	await pgClient.connect();

    const dumpFilePath = parameters.FILE;	
    const dumpFile = fs.createWriteStream(dumpFilePath);
    // dumpFile.on('error',function(err) {console.log(err)})
		
	const schema = parameters.OWNER;
    if (parameters.SQLTRACE) {
      sqlTrace.write(`${sqlGetSystemInformation}\n\/\n`)
    }
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
    if (parameters.SQLTRACE) {
      sqlTrace.write(`${sqlGenerateQueries}\n\/\n`)
    }
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
	  if (i > 0) {
		dumpFile.write(',');
      }
	  dumpFile.write(`"${sqlQueries[i].table_name}" :`);
	  const startTime = new Date().getTime()
      if (parameters.SQLTRACE) {
        sqlTrace.write(`${sqlQueries[i].query}\n\/\n`)
      }
      const rows = await fetchData(pgClient,sqlQueries[i].query,dumpFile) 
      const elapsedTime = new Date().getTime() - startTime
      logWriter.write(`${new Date().toISOString()} - Table: "${sqlQueries[i].table_name}". Rows: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
	}

    dumpFile.write('}}');
	dumpFile.close();

	await pgClient.end();
	logWriter.write('Export operation successful.');
    if (logWriter !== process.stdout) {
	  console.log(`Export operation successful: See "${parameters.LOGFILE}" for details.`);
    }
  } catch (e) {
    if (logWriter !== process.stdout) {
	  console.log(`Export operation failed: See "${parameters.LOGFILE}" for details.`);
  	  logWriter.write('Export operation failed.\n');
	  logWriter.write(e.stack);
    }
	else {
    	console.log('Export operation Failed.');
        console.log(e);
	}
    if (pgClient !== undefined) {
  	  await pgClient.end();
	}
  }
  
  if (logWriter !== process.stdout) {
	logWriter.close();
  }
  
  if (parameters.SQLTRACE) {
    sqlTrace.close();
  }
  
}

main();
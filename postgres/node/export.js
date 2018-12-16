"use strict";
const fs = require('fs');
const {Client} = require('pg')
const QueryStream = require('pg-query-stream')
const JSONStream = require('JSONStream')
const Transform = require('stream').Transform;
const path = require('path');

const Yadamu = require('../../common/yadamuCore.js');
const PostgresCore = require('./postgresCore.js');

const sqlGetSystemInformation =
`select current_database() database_name,current_user,session_user,current_setting('server_version_num') database_version`;					   

const sqlGenerateQueries =
`select t.table_schema, t.table_name
	   ,string_agg('"' || column_name || '"',',' order by ordinal_position) "columns" 
	   ,jsonb_agg(case when data_type = 'USER-DEFINED' then udt_name else data_type end order by ordinal_position) "dataTypes"
       ,jsonb_agg(case
                     when (numeric_precision is not null) and (numeric_scale is not null) 
                       then cast(numeric_precision as varchar) || ',' || cast(numeric_scale as varchar)
                     when (numeric_precision is not null) 
                       then cast(numeric_precision as varchar)
                     when (character_maximum_length is not null)
                       then cast(character_maximum_length as varchar)
                     else
                       ''
                   end
                   order by ordinal_position
                 ) "sizeConstraints"
	   ,'select jsonb_build_array(' || string_agg('"' || column_name || '"' ||
                                                  case   
                                                    when data_type = 'xml' then
                                                       '::text'
                                                    else
                                                      ''
                                                  end
                                                 ,',' order by ordinal_position
                                                 ) || ') "json" from "' || t.table_schema || '"."' || t.table_name ||'"' QUERY 
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

async function fetchData(pgClient,sqlQuery,outStream) {

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

  let pgClient;
  let parameters;
  let logWriter = process.stdout;
  let status;

  try {
    parameters = PostgresCore.processArguments(process.argv);
    status = Yadamu.getStatus(parameters,'Export');

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
    }

	pgClient = await PostgresCore.getClient(parameters,logWriter,status);

    const exportFilePath = path.resolve(parameters.FILE);
    const exportFile = fs.createWriteStream(exportFilePath);
    // exportFile.on('error',function(err) {console.log(err)})
    logWriter.write(`${new Date().toISOString()}[Export]: Generating file "${exportFilePath}".\n`)
    

    if (status.sqlTrace) {
      status.sqlTrace.write(`${sqlGetSystemInformation}\n\/\n`)
    }
    const sysInfo = await getSystemInformation(pgClient);
	exportFile.write('{"systemInformation":');
	exportFile.write(JSON.stringify({
		                 "date" : new Date().toISOString()
                        ,"timeZoneOffset"  : new Date().getTimezoneOffset()
					    ,"vendor" : "Postges"
					    ,"schema" :  parameters.OWNER
					    ,"exportVersion": 1
						,"sessionUser" : sysInfo.session_user
						,"dbName" : sysInfo.database_name
						,"databaseVersion" : sysInfo.database_version
	}));
	
	
	exportFile.write(',"metadata":{');
    if (status.sqlTrace) {
      status.sqlTrace.write(`${sqlGenerateQueries}\n\/\n`)
    }
	const sqlQueries = await generateQueries(pgClient, parameters.OWNER);
	for (let i=0; i < sqlQueries.length; i++) {
	  if (i > 0) {
		exportFile.write(',');
      }

	  exportFile.write(`"${sqlQueries[i].table_name}" : ${JSON.stringify({
						                                 "owner"           : sqlQueries[i].table_schema
                                                        ,"tableName"       : sqlQueries[i].table_name
                                                        ,"columns"         : sqlQueries[i].columns
                                                        ,"dataTypes"       : sqlQueries[i].dataTypes
														,"sizeConstraints" : sqlQueries[i].sizeConstraints
	                 })}`)				   
	}
	exportFile.write('},"data":{');
	for (let i=0; i < sqlQueries.length; i++) {
	  if (i > 0) {
		exportFile.write(',');
      }
	  exportFile.write(`"${sqlQueries[i].table_name}" :`);
	  const startTime = new Date().getTime()
      const sqlStatement = sqlQueries[i].query
      if (status.sqlTrace) {
        status.sqlTrace.write(`${sqlStatement}\n\/\n`)
      }
      const rows = await fetchData(pgClient,sqlStatement,exportFile) 
      const elapsedTime = new Date().getTime() - startTime
      logWriter.write(`${new Date().toISOString()} - Table: "${sqlQueries[i].table_name}". Rows: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
	}

    exportFile.write('}}');
	exportFile.close();

	await pgClient.end();
    Yadamu.reportStatus(status,logWriter);
  } catch (e) {
    if (logWriter !== process.stdout) {
	  console.log(`Export operation failed: See "${parameters.LOGFILE}" for details.`);
  	  logWriter.write('Export operation failed.\n');
      logWriter.write(`${e}\n${e.stack}\n`);
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
  
  if (status.sqlTrace) {
    status.sqlTrace.close();
  }
  
}

main();
"use strict";
 
const fs = require('fs');
const common = require('./common.js');
const mysql = require('mysql');
const JSONStream = require('JSONStream')
const Transform = require('stream').Transform;

function connect(conn) {
	
  return new Promise(function(resolve,reject) {
	                   conn.connect(function(err) {
		                              if (err) {
		                                reject(err);
	                                  }
  			                          resolve();
                                    })
				    })
}	
	  
function query(conn,sqlQuery,args) {
	
  return new Promise(function(resolve,reject) {
	                   conn.query(sqlQuery,args,function(err,rows,fields) {
		                                     if (err) {
		                                       reject(err);
	                                         }
											 resolve(rows);
                                           })
                     })
}  

async function getSystemInformation(conn) {    	
	const sqlStatement = `select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION"`;					   
	const results = await query(conn,sqlStatement); 
	return results;
}

async function generateQueries(conn,schema) {    	
	const sqlStatement = 
 `select table_schema
        ,table_name
		,group_concat(concat('"',column_name,'"') separator ',')  "columns"
		,group_concat(concat('"',data_type,'"') separator ',')  "dataTypes"
		,group_concat(concat('"',	
                             case when data_type = 'decimal'	
                                    then concat(numeric_precision,',',numeric_scale)	
                                  when data_type = 'varchar'	
                                    then character_maximum_length	
                                  when data_type = 'char'	
                                    then character_maximum_length	
                                  when data_type = 'character'	
                                    then character_maximum_length	
                                  else	
                                    ''	
                             end,	
                             '"') separator ',') "sizeConstraints"
	    ,concat('select json_array(',group_concat('"',column_name,'"'),') "json" from "',table_schema,'"."',table_name,'"') QUERY	
    from information_schema.columns	
   where table_schema = ? group by table_schema, table_name`;

   const results = await query(conn,sqlStatement,[schema]);
   return results;
}

function fetchData(conn,sqlQuery,outStream) {

  let counter = 0;
  const parser = new Transform({objectMode:true});
  parser._transform = function(data,encodoing,done) {
	counter++;
	this.push(data.json);
	done();
  }
 
  const jsonStream = JSONStream.stringify('[',',',']');
  
  return new Promise(async function(resolve,reject) {  
    const stream = conn.query(sqlQuery).stream();
    jsonStream.on('end',function() {resolve(counter)})
	stream.on('error',function(err){reject(err)});
    stream.pipe(parser).pipe(jsonStream).pipe(outStream,{end: false })
  })
}

async function main(){
	
  let conn = undefined;
  let parameters = undefined;
  let logWriter = process.stdout;
	
  try {

    parameters = common.processArguments(process.argv,'export');

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }
	
    const connectionDetails = {
            host      : parameters.HOSTNAME
           ,user      : parameters.USERNAME
           ,password  : parameters.PASSWORD
    }

    conn = mysql.createConnection(connectionDetails);
	await connect(conn);
    await query(conn,'SET SESSION SQL_MODE=ANSI_QUOTES');
	
    const dumpFilePath = parameters.FILE;	
    const dumpFile = fs.createWriteStream(dumpFilePath);
    dumpFile.on('error',function(err) {console.log(err)})
	
    const schema = parameters.OWNER;
	const sysInfo = await getSystemInformation(conn);
	console.log
	dumpFile.write('{"systemInformation":');
	dumpFile.write(JSON.stringify({
		                 "date"            : new Date().toISOString()
					    ,"vendor"          : "MySQL"
					    ,"schema"          : schema
					    ,"exportVersion"   : 1
						,"sessionUser"     : sysInfo[0].SESSION_USER
						,"currentUser"     : sysInfo[0].CURRENT_USER
						,"dbName"          : sysInfo[0].DATABASE_NAME
						,"databaseVersion" : sysInfo[0].DATABASE_VERSION
	}));
	
	dumpFile.write(',"metadata":{');
	const sqlQueries = await generateQueries(conn,schema);
	for (let i=0; i < sqlQueries.length; i++) {
	  const row = sqlQueries[i];
	  if (i > 0) {
		dumpFile.write(',');
      }
	  dumpFile.write(`"${row.TABLE_NAME}" : ${JSON.stringify({
						                             "owner"          : row.TABLE_SCHEMA
                                                    ,"tableName"      : row.TABLE_NAME
                                                    ,"columns"        : row.columns
                                                    ,"dataTypes"      : row.dataTypes
												    ,"dataTypeSizing" : row.sizeConstraints
	                 })}`)				   
	}
	dumpFile.write('},"data":{');
	for (let i=0; i < sqlQueries.length; i++) {
      const row = sqlQueries[i]
	  const startTime = new Date().getTime()
	  if (i > 0) {
		dumpFile.write(',');
      }
	  dumpFile.write(`"${row.TABLE_NAME}" :`);
      const rows = await fetchData(conn,row.QUERY,dumpFile) 
      const elapsedTime = new Date().getTime() - startTime
      logWriter.write(`${new Date().toISOString()} - Table: "${row.TABLE_NAME}". Rows: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
	}

    dumpFile.write('}}');
	dumpFile.close();
	
	await conn.end();
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
	if (conn !== undefined) {
      await conn.end();
	}
  }
  
  if (logWriter !== process.stdout) {
	logWriter.close();
  }  
}

main();
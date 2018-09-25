"use strict";
 
const fs = require('fs');
const common = require('./common.js');
const sql = require('mssql');

async function getSystemInformation(dbConn) {    	
	const sqlStatement = `select db_Name() "DATABASE_NAME", current_user "CURRENT_USER", session_user "SESSION_USER", CONVERT(NVARCHAR(20),SERVERPROPERTY('ProductVersion')) "DATABASE_VERSION",CONVERT(NVARCHAR(128),SERVERPROPERTY('MachineName')) "HOSTNAME"`;					   
	const results = await await dbConn.query(sqlStatement);
	return results.recordsets[0];
}

async function generateQueries(dbConn,schema) {    	
	const sqlStatement = 
`select t.table_schema
        ,t.table_name
		,STRING_AGG('"' + column_name + '"',',')  "columns"
		,STRING_AGG('"' + data_type   + '"',',')  "dataTypes"
		,STRING_AGG('"'
		          + case 
				      when data_type = 'decimal'	
                        then  cast(numeric_precision as VARCHAR) + ',' + cast(numeric_scale as VARCHAR)
                      when data_type = 'varchar'	
                        then cast(character_maximum_length as varchar)
                      when data_type = 'char'	
                        then cast(character_maximum_length as varchar)
                      when data_type = 'character'	
                        then cast(character_maximum_length as varchar)
                      else	
                         ''	
                    end
                  + '"',',') "sizeConstraints"
	      -- ,'select ' + STRING_AGG('"' + column_name + '" as [' + cast(ORDINAL_POSITION as VARCHAR) + ']',',') + ' from "' + t.table_schema + '"."' + t.table_name + '"' QUERY	
	      ,'select ' + STRING_AGG('"' + column_name + '"',',') + ' from "' + t.table_schema + '"."' + t.table_name + '"' QUERY	
      from information_schema.columns c, information_schema.tables t
     where t.table_name = c.table_name 
	   and t.table_schema = c.table_schema
	   and t.table_type = 'BASE TABLE'
       and t.table_schema = 'dbo' 
	 group by t.table_schema, t.table_name`;	
  
   const results = await dbConn.query(sqlStatement,[schema]);
   return results.recordsets[0]
}

function fetchData(request,tableInfo,outStream) {

  outStream.write('[');
  request.stream = true // You can set streaming differently for each request
 
  let counter = 0;

  return new Promise(async function(resolve,reject) {  
     
	 request.on('done', function(result) {
	   outStream.write(']');
	   resolve(counter)
    })

	const column_list = JSON.parse(`[${tableInfo.columns}]`);

    request.on('row', function(row){
		counter++
		const array = []
		for (let i=0; i < column_list.length; i++) {
			if (row[column_list[i]] instanceof Buffer) {
			  array.push(row[column_list[i]].toString('hex'))
			}
			else {
			  array.push(row[column_list[i]]);
			}
		}
		if (counter > 1) {
		  outStream.write(',');
		}
		outStream.write(JSON.stringify(array));
    })

    request.on('error',function(err) {
		{reject(err)}
    })
  
    request.query(tableInfo.QUERY) // or request.execute(procedure)

  })
}

async function main(){
	
  let parameters = undefined;
  let logWriter = process.stdout;
	
  try {

    parameters = common.processArguments(process.argv,'export');

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }
	
	const config = {
      server    : parameters.HOSTNAME
     ,user      : parameters.USERNAME
     ,database  : parameters.DATABASE
     ,password  : parameters.PASSWORD
     ,port      : parameters.PORT
	 ,options: {
        encrypt: false // Use this if you're on Windows Azure
      }
    }

    const dbConn = new sql.ConnectionPool(config);
	await dbConn.connect()
	
    const dumpFilePath = parameters.FILE;	
    const dumpFile = fs.createWriteStream(dumpFilePath);
    dumpFile.on('error',function(err) {console.log(err)})
	
    const schema = parameters.OWNER;
	
	const sysInfo = await getSystemInformation(dbConn);
	console.log(sysInfo);
	dumpFile.write('{"systemInformation":');
	dumpFile.write(JSON.stringify({
		                 "date"            : new Date().toISOString()
					    ,"vendor"          : "MSSQLSERVER"
					    ,"schema"          : schema
					    ,"exportVersion"   : 1
						,"sessionUser"     : sysInfo[0].SESSION_USER
						,"currentUser"     : sysInfo[0].CURRENT_USER
						,"dbName"          : sysInfo[0].DATABASE_NAME
						,"databaseVersion" : sysInfo[0].DATABASE_VERSION
						,"hostname"        : sysInfo.HOSTNAME
	}));

	dumpFile.write(',"metadata":{')
	const sqlQueries = await generateQueries(dbConn,schema);

	for (let i=0; i < sqlQueries.length; i++) {
	  const row = sqlQueries[i];
	  if (i > 0) {
		dumpFile.write(',');
      }
	  dumpFile.write(`"${row.table_name}" : ${JSON.stringify({
						                             "owner"          : row.table_schema
                                                    ,"tableName"      : row.table_name
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
	  dumpFile.write(`"${row.table_name}" :`);
      const rows = await fetchData(new sql.Request(dbConn),row,dumpFile) 
      const elapsedTime = new Date().getTime() - startTime
      logWriter.write(`${new Date().toISOString()} - Table: "${row.table_name}". Rows: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
	}

    dumpFile.write('}}');
	dumpFile.close();
	
	await dbConn.release();
	logWriter.write('Export operation successful.\n');
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
	if (sql !== undefined) {
      await sql.close();
	}
  }
  
  if (logWriter !== process.stdout) {
	logWriter.close();
  }  
  process.exit()
}

main();
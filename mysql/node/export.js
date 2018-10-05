"use strict";
 
const fs = require('fs');
const common = require('./common.js');
const mysql = require('mysql');
const JSONStream = require('JSONStream')
const Transform = require('stream').Transform;

const sqltAnsiQuotingMode =
`SET SESSION SQL_MODE=ANSI_QUOTES`

const sqlGetSystemInformation = 
`select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION"`;					   

const sqlGenerateTables = 
`select t.table_schema
       ,t.table_name
  	   ,group_concat(concat('"',column_name,'"') order by ordinal_position separator ',')  "columns"
	   ,group_concat(concat('"',data_type,'"') order by ordinal_position separator ',')  "dataTypes"
	   ,group_concat(concat('"',	
                            case when (numeric_precision is not null) and (numeric_scale is not null)
                                   then concat(numeric_precision,',',numeric_scale)	
                                 when (numeric_precision is not null)
                                   then numeric_precision
                                 when (character_maximum_length is not null)
                                   then character_maximum_length
                                 else	
                                   ''	
                            end,	
                            '"'
                           ) 
                           order by ordinal_position separator ','
                    ) "sizeConstraints"
	   ,concat(
          'select json_array('
          ,group_concat(
            case 
              when data_type = 'timestamp'
                -- Force ISO 8601 rendering of value 
                then concat('DATE_FORMAT(convert_tz("', column_name, '", @@session.time_zone, ''+00:00''),''%Y-%m-%dT%TZ'')')
              when data_type = 'datetime'
                -- Force ISO 8601 rendering of value 
                then concat('DATE_FORMAT("', column_name, '", ''%Y-%m-%dT%T'')')
              when data_type = 'year'
                -- Prevent rendering of value as base64:type13: 
                then concat('CAST("', column_name, '"as DECIMAL)')
              when data_type like '%blob'
                -- Force HEXBINARY rendering of value
                then concat('HEX("', column_name, '")')
              else
                concat('"',column_name,'"')
            end
            order by ordinal_position separator ','
          )
          ,') "json" from "'
          ,t.table_schema
          ,'"."'
          ,t.table_name
          ,'"'
        ) QUERY	
   from information_schema.columns	c, information_schema.tables t
  where t.table_name = c.table_name 
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = ?
	  group by t.table_schema, t.table_name`;

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

	const results = await query(conn,sqlGetSystemInformation); 
	return results;

}

async function generateQueries(conn,schema) {    	

   const results = await query(conn,sqlGenerateTables,[schema]);
   return results;
}

function fetchData(conn,sqlQuery,outStream) {

  let counter = 0;
  const parser = new Transform({objectMode:true});
  parser._transform = function(data,encodoing,done) {
	counter++;
    // We get a string, not JSON, and the string can contain \r or \n....
 	this.push(JSON.parse(data.json.replace(/\r?\n|\r/g,"")))
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
            host      : parameters.HOSTNAME
           ,user      : parameters.USERNAME
           ,password  : parameters.PASSWORD
    }

    conn = mysql.createConnection(connectionDetails);
	await connect(conn);
    if (parameters.SQLTRACE) {
      sqlTrace.write(`${sqltAnsiQuotingMode}\n\/\n`)
    }
    await query(conn,sqltAnsiQuotingMode);
	
    const dumpFilePath = parameters.FILE;	
    const dumpFile = fs.createWriteStream(dumpFilePath);
    // dumpFile.on('error',function(err) {console.log(err)})
	
    const schema = parameters.OWNER;
    if (parameters.SQLTRACE) {
      sqlTrace.write(`${sqlGetSystemInformation}\n\/\n`)
    }
	const sysInfo = await getSystemInformation(conn);
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
    if (parameters.SQLTRACE) {
      sqlTrace.write(`${sqlGenerateTables}\n\/\n`)
    }
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
	  if (i > 0) {
		dumpFile.write(',');
      }
	  dumpFile.write(`"${row.TABLE_NAME}" :`);
      if (parameters.SQLTRACE) {
        sqlTrace.write(`${row.QUERY}\n\/\n`)
      }
	  const startTime = new Date().getTime()
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

  if (parameters.SQLTRACE) {
    sqlTrace.close();
  }
  
}

main();
"use strict"; 
const fs = require('fs');
const mariadb = require('mariadb');
const Transform = require('stream').Transform;

const Yadamu = require('../../common/yadamuCore.js');
const MariaCore = require('./mariaCore.js');

const sqlAnsiQuotingMode =
`SET SESSION SQL_MODE=ANSI_QUOTES`

const sqlGetSystemInformation = 
`select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID"`;					   

const sqlGenerateTables = 
// Cannot use JSON_ARRAYAGG for DATA_TYPES and SIZE_CONSTRAINTS beacuse MYSQL implementation of JSON_ARRAYAGG does not support ordering
 
`select c.table_schema "table_schema"
       ,c.table_name "table_name"
       ,group_concat(concat('"',column_name,'"') order by ordinal_position separator ',')  "columns"
       ,concat('[',group_concat(json_quote(data_type) order by ordinal_position separator ','),']')  "dataTypes"
       ,concat('[',group_concat(json_quote(
                            case when (numeric_precision is not null) and (numeric_scale is not null)
                                   then concat(numeric_precision,',',numeric_scale) 
                                 when (numeric_precision is not null)
                                   then case
                                          when column_type like '%unsigned' 
                                            then numeric_precision
                                          else
                                            numeric_precision + 1
                                        end
                                 when (datetime_precision is not null)
                                   then datetime_precision
                                 when (character_maximum_length is not null)
                                   then character_maximum_length
                                 else   
                                   ''   
                            end
                           ) 
                           order by ordinal_position separator ','
                    ),']') "sizeConstraints"
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
              when data_type = 'varbinary'
                -- Force HEXBINARY rendering of value
                then concat('HEX("', column_name, '")')
              when data_type = 'binary'
                -- Force HEXBINARY rendering of value
                then concat('HEX("', column_name, '")')
              else
                concat('"',column_name,'"')
            end
            order by ordinal_position separator ','
          )
          ,') "json" from "'
          ,c.table_schema
          ,'"."'
          ,c.table_name
          ,'"'
        ) "query"
   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
     and c.extra <> 'VIRTUAL GENERATED'
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = ?
	  group by t.table_schema, t.table_name`;

function fetchData(conn,sqlQuery,outStream) {

   let counter = 0;
   
   return new Promise(function(resolve,reject) {
     conn.queryStream(sqlQuery).on('data',function(row) {
                                            if (counter > 0) {
                                              outStream.write(',')
                                            }
                                            if (typeof row.json === 'object') {
                                              // Working with MySQL 8.0 ?
                                              outStream.write(JSON.stringify(row.json));
                                            }
                                            else {
                                             outStream.write(row.json);
                                            }
                                           counter++;
                             }).on('end',function() {
                                           resolve(counter);
                             }).on('error',function(err) {
                                            reject(err);
                             }) 
   })     
}

async function main(){
	
  let pool;
  let conn;
  let parameters;
  let sqlTrace;
  let logWriter = process.stdout;
	
  try {

    parameters = MariaCore.processArguments(process.argv,'export');

	if (parameters.LOGFILE) {
	  logWriter = fs.createWriteStream(parameters.LOGFILE);
    }
    
    if (parameters.SQLTRACE) {
      sqlTrace = fs.createWriteStream(parameters.SQLTRACE);
    }

    const connectionDetails = {
            host        : parameters.HOSTNAME
           ,user        : parameters.USERNAME
           ,password    : parameters.PASSWORD
           ,port        : parameters.PORT ? parameters.PORT : 3307
           ,rowsAsArray : false
    }
    
    pool = mariadb.createPool(connectionDetails);
    conn = await pool.getConnection();

    if (parameters.SQLTRACE) {
      sqlTrace.write(`${sqlAnsiQuotingMode}\n\/\n`)
    }
    
    await conn.query(sqlAnsiQuotingMode);
	
    const dumpFilePath = parameters.FILE;	
    const dumpFile = fs.createWriteStream(dumpFilePath);
    // dumpFile.on('error',function(err) {console.log(err)})
	
    const schema = parameters.OWNER;
    if (parameters.SQLTRACE) {
      sqlTrace.write(`${sqlGetSystemInformation}\n\/\n`)
    }
    
	const mysqlInfo = await conn.query(sqlGetSystemInformation);
	
	dumpFile.write('{"systemInformation":');
	dumpFile.write(JSON.stringify({
		                 "date"            : new Date().toISOString()
                        ,"timeZoneOffset"  : new Date().getTimezoneOffset()
					    ,"vendor"          : "MariaDB"
					    ,"schema"          : schema
					    ,"exportVersion"   : 1
						,"sessionUser"     : mysqlInfo[0].SESSION_USER
						,"currentUser"     : mysqlInfo[0].CURRENT_USER
						,"dbName"          : mysqlInfo[0].DATABASE_NAME
						,"databaseVersion" : mysqlInfo[0].DATABASE_VERSION
                        ,"serverVendor"    : mysqlInfo[0].SERVER_VENDOR_ID
	}));
	
	dumpFile.write(',"metadata":{');
    if (parameters.SQLTRACE) {
      sqlTrace.write(`${sqlGenerateTables}\n\/\n`)
    }
	const sqlQueries = await conn.query(sqlGenerateTables,[schema]);
	for (let i=0; i < sqlQueries.length; i++) {
	  const row = sqlQueries[i];
	  if (i > 0) {
		dumpFile.write(',');
      }
	  dumpFile.write(`"${row.table_name}" : ${JSON.stringify({
						                             "owner"           : row.table_schema
                                                    ,"tableName"       : row.table_name
                                                    ,"columns"         : row.columns
                                                    ,"dataTypes"       : JSON.parse(row.dataTypes)
												    ,"sizeConstraints" : JSON.parse(row.sizeConstraints)
	                 })}`)				   
	}
	dumpFile.write('},"data":{');
	for (let i=0; i < sqlQueries.length; i++) {
      const row = sqlQueries[i]
	  if (i > 0) {
		dumpFile.write(',');
      }
	  dumpFile.write(`"${row.table_name}" :[`);
      if (parameters.SQLTRACE) {
        sqlTrace.write(`${row.QUERY}\n\/\n`)
      }
	  const startTime = new Date().getTime()
      const rows = await fetchData(conn,row.query,dumpFile) 
      const elapsedTime = new Date().getTime() - startTime
      dumpFile.write(']');
      logWriter.write(`${new Date().toISOString()} - Table: "${row.table_name}". Rows: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
	}

    dumpFile.write('}}');
	dumpFile.close();
	
	await conn.end();
 	await pool.end();
	logWriter.write(`Export operation successful.`);
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
	if (pool !== undefined) {
      await pool.end();
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
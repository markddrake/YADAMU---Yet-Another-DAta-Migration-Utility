"use strict";
 
const fs = require('fs');
const common = require('./common.js');
const mysql = require('mysql');
const JSONStream = require('JSONStream')
const Transform = require('stream').Transform;

const sqlAnsiQuotingMode =
`SET SESSION SQL_MODE=ANSI_QUOTES`

const sqlGetSystemInformation = 
`select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID"`;                     

const sqlGenerateMetadata =
`select c.table_schema "table_schema"
       ,c.table_name "table_name"
       ,group_concat(concat('"',column_name,'"') order by ordinal_position separator ',')  "columns"
       ,group_concat(concat('"',data_type,'"') order by ordinal_position separator ',')  "dataTypes"
       ,group_concat(concat('"',    
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
`;


// Check for duplicate entries INFORMATION_SCHEMA.columns

const sqlCheckInformationSchemaState =
`select distinct c.table_schema, c.table_name
   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
    and c.extra <> 'VIRTUAL GENERATED'
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = ?
  group by TABLE_SCHEMA,TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION
  having count(*) > 1`


const sqlInformationSchemaClean =
`   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
    and c.extra <> 'VIRTUAL GENERATED'
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = ?
      group by t.table_schema, t.table_name`;
      
    
// Hack for Duplicate Entries in INFORMATION_SCHEMA.columns seen MSSQL 5.7

const sqlInformationSchemaFix  = 
`   from (
     select distinct c.table_catalog, c.table_schema, c.table_name,column_name,ordinal_position,data_type,column_type,character_maximum_length,numeric_precision,numeric_scale,datetime_precision
       from information_schema.columns c, information_schema.tables t
       where t.table_name = c.table_name 
         and c.extra <> 'VIRTUAL GENERATED'
         and t.table_schema = c.table_schema
         and t.table_type = 'BASE TABLE'
         and t.table_schema = ?
   ) c
  group by c.table_schema, c.table_name`;

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

async function getMetadataQuery(conn,schema,logWriter,sqlTrace) { 

   /*
   **
   ** During testing on 5.7 it appeared tha that is is possible for the Information Schema to get corrupted
   ** In this state it contains duplicate entires for each column in the table.
   ** 
   ** This routine checks for this state and creates a query that will workaround the problem if the 
   ** Information schema is corrupt.
   ** 
   */   
   
   if (sqlTrace) {
     sqlTrace.write(`${sqlCheckInformationSchemaState}\n\/\n`)
   }

   const results = await query(conn,sqlCheckInformationSchemaState,[schema]);
   if (results.length ===  0) {
     return sqlGenerateMetadata + sqlInformationSchemaClean;
   }
   else {
     for (const i in results) {
       logWriter.write(`${new Date().toISOString()}[WARNING]: Table: "${results[i].table_schema}"."${results[i].table_name}". Duplicate entires detected in INFORMATION_SCHEMA.COLUMNS.\n`)
     }
     return sqlGenerateMetadata + sqlInformationSchemaFix;
   }
   
}

async function generateQueries(conn,schema,sqlGenerateTables) {       

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
    
  let conn;
  let parameters;
  let sqlTrace;
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
      sqlTrace.write(`${sqlAnsiQuotingMode}\n\/\n`)
    }
    await query(conn,sqlAnsiQuotingMode);
    
    const dumpFilePath = parameters.FILE;   
    const dumpFile = fs.createWriteStream(dumpFilePath);
    // dumpFile.on('error',function(err) {console.log(err)})
    
    const schema = parameters.OWNER;
    if (parameters.SQLTRACE) {
      sqlTrace.write(`${sqlGetSystemInformation}\n\/\n`)
    }
    const mysqlInfo = await getSystemInformation(conn);
    dumpFile.write('{"systemInformation":');
    dumpFile.write(JSON.stringify({
                         "date"            : new Date().toISOString()
                        ,"vendor"          : "MySQL"
                        ,"schema"          : schema
                        ,"exportVersion"   : 1
                        ,"sessionUser"     : mysqlInfo[0].SESSION_USER
                        ,"currentUser"     : mysqlInfo[0].CURRENT_USER
                        ,"dbName"          : mysqlInfo[0].DATABASE_NAME
                        ,"databaseVersion" : mysqlInfo[0].DATABASE_VERSION
                        ,"serverVendor"    : mysqlInfo[0].SERVER_VENDOR_ID
    }));
        
    const sqlGenerateTables = await getMetadataQuery(conn,schema,logWriter,sqlTrace);
       
    dumpFile.write(',"metadata":{');
    if (parameters.SQLTRACE) {
      sqlTrace.write(`${sqlGenerateTables}\n\/\n`)
    }
    const sqlQueries = await generateQueries(conn,schema,sqlGenerateTables);
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
      if (i > 0) {
        dumpFile.write(',');
      }
      dumpFile.write(`"${row.table_name}" :`);
      if (parameters.SQLTRACE) {
        sqlTrace.write(`${row.query}\n\/\n`)
      }
      const startTime = new Date().getTime()
      const rows = await fetchData(conn,row.query,dumpFile) 
      const elapsedTime = new Date().getTime() - startTime
      logWriter.write(`${new Date().toISOString()} - Table: "${row.table_name}". Rows: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
    }

    dumpFile.write('}}');
    dumpFile.close();
    
    await conn.end();
    logWriter.write(`Export operation successful.\n`);
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
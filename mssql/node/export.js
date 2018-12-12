"use strict";
const fs = require('fs');
const sql = require('mssql');
const path = require('path');

const Yadamu = require('../../common/yadamuCore.js');
const MsSQLCore = require('./mssqlCore.js');

const sqlGetSystemInformation = 
`select db_Name() "DATABASE_NAME", current_user "CURRENT_USER", session_user "SESSION_USER", CONVERT(NVARCHAR(20),SERVERPROPERTY('ProductVersion')) "DATABASE_VERSION",CONVERT(NVARCHAR(128),SERVERPROPERTY('MachineName')) "HOSTNAME"`;                     

const sqlGenerateQueries =
`select t.table_schema
       ,t.table_name
       ,string_agg(concat('"',c.column_name,'"'),',') within group (order by ordinal_position) "columns"
       ,string_agg(concat('"',data_type,'"'),',') within group (order by ordinal_position) "dataTypes"
       ,string_agg(case
                     when (numeric_precision is not null) and (numeric_scale is not null) 
                       then concat('"',numeric_precision,',',numeric_scale,'"')
                     when (numeric_precision is not null) 
                       then concat('"',numeric_precision,'"')
                     when (datetime_precision is not null)
                       then concat('"',datetime_precision,'"')
                     when (character_maximum_length is not null)
                       then concat('"',character_maximum_length,'"')
                     else
                       '""'
                   end
                  ,','
                 )
                 within group (order by ordinal_position) "sizeConstraints"
       ,concat('select ',string_agg(case 
                                      when data_type = 'hierarchyid' then
                                        concat('cast("',column_name,'" as NVARCHAR(4000)) "',column_name,'"') 
                                      when data_type = 'geography' then
                                        concat('"',column_name,'".AsTextZM() "',column_name,'"') 
                                      when data_type = 'geometry' then
                                        concat('"',column_name,'".AsTextZM() "',column_name,'"') 
                                      when data_type = 'datetime2' then
                                        concat('convert(VARCHAR(33),"',column_name,'",127) "',column_name,'"') 
                                      when data_type = 'datetimeoffset' then
                                        concat('convert(VARCHAR(33),"',column_name,'",127) "',column_name,'"') 
                                      else 
                                        concat('"',column_name,'"') 
                                    end
                                   ,','
                                  ) 
                                  within group (order by ordinal_position)
                        ,' from "',t.table_schema,'"."',t.table_name,'"') QUERY
   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = @SCHEMA
  group by t.table_schema, t.table_name`;    
 
 
 
async function getSystemInformation(request) {   

    const results = await await request.query(sqlGetSystemInformation);
    return results.recordsets[0];
 
}

async function generateQueries(request,schema) {     

 const results = await request.input('SCHEMA',sql.VARCHAR,schema).batch(sqlGenerateQueries);
 return results.recordsets[0]

}

function fetchData(request,tableInfo,outStream) {

  const column_list = JSON.parse(`[${tableInfo.columns}]`);

  outStream.write('[');
  request.stream = true // You can set streaming differently for each request
 
  let counter = 0;

  return new Promise(async function(resolve,reject) { 

    request.on('done', function(result) {
      outStream.write(']');
      resolve(counter)
    })
  
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
      reject(err)
    })
 
    request.query(tableInfo.QUERY) // or request.execute(procedure)
  })
}

function closeFile(outStream) {
        
  return new Promise(function(resolve,reject) {
    outStream.on('finish',function() { resolve() });
    outStream.close();
  })

}

async function main(){
    
  let pool;
  let parameters;
  let sqlTrace;
  let logWriter = process.stdout;
  let status;
    
  try {

    parameters = MsSQLCore.processArguments(process.argv);
    status = Yadamu.getStatus(parameters,'Export');

    if (parameters.LOGFILE) {
      logWriter = fs.createWriteStream(parameters.LOGFILE,{flags : "a"});
    }
    
    pool = await MsSQLCore.getConnectionPool(parameters,status);
    const request = pool.request();
   
    const exportFilePath = path.resolve(parameters.FILE);
    const exportFile = fs.createWriteStream(exportFilePath);
    // exportFile.on('error',function(err) {console.log(err)})
    logWriter.write(`${new Date().toISOString()}[Export]: Generating file "${exportFilePath}".\n`)

    const schema = parameters.OWNER;
    
    if (status.sqlTrace) {
      status.sqlTrace.write(`${sqlGetSystemInformation}\n\/\n`)
    }
    
    const sysInfo = await getSystemInformation(request);
    exportFile.write('{"systemInformation":');
    exportFile.write(JSON.stringify({
                      "date"            : new Date().toISOString()
                     ,"timeZoneOffset"  : new Date().getTimezoneOffset()
                     ,"vendor"          : "MSSQLSERVER"
                     ,"schema"          : schema
                     ,"exportVersion"   : 1
                     ,"sessionUser"     : sysInfo[0].SESSION_USER
                     ,"currentUser"     : sysInfo[0].CURRENT_USER
                     ,"dbName"          : sysInfo[0].DATABASE_NAME
                     ,"databaseVersion" : sysInfo[0].DATABASE_VERSION
                     ,"softwareVendor"  : "Microsoft Corporation"
                     ,"hostname"        : sysInfo.HOSTNAME
    }));
     
    exportFile.write(',"metadata":{')
    if (status.sqlTrace) {
      status.sqlTrace.write(`${sqlGenerateQueries}\n\/\n`)
    }
         
    const sqlQueries = await generateQueries(request,schema);
     
    for (let i=0; i < sqlQueries.length; i++) {
      const row = sqlQueries[i];
      if (i > 0) {
        exportFile.write(',');
      }
      exportFile.write(`"${row.table_name}" : ${JSON.stringify({
                           "owner" : row.table_schema
                          ,"tableName" : row.table_name
                          ,"columns" : row.columns
                          ,"dataTypes" : JSON.parse('[' + row.dataTypes + ']')
                          ,"sizeConstraints" : JSON.parse('[' + row.sizeConstraints + ']')
                          ,selectList : row.selectList
      })}`)               
    }

    exportFile.write('},"data":{');
    for (let i=0; i < sqlQueries.length; i++) {
      const row = sqlQueries[i]
      if (i > 0) {
        exportFile.write(',');
      }
      exportFile.write(`"${row.table_name}" :`);
      if (status.sqlTrace) {
        status.sqlTrace.write(`${row.QUERY}\n\/\n`)
      }
      const startTime = new Date().getTime()
      const rows = await fetchData(new sql.Request(pool),row,exportFile) 
      const elapsedTime = new Date().getTime() - startTime
      logWriter.write(`${new Date().toISOString()} - Table: "${row.table_name}". Rows: ${rows}. Elaspsed Time: ${elapsedTime}ms. Throughput: ${Math.round((rows/elapsedTime) * 1000)} rows/s.\n`)
    }
    
    exportFile.write('}}');    
    await closeFile(exportFile);
    
    await pool.release();
    Yadamu.reportStatus(status,logWriter);
  } catch (e) {
    if (logWriter !== process.stdout) {
      console.log(`Export operation failed: See "${parameters.LOGFILE}" for details.`);
      logWriter.write('Export operation failed.\n');
      logWriter.write(`${e.stack}\n`);
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

  if (status.sqlTrace) {
    status.sqlTrace.close();
  }
 
  process.exit()
}


main();
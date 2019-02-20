"use strict";
const sql = require('mssql');

const Yadamu = require('../../common/yadamuCore.js');

const sqlTableInfo =
`select t.table_schema "TABLE_SCHEMA"
       ,t.table_name   "TABLE_NAME"
       ,string_agg(concat('"',c.column_name,'"'),',') within group (order by ordinal_position) "COLUMN_LIST"
       ,string_agg(concat('"',data_type,'"'),',') within group (order by ordinal_position) "DATA_TYPES"
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
                 within group (order by ordinal_position) "SIZE_CONSTRAINTS"
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
                        ,' from "',t.table_schema,'"."',t.table_name,'"') "SQL_STATEMENT"
   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = @SCHEMA
  group by t.table_schema, t.table_name`;    


async function getTableInfo(request,schema,status) {
             
  if (status.sqlTrace) {
    status.sqlTrace.write(`${sqlTableInfo}\n\/\n`)
  }
    
  const results = await request.input('SCHEMA',sql.VARCHAR,schema).batch(sqlTableInfo);
  return results.recordsets[0]
  
}
       
function generateMetadata(tableInfo) {
    
  const metadata = {}
  for (let table of tableInfo) {
    metadata[table.TABLE_NAME] = {
      owner                    : table.TABLE_SCHEMA
     ,tableName                : table.TABLE_NAME
     ,columns                  : table.COLUMN_LIST
     ,dataTypes                : JSON.parse('[' + table.DATA_TYPES + ']')
     ,sizeConstraints          : JSON.parse('[' + table.SIZE_CONSTRAINTS + ']')
    }
  }
  return metadata;    
}
    
function processArguments(args,operation) {

 const parameters = {
	 FILE : "export.json"
					,PORT : 1433
					,OWNER : 'dbo'
					,TOUSER : 'dbo'
					,FROMUSER : 'dbo'
 ,MODE : "DDL_AND_CONTENT"
 ,BATCHSIZE : 500
 ,COMMITROWS : 1000

 }

 process.argv.forEach(function (arg) {
	 
   if (arg.indexOf('=') > -1) {
     const parameterName = arg.substring(0,arg.indexOf('='));
	 const parameterValue = arg.substring(arg.indexOf('=')+1);
     switch (parameterName.toUpperCase()) {
	 case 'DATABASE':
	 case '--DATABASE':
 	 parameters.DATABASE = parameterValue;
			break;
	 case 'HOSTNAME':
	 case '--HOSTNAME':
 	 parameters.HOSTNAME = parameterValue;
			break;
	 case 'HOSTNAME':
	 case '--HOSTNAME':
 	 parameters.HOSTNAME = parameterValue;
			break;
	 case 'PORT':
	 case '--PORT':
	 parameters.PORT = parameterValue;
			break;
	 case 'PASSWORD':
	 case '--PASSWORD':
	 parameters.PASSWORD = parameterValue;
			break;
	 case 'USERNAME':
	 case '--USERNAME':
	 parameters.USERNAME = parameterValue;
			break;
	 case 'FILE':
	 case '--FILE':
	 parameters.FILE = parameterValue;
			break;
	 case 'OWNER':
	 case '--OWNER':
		 parameters.OWNER = Yadamu.processValue(parameterValue);
			break;
	 case 'FROMUSER':
	 case '--FROMUSER':
		 parameters.FROMUSER = Yadamu.processValue(parameterValue);
			break;
	 case 'TOUSER':
	 case '--TOUSER':
		 parameters.TOUSER = Yadamu.processValue(parameterValue);
			break;
	 case 'LOGFILE':
	 case '--LOGFILE':
		 parameters.LOGFILE = parameterValue;
			break;
	 case 'SQLTRACE':
	 case '--SQLTRACE':
		 parameters.SQLTRACE = parameterValue;
			break;
	 case 'LOGLEVEL':
	 case '--LOGLEVEL':
		 parameters.LOGLEVEL = parameterValue;
			break;
	 case 'DUMPFILE':
	 case '--DUMPFILE':
		 parameters.DUMPFILE = parameterValue.toUpperCase();
			break;
	 case 'MODE':
		 parameters.MODE = parameterValue.toUpperCase();
			break;
		 default:
		 console.log(`Unknown parameter: "${parameterName}".`)			
	 }
	 }
 })
 
 return parameters;
}

async function getConnectionPool(parameters,status) {

  const config = {
          server          : parameters.HOSTNAME
         ,user            : parameters.USERNAME
         ,database        : parameters.DATABASE
         ,password        : parameters.PASSWORD
         ,port            : parameters.PORT
         ,requestTimeout  : 2 * 60 * 60 * 10000
         ,options   : {
             encrypt: false // Use this if you're on Windows Azure
          }
        }
        
    const pool = new sql.ConnectionPool(config);
    await pool.connect()
    const statement = `SET QUOTED_IDENTIFIER ON`
    if (status.sqlTrace) {
      status.sqlTrace.write(`${statement}\n\/\n`)
    }
    await pool.query(statement);
    return pool;
    
}

async function useDatabase(conn,databaseName,status) {
    
    const statement = `use ${databaseName}`
    if (status.sqlTrace) {
      status.sqlTrace.write(`${statement}\n\/\n`)
    }
    await conn.query(statement);
}


module.exports.processArguments  = processArguments
module.exports.getConnectionPool = getConnectionPool
module.exports.useDatabase       = useDatabase
module.exports.getTableInfo      = getTableInfo
module.exports.generateMetadata  = generateMetadata
